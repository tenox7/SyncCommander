package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

// L7: every backend joins its base with cleanRel's path and tests for the
// base with isBaseRel, so the two agree and no path leaves the base.
func TestCleanRel(t *testing.T) {
	for in, want := range map[string]string{
		"": "", ".": "", "/": "", "./": "", "..": "", "../..": "", "/..": "", "./..": "", "keep/../..": "",
		"a": "a", "/a/b/": "a/b", "a/./b": "a/b", "a//b": "a/b", "a/../b": "b", "../x": "x",
		"../../.ssh/authorized_keys": ".ssh/authorized_keys", "...": "...", ".hidden": ".hidden",
	} {
		if got := cleanRel(in); got != want {
			t.Errorf("cleanRel(%q) = %q, want %q", in, got, want)
		}
		if got := isBaseRel(in); got != (want == "") {
			t.Errorf("isBaseRel(%q) = %v, want %v", in, got, want == "")
		}
	}
}

// filesUnder lists everything under dir, folders with a trailing slash.
func filesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == dir {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			rel += "/"
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

func putFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// basePaths are paths that land on the base itself once cleaned.
var basePaths = []string{"..", "../..", "keep/../..", "./..", "/.."}

// refused reports a backend's refusal to remove its base: another error,
// such as no such file, says the guard let the path through.
func refused(err error) bool { return err != nil && strings.Contains(err.Error(), "refus") }

// L7: LocalPath took ".." to the base, and the base guard did not know it,
// so RemoveAll("..") was os.RemoveAll(base). The base sits two folders deep
// in the test's own folder, which is all an old build could remove.
func TestLocalRemoveRefusesPathsOntoTheBase(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "a", "b", "base")
	putFile(t, filepath.Join(base, "keep", "f.txt"), "keep")
	empty := filepath.Join(root, "a", "empty")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b, e := NewLocalBackend(base), NewLocalBackend(empty)
	for _, rel := range basePaths {
		if err := b.RemoveAll(ctx, rel); !refused(err) {
			t.Errorf("RemoveAll(%q) = %v, want it refused", rel, err)
		}
		if err := e.Remove(ctx, rel); !refused(err) {
			t.Errorf("Remove(%q) on an empty base = %v, want it refused", rel, err)
		}
	}
	want := []string{"a/", "a/b/", "a/b/base/", "a/b/base/keep/", "a/b/base/keep/f.txt", "a/empty/"}
	if got := filesUnder(t, root); !slices.Equal(got, want) {
		t.Errorf("after the removes the test folder holds %q, want %q", got, want)
	}
}

// storedPaths lists the paths the fake FTP server holds files at.
func (f *fakeFTPD) storedPaths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.store))
	for p := range f.store {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// L7: ftp joined relPath onto its base with path.Join, which keeps a
// leading "../": a copy to "../../evil" was stored above the base, and
// RemoveAll("..") removed the base's parent. The fake keeps its files in
// memory.
func TestFTPPathsStayUnderTheBase(t *testing.T) {
	f := &fakeFTPD{
		ls: map[string][]string{
			"/srv":          {lsLine("base", true)},
			"/srv/base":     {lsLine("keep.txt", false), lsLine("sub", true)},
			"/srv/base/sub": {lsLine(".", true), lsLine("..", true), lsLine("g.txt", false)},
		},
		store: map[string][]byte{"/srv/base/keep.txt": []byte("keep!"), "/srv/base/sub/g.txt": []byte("g")},
	}
	b := openFakeFTP(t, f, "/srv/base")
	ctx := context.Background()
	if err := b.CopyFrom(ctx, "../../evil", strings.NewReader("evil"), model.PutParams{Mode: 0o644}); err != nil {
		t.Errorf("CopyFrom(../../evil): %v", err)
	}
	for _, rel := range basePaths {
		if err := b.RemoveAll(ctx, rel); !refused(err) {
			t.Errorf("RemoveAll(%q) = %v, want it refused", rel, err)
		}
	}
	// The "." and ".." of a listing are no children, and no reason to
	// refuse a folder.
	if err := b.RemoveAll(ctx, "sub"); err != nil {
		t.Errorf("RemoveAll(sub): %v", err)
	}
	if got, want := f.storedPaths(), []string{"/srv/base/evil", "/srv/base/keep.txt"}; !slices.Equal(got, want) {
		t.Errorf("the server holds %q, want %q", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if want := []string{"/srv/base/sub"}; !slices.Equal(f.rmd, want) {
		t.Errorf("RMD removed %q, want %q", f.rmd, want)
	}
}

// L7: ftp's RemoveAll joins each name its listing gives onto the folder.
// A hostile server lists "../../victim" in a folder the user deletes: an
// old build deleted /srv/victim, outside the base. A listing with a name
// that is no file name removes nothing in that folder.
func TestFTPRemoveAllRefusesAListedNameThatIsNoFileName(t *testing.T) {
	f := &fakeFTPD{
		ls: map[string][]string{
			"/srv/base":     {lsLine("dir", true)},
			"/srv/base/dir": {lsLine("f.txt", false), lsLine("../../victim", false)},
		},
		store: map[string][]byte{"/srv/base/dir/f.txt": []byte("f"), "/srv/victim": []byte("v")},
	}
	b := openFakeFTP(t, f, "/srv/base")
	err := b.RemoveAll(context.Background(), "dir")
	if err == nil || !strings.Contains(err.Error(), `"../../victim"`) {
		t.Errorf("RemoveAll(dir) = %v, want the name that is no file name", err)
	}
	if got, want := f.storedPaths(), []string{"/srv/base/dir/f.txt", "/srv/victim"}; !slices.Equal(got, want) {
		t.Errorf("the server holds %q, want %q", got, want)
	}
}

// L7, S6: sftp, scp and rsync+ssh join relPath onto the base in
// sshShell.abs. The server serves this machine's files: the base sits two
// folders deep in the test's own folder, which is all an old build could
// write to or remove.
func TestSFTPPathsStayUnderTheBase(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "a", "b", "base")
	putFile(t, filepath.Join(base, "keep", "f.txt"), "keep")
	b, err := NewSFTPBackend(context.Background(), "sftp://x:pw@"+startSFTPServer(t)+filepath.ToSlash(base), true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if err := b.CopyFrom(ctx, "../../evil", strings.NewReader("evil"), model.PutParams{Mode: 0o644}); err != nil {
		t.Errorf("CopyFrom(../../evil): %v", err)
	}
	for _, rel := range basePaths {
		if err := b.RemoveAll(ctx, rel); !refused(err) {
			t.Errorf("RemoveAll(%q) = %v, want it refused", rel, err)
		}
	}
	want := []string{"a/", "a/b/", "a/b/base/", "a/b/base/evil", "a/b/base/keep/", "a/b/base/keep/f.txt"}
	if got := filesUnder(t, root); !slices.Equal(got, want) {
		t.Errorf("the test folder holds %q, want %q", got, want)
	}
}

// startSFTPRequestServer is startSFTPServer with an sftp server that h
// serves, such as the library's in-memory one.
func startSFTPRequestServer(t *testing.T, h sftp.Handlers) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{NoClientAuth: true}
	cfg.AddHostKey(signer)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				conn, chans, reqs, err := ssh.NewServerConn(nc, cfg)
				if err != nil {
					return
				}
				defer conn.Close()
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					c, creqs, err := ch.Accept()
					if err != nil {
						continue
					}
					go func() {
						defer c.Close()
						for req := range creqs {
							var sub struct{ Name string }
							ok := req.Type == "subsystem" && ssh.Unmarshal(req.Payload, &sub) == nil && sub.Name == "sftp"
							req.Reply(ok, nil)
							if ok {
								sftp.NewRequestServer(c, h).Serve()
								return
							}
						}
					}()
				}
			}()
		}
	}()
	return l.Addr().String()
}

// hostileLister lists one more folder than each folder in once holds, the
// first time that folder is listed, by a name the client library reads as
// "..": it keeps the last part of a name with a slash.
type hostileLister struct {
	sftp.FileLister
	mu   sync.Mutex
	once map[string]bool
}

type namedDir string

func (n namedDir) Name() string     { return string(n) }
func (namedDir) Size() int64        { return 0 }
func (namedDir) Mode() fs.FileMode  { return fs.ModeDir | 0o755 }
func (namedDir) ModTime() time.Time { return time.Unix(1700000000, 0) }
func (namedDir) IsDir() bool        { return true }
func (namedDir) Sys() any           { return nil }

// listAt serves a listing that is all in memory.
type listAt []os.FileInfo

func (l listAt) ListAt(out []os.FileInfo, off int64) (int, error) {
	if off >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(out, l[off:])
	if off+int64(n) >= int64(len(l)) {
		return n, io.EOF
	}
	return n, nil
}

func (h *hostileLister) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	la, err := h.FileLister.Filelist(r)
	dir := path.Clean(r.Filepath)
	h.mu.Lock()
	inject := r.Method == "List" && h.once[dir]
	if inject {
		h.once[dir] = false
	}
	h.mu.Unlock()
	if err != nil || !inject {
		return la, err
	}
	all := listAt{namedDir("x/..")}
	buf := make([]os.FileInfo, 16)
	for off := int64(0); ; {
		n, err := la.ListAt(buf, off)
		all = append(all, buf[:n]...)
		off += int64(n)
		if err != nil {
			break
		}
	}
	return all, nil
}

// L7, S6: sftp's RemoveAll joins each name its listing gives onto the
// folder. A hostile server lists "x/.." in the folder deleted and in the
// base, which the library hands on as "..": an old build climbed from the
// folder to the base and on to the base's parent, and deleted
// /srv/outside.txt. The server keeps its files in memory.
func TestSFTPRemoveAllRefusesAListedNameThatIsNoFileName(t *testing.T) {
	h := sftp.InMemHandler()
	// The open flags of SSH_FXP_OPEN: read 1, write 2, create 8, trunc 16.
	req := func(method, p string, flags uint32) *sftp.Request {
		r := sftp.NewRequest(method, p)
		r.Flags = flags
		return r
	}
	put := func(p, body string) {
		w, err := h.FilePut.Filewrite(req("Put", p, 2|8|16))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteAt([]byte(body), 0); err != nil {
			t.Fatal(err)
		}
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
	for _, d := range []string{"/srv", "/srv/base", "/srv/base/dir"} {
		if err := h.FileCmd.Filecmd(sftp.NewRequest("Mkdir", d)); err != nil {
			t.Fatal(err)
		}
	}
	put("/srv/outside.txt", "outside")
	put("/srv/base/keep.txt", "keep")
	put("/srv/base/dir/f.txt", "f")
	lister := &hostileLister{FileLister: h.FileList, once: map[string]bool{"/srv/base/dir": true, "/srv/base": true}}
	h.FileList = lister

	b, err := NewSFTPBackend(context.Background(), "sftp://x:pw@"+startSFTPRequestServer(t, h)+"/srv/base", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	err = b.RemoveAll(context.Background(), "dir")
	if err == nil || !strings.Contains(err.Error(), `".."`) {
		t.Errorf(`RemoveAll(dir) = %v, want the name ".." refused`, err)
	}
	for _, p := range []string{"/srv/outside.txt", "/srv/base/keep.txt", "/srv/base/dir/f.txt"} {
		if _, err := h.FileGet.Fileread(req("Get", p, 1)); err != nil {
			t.Errorf("%s is gone: %v", p, err)
		}
	}
}

// L7: rclone joins a path onto the remote's root as it is given. The base
// sits two folders deep in the test's own folder.
func TestRclonePathsStayUnderTheBase(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "a", "b", "base")
	putFile(t, filepath.Join(base, "keep", "f.txt"), "keep")
	b, err := NewRcloneBackend(context.Background(), "rclone://"+base, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if err := b.CopyFrom(ctx, "../../evil", strings.NewReader("evil"), model.PutParams{Mode: 0o644}); err != nil {
		t.Errorf("CopyFrom(../../evil): %v", err)
	}
	if err := b.Mkdir(ctx, "x/../../d", 0o755); err != nil {
		t.Errorf("Mkdir(x/../../d): %v", err)
	}
	for _, rel := range basePaths {
		if err := b.RemoveAll(ctx, rel); !refused(err) {
			t.Errorf("RemoveAll(%q) = %v, want it refused", rel, err)
		}
	}
	// Each call takes its path as cleanRel gives it.
	const f = "../keep/f.txt"
	read := func(rc io.ReadCloser, err error) string {
		if err != nil {
			return "error: " + err.Error()
		}
		defer rc.Close()
		body, _ := io.ReadAll(rc)
		return string(body)
	}
	if got := read(b.Open(ctx, f)); got != "keep" {
		t.Errorf("Open(%s) read %q, want %q", f, got, "keep")
	}
	if got := read(b.OpenAt(ctx, f, 1)); got != "eep" {
		t.Errorf("OpenAt(%s, 1) read %q, want %q", f, got, "eep")
	}
	if got, err := b.List(ctx, "../keep"); err != nil || len(got) != 1 || got[0].RelPath != "keep/f.txt" {
		t.Errorf("List(../keep) = %+v, %v, want keep/f.txt", got, err)
	}
	if e, err := b.Stat(ctx, f); err != nil || e.Size != 4 {
		t.Errorf("Stat(%s) = %+v, %v, want 4 bytes", f, e, err)
	}
	if algos, _ := b.ChecksumAlgos(ctx); len(algos) > 0 {
		want, err := b.Checksum(ctx, "keep/f.txt", algos[0])
		if got, gerr := b.Checksum(ctx, f, algos[0]); err != nil || gerr != nil || got != want {
			t.Errorf("Checksum(%s) = %q, %v, want %q, %v", f, got, gerr, want, err)
		}
	}
	mt := time.Unix(1600000000, 0)
	if err := b.SetTimes(ctx, f, mt, mt, time.Time{}); err != nil {
		t.Errorf("SetTimes(%s): %v", f, err)
	} else if fi, err := os.Stat(filepath.Join(base, "keep", "f.txt")); err != nil || !fi.ModTime().Equal(mt) {
		t.Errorf("SetTimes(%s) left keep/f.txt at %v, want %v", f, fi.ModTime(), mt)
	}
	if err := b.Rename(ctx, f, "../keep/g.txt"); err != nil {
		t.Errorf("Rename(%s, ../keep/g.txt): %v", f, err)
	}
	if err := b.Remove(ctx, "../keep/g.txt"); err != nil {
		t.Errorf("Remove(../keep/g.txt): %v", err)
	}
	want := []string{"a/", "a/b/", "a/b/base/", "a/b/base/d/", "a/b/base/evil", "a/b/base/keep/"}
	if got := filesUnder(t, root); !slices.Equal(got, want) {
		t.Errorf("the test folder holds %q, want %q", got, want)
	}
}

// L7: a recursive preload of ".." lists the base, as List("..") does. A
// local remote lists nothing ahead; the in-memory one does.
func TestRclonePreloadStaysUnderTheBase(t *testing.T) {
	ctx := context.Background()
	b, err := NewRcloneBackend(ctx, "rclone://:memory:l7/base", false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CopyFrom(ctx, "f.txt", strings.NewReader("f"), model.PutParams{Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	mark := len(Log.Lines())
	if err := b.PreloadRecursive(ctx, ".."); err != nil {
		t.Fatal(err)
	}
	if got, err := b.List(ctx, ""); err != nil || len(got) != 1 || got[0].RelPath != "f.txt" {
		t.Errorf("List() = %+v, %v, want f.txt", got, err)
	}
	var lists []string
	for _, l := range Log.Lines()[mark:] {
		if _, what, ok := strings.Cut(l, " >>> "); ok {
			lists = append(lists, what)
		}
	}
	if !slices.ContainsFunc(lists, func(l string) bool {
		return strings.HasPrefix(l, "recursive list ") && strings.HasSuffix(l, ":l7/base")
	}) {
		t.Errorf("the calls were %q, want a recursive list of l7/base", lists)
	}
}

// L7: the rsync daemon backend stages the folders of an upload and of a
// new folder under a local temp folder, by the path, and sends them to
// module/base. TMPDIR, TMP and TEMP point two folders deep in the test's
// own folder, which is all an old build could write to.
func TestRsyncDaemonPathsStayUnderTheBase(t *testing.T) {
	root := t.TempDir()
	tmp, mod := filepath.Join(root, "a", "b", "tmp"), filepath.Join(root, "mod")
	putFile(t, filepath.Join(mod, "base", "keep", "f.txt"), "keep")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(v, tmp)
	}
	b, err := NewRsyncBackend(startGorsyncdModule(t, mod, true) + "/base")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if err := b.Mkdir(ctx, "../../d", 0o755); err != nil {
		t.Errorf("Mkdir(../../d): %v", err)
	}
	if err := b.CopyFrom(ctx, "../../evil", strings.NewReader("evil"), model.PutParams{Mode: 0o644}); err != nil {
		t.Errorf("CopyFrom(../../evil): %v", err)
	}
	for _, rel := range basePaths {
		if err := b.RemoveAll(ctx, rel); !refused(err) {
			t.Errorf("RemoveAll(%q) = %v, want it refused", rel, err)
		}
	}
	want := []string{"a/", "a/b/", "a/b/tmp/", "mod/", "mod/base/", "mod/base/d/", "mod/base/evil", "mod/base/keep/", "mod/base/keep/f.txt"}
	if got := filesUnder(t, root); !slices.Equal(got, want) {
		t.Errorf("the test folder holds %q, want %q", got, want)
	}
}

// L7: webdav and restic address a path through pathFor, which sent
// "/dav/base/../../x" as it was: the server takes it for "/x".
func TestHTTPPathForStaysUnderTheBase(t *testing.T) {
	for _, base := range []string{"dav/base", ""} {
		h := &httpBase{base: base}
		top := "/" + base
		for in, want := range map[string]string{
			"": top, "..": top, "a/../..": top,
			"../../x": path.Join(top, "x"), "a/../b/": path.Join(top, "b"), "/a/b/": path.Join(top, "a/b"),
		} {
			if got := h.pathFor(in); got != want {
				t.Errorf("base %q: pathFor(%q) = %q, want %q", base, in, got, want)
			}
		}
	}
}

// L7: the rsync daemon backend names a path module/base/rel, and stages an
// upload's folders under a local temp folder by its path.
func TestRsyncPathsStayUnderTheBase(t *testing.T) {
	b, err := NewRsyncBackend("rsync://127.0.0.1:1/mod/base")
	if err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{"../../x": "mod/base/x", "..": "mod/base", "a/../../b": "mod/base/b"} {
		if got, err := b.modulePath(in, false); got != want || err != nil {
			t.Errorf("modulePath(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	stage := filepath.Join(t.TempDir(), "a", "b", "stage")
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	top, leaf, err := stageUploadPath(stage, "../../x/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if top != filepath.Join(stage, "x") || leaf != filepath.Join(stage, "x", "f.txt") {
		t.Errorf("stageUploadPath(../../x/f.txt) = %q, %q, want both under %q", top, leaf, stage)
	}
}
