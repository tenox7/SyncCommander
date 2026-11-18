package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/sftp"

	"sc/internal/sshtest"
	"sc/model"
)

// writeFiles writes each name's body below root.
func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// copyFiles writes each name's body below b over sftp.
func copyFiles(t *testing.T, b model.Backend, files map[string]string) {
	t.Helper()
	for name, body := range files {
		if err := b.CopyFrom(context.Background(), name, strings.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
}

func sftpAt(t *testing.T, url string) *SFTPBackend {
	t.Helper()
	b, err := NewSFTPBackend(context.Background(), url, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// S25: a NAS may give sftp a root of its own, so that sftp's /photo is the
// shell's /volume1/photo. sc listed with find, summed and removed folders
// with shell commands on sftp's paths: it listed another folder, summed
// another file, and its rm -rf removed the shell's folder of that name, or
// nothing, and said done. sc now uses the shell only where both see one
// folder.
func TestSFTPUsesTheShellOnlyWhereItSeesSFTPsFiles(t *testing.T) {
	// The shell's folder: other files at sftp's path.
	base := t.TempDir()
	writeFiles(t, base, map[string]string{"f.txt": "the shell's file", "decoy.txt": "not sftp's", "sub/g.txt": "the shell's"})
	long := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(base, long, long); err != nil {
		t.Fatal(err)
	}
	h := sftp.InMemHandler()
	b := sftpAt(t, "sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{SFTP: true, SFTPHandlers: &h})+base)
	copyFiles(t, b, map[string]string{"f.txt": "sftp's file", "sub/g.txt": "sftp's"})
	ctx := context.Background()
	mark := len(Log.Lines())
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	entries, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"f.txt", "sub"}) {
		t.Errorf("listed %v, want sftp's f.txt and sub", names)
	}
	if !loggedSince(mark, "the two see other files") {
		t.Error("no line says why the shell is not used")
	}
	if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) != 0 {
		t.Errorf("ChecksumAlgos = %v, %v; want none: the shell sums other files", algos, err)
	}
	if sum, err := b.Checksum(ctx, "f.txt", "sha256"); err == nil {
		t.Errorf("f.txt summed to %s (the shell's file is %s)", sum, sha256Hex("the shell's file"))
	}
	if sums, err := b.ChecksumFiles(ctx, []string{"f.txt"}, "sha256"); err == nil {
		t.Errorf("f.txt summed in a batch to %v", sums)
	}
	if sum, err := b.ChecksumRange(ctx, "f.txt", "sha256", 0, 4); err == nil {
		t.Errorf("a part of f.txt summed to %s", sum)
	}
	if err := b.RemoveAll(ctx, "sub"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat(ctx, "sub"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("sftp's sub after its remove: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "sub", "g.txt")); err != nil {
		t.Errorf("the shell's sub/g.txt after sftp's sub was removed: %v", err)
	}
	if n := countSent(mark, "sftp", "rm -rf "); n != 0 {
		t.Errorf("%d rm -rf sent to a shell that sees other files", n)
	}
	if n := countSent(mark, "sftp", ">>> echo "); n != 1 {
		t.Errorf("the shell was checked %d times, want once: %q", n, sentSince(mark, "sftp"))
	}
}

// S25: where the shell and sftp see one folder, the shell lists, sums and
// removes, as before.
func TestSFTPUsesTheShellWhereItSeesSFTPsFiles(t *testing.T) {
	base := t.TempDir()
	writeFiles(t, base, map[string]string{"sub/f.txt": "x"})
	b := sftpAt(t, "sftp://x:pw@"+startShellServer(t, true)+base)
	ctx := context.Background()
	mark := len(Log.Lines())
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if entries, err := b.List(ctx, "sub"); err != nil || len(entries) != 1 {
		t.Fatalf("List(sub) = %v, %v", entries, err)
	}
	if loggedSince(mark, "READDIR") {
		t.Error("listed by READDIR, not by the shell's find")
	}
	if loggedSince(mark, "not used") || loggedSince(mark, "runs no commands") {
		t.Errorf("the shell was not used: %q", sentSince(mark, "sftp"))
	}
	if sum, err := b.Checksum(ctx, "sub/f.txt", "sha256"); err != nil || sum != sha256Hex("x") {
		t.Errorf("sum %q, %v; want %s", sum, err, sha256Hex("x"))
	}
	if err := b.RemoveAll(ctx, "sub"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "sub")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("sub after its remove: %v", err)
	}
	if n := countSent(mark, "sftp", "rm -rf "); n != 1 {
		t.Errorf("%d rm -rf sent, want 1", n)
	}
}

// S25: a folder that is not there yet is told by the nearest folder above
// it, so its sums are known before its first file: the shell's where both
// see that folder, none where sftp has a root of its own.
func TestSFTPTellsANewFolderByTheOneAboveIt(t *testing.T) {
	ctx := context.Background()
	t.Run("the same files", func(t *testing.T) {
		b := sftpAt(t, "sftp://x:pw@"+startShellServer(t, true)+filepath.Join(t.TempDir(), "new", "deeper"))
		if algos, err := b.ChecksumAlgos(ctx); err != nil || !slices.Contains(algos, "sha256") {
			t.Errorf("ChecksumAlgos = %v, %v; want the shell's", algos, err)
		}
		copyFiles(t, b, map[string]string{"f.txt": "x"})
		if sum, err := b.Checksum(ctx, "f.txt", "sha256"); err != nil || sum != sha256Hex("x") {
			t.Errorf("sum %q, %v; want %s", sum, err, sha256Hex("x"))
		}
	})
	t.Run("other files", func(t *testing.T) {
		h := sftp.InMemHandler()
		addr := sshtest.StartWith(t, sshtest.Options{SFTP: true, SFTPHandlers: &h})
		share := filepath.Join(t.TempDir(), "share") // sftp's alone
		copyFiles(t, sftpAt(t, "sftp://x:pw@"+addr+share), map[string]string{"x.txt": "x"})
		if _, err := os.Lstat(share); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("the shell has the share: %v", err)
		}
		b := sftpAt(t, "sftp://x:pw@"+addr+share+"/new")
		mark := len(Log.Lines())
		if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) != 0 {
			t.Errorf("ChecksumAlgos = %v, %v; want none: the shell has no %s", algos, err, share)
		}
		if !loggedSince(mark, "the shell finds no "+share) {
			t.Errorf("no line says the shell has no %s", share)
		}
	})
}

// S25: a folder that changed between sftp's look and the shell's, as when
// a file is written into it then, is the same folder: it is looked at
// again.
func TestSFTPLooksAgainAtAFolderThatChanged(t *testing.T) {
	realFind, err := exec.LookPath("find")
	if err != nil {
		t.Skip("no find")
	}
	base := t.TempDir()
	long := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(base, long, long); err != nil {
		t.Fatal(err)
	}
	mark := filepath.Join(t.TempDir(), "touched")
	// The first find the shell runs changes the folder's time, after sftp
	// looked at it.
	fakeCommand(t, "find", fmt.Sprintf(`[ -e %[1]s ] || { : > %[1]s; touch %[2]s; }
exec %[3]s "$@"`, shellQuote(mark), shellQuote(base), shellQuote(realFind)))
	b := sftpAt(t, "sftp://x:pw@"+startShellServer(t, true)+base)
	ctx := context.Background()
	logMark := len(Log.Lines())
	if algos, err := b.ChecksumAlgos(ctx); err != nil || !slices.Contains(algos, "sha256") {
		t.Errorf("ChecksumAlgos = %v, %v; want the shell's: %q", algos, err, sentSince(logMark, "sftp"))
	}
	if _, err := os.Stat(mark); err != nil {
		t.Fatal("the fake find never ran")
	}
	if fi, err := os.Stat(base); err != nil || fi.ModTime().Year() == 2001 {
		t.Fatalf("the folder's time was not changed: %v, %v", fi.ModTime(), err)
	}
}

// sftpLooks answers sftp's stat requests from the disk, with what the
// path at is seen as changed by twist.
type sftpLooks struct {
	at    string
	twist func(os.FileInfo) os.FileInfo
}

func (l sftpLooks) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	if r.Method != "Stat" && r.Method != "Lstat" {
		return nil, sftp.ErrSSHFxOpUnsupported
	}
	fi, err := os.Lstat(r.Filepath)
	if err != nil {
		return nil, err
	}
	if r.Filepath == l.at {
		fi = l.twist(fi)
	}
	return oneInfo{fi}, nil
}

// oneInfo is the answer to one stat request.
type oneInfo struct{ fi os.FileInfo }

func (o oneInfo) ListAt(out []os.FileInfo, off int64) (int, error) {
	if off > 0 || len(out) == 0 {
		return 0, io.EOF
	}
	out[0] = o.fi
	return 1, io.EOF
}

// seenAs is a FileInfo with another mode, size and time.
type seenAs struct {
	os.FileInfo
	mode  os.FileMode
	size  int64
	mtime time.Time
}

func (s seenAs) Mode() os.FileMode  { return s.mode }
func (s seenAs) Size() int64        { return s.size }
func (s seenAs) ModTime() time.Time { return s.mtime }
func (s seenAs) IsDir() bool        { return s.mode.IsDir() }

// S25: a folder that differs from the shell's in any one of its mode, size
// and time is another folder, as one of the same mode and size is, whose
// time alone differs.
func TestSFTPTellsAnotherFolderByAnyOfItsLooks(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		twist func(*seenAs)
		same  bool
	}{
		{"nothing", func(*seenAs) {}, true},
		{"mode", func(s *seenAs) { s.mode ^= 0o070 }, false},
		{"size", func(s *seenAs) { s.size++ }, false},
		{"time", func(s *seenAs) { s.mtime = s.mtime.Add(time.Hour) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			h := sftp.InMemHandler()
			h.FileList = sftpLooks{at: base, twist: func(fi os.FileInfo) os.FileInfo {
				s := seenAs{FileInfo: fi, mode: fi.Mode(), size: fi.Size(), mtime: fi.ModTime()}
				tc.twist(&s)
				return s
			}}
			b := sftpAt(t, "sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{SFTP: true, SFTPHandlers: &h})+base)
			mark := len(Log.Lines())
			algos, err := b.ChecksumAlgos(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if used := slices.Contains(algos, "sha256"); used != tc.same {
				t.Errorf("the shell's sums offered: %v (%v), want %v: %q", used, algos, tc.same, sentSince(mark, "sftp"))
			}
			if other := loggedSince(mark, "the two see other files"); other == tc.same {
				t.Errorf("logged that the two see other files: %v, want %v", other, !tc.same)
			}
		})
	}
}

// S25: a check that was stopped, or that lost its connection, says nothing
// of the shell: it is asked again.
func TestSFTPAsksAgainAfterACheckThatDidNotEnd(t *testing.T) {
	base := t.TempDir()
	t.Run("stopped", func(t *testing.T) {
		b := sftpAt(t, "sftp://x:pw@"+startShellServer(t, true)+base)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if ok, err := b.useShell(ctx); ok || !errors.Is(err, context.Canceled) {
			t.Errorf("a stopped check: %v, %v; want no, and the cancel", ok, err)
		}
		if ok, err := b.useShell(context.Background()); !ok || err != nil {
			t.Errorf("the next check: %v, %v; want the shell used", ok, err)
		}
	})
	t.Run("connection lost", func(t *testing.T) {
		b := sftpAt(t, "sftp://x:pw@"+startShellServer(t, true)+base)
		b.client.Close()
		if ok, err := b.useShell(context.Background()); ok || !isConnLost(err) {
			t.Errorf("a check on a lost connection: %v, %v; want no, and the loss", ok, err)
		}
		if s := b.shell.Load(); s != shellUnknown {
			t.Errorf("kept %d after a lost connection, want shellUnknown (%d)", s, shellUnknown)
		}
	})
}

// S25: an account limited to sftp is a server set up as it should be, and
// each connection checks its shell: a plain line says it runs no commands,
// not a warning.
func TestSFTPSaysPlainlyThatAnSFTPOnlyLoginRunsNoCommands(t *testing.T) {
	b := sftpAt(t, "sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{OnlySFTP: true})+t.TempDir())
	mark := len(Log.Lines())
	if algos, err := b.ChecksumAlgos(context.Background()); err != nil || len(algos) != 0 {
		t.Errorf("ChecksumAlgos = %v, %v; want none", algos, err)
	}
	if !loggedSince(mark, "the login runs no commands") {
		t.Error("no line says the login runs no commands")
	}
	if loggedSince(mark, " WARN ") {
		t.Errorf("warned: %q", Log.Lines()[mark:])
	}
}
