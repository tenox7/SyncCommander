package transport

import (
	"context"
	"errors"
	"path"
	"slices"
	"strings"
	"testing"

	rsyncpkg "github.com/gokrazy/rsync"
	"github.com/gokrazy/rsync/rsyncclient"

	"sc/model"
)

// rsync lists a directory's children as one run after its parent's run, so a
// dir's own entry is followed by its siblings, not its children. lateChildren
// opens a dir on its first child, completes it once the stream leaves its
// subtree, and completes empty dirs at finish.
func TestEmitGrouperLateChildrenDirRuns(t *testing.T) {
	c := newListCache()
	var log []string
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		g := &emitGrouper{
			lateChildren: true,
			emit: func(parent string, entries []model.FileEntry) {
				var names []string
				for _, e := range entries {
					names = append(names, e.Name)
				}
				log = append(log, "emit "+parent+":"+strings.Join(names, ","))
				emit(parent, entries)
			},
			complete: func(dir string) {
				log = append(log, "done "+dir)
				complete(dir)
			},
		}
		for _, p := range []string{"a/", "b/", "c/", "a/sub/", "a/x", "a/sub/1", "b/1"} {
			rel := strings.TrimSuffix(p, "/")
			g.add(model.FileEntry{RelPath: rel, Name: path.Base(rel), IsDir: strings.HasSuffix(p, "/")})
		}
		g.finish()
		return nil
	})
	waitDone(t, c)
	want := []string{
		"emit :a,b,c", "emit a:sub,x", "emit a/sub:1", "done a/sub", "done a",
		"emit b:1", "done b", "done c",
	}
	if !slices.Equal(log, want) {
		t.Fatalf("sequence:\n got %q\nwant %q", log, want)
	}
	for dir, n := range map[string]int{"": 3, "a": 2, "a/sub": 1, "b": 1, "c": 0} {
		mustServe(t, serveAsync(c, dir), n, false)
	}
}

func fileInfos(names ...string) []rsyncpkg.FileInfo {
	var out []rsyncpkg.FileInfo
	for _, n := range names {
		fi := rsyncpkg.FileInfo{Name: strings.TrimSuffix(n, "/"), Mode: 0o644}
		if strings.HasSuffix(n, "/") {
			fi.Mode = rsyncpkg.S_IFDIR | 0o755
		}
		out = append(out, fi)
	}
	return out
}

// A directory fed through the file list callback is served while the run is
// still receiving the rest of the tree.
func TestFileListStreamReleasesDirMidRun(t *testing.T) {
	c := newListCache()
	release := make(chan struct{})
	c.start(context.Background(), "sub", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
		add, finish := fileListStream(scope, emit, complete, nil)
		for _, fi := range fileInfos(".", "a/", "b/", "a/1", "a/2", "b/1") {
			add(fi)
		}
		<-release
		add(fileInfos("b/2")[0])
		finish()
		return nil
	})
	mustServe(t, serveAsync(c, "sub/a"), 2, false)
	b := serveAsync(c, "sub/b")
	mustBlock(t, b)
	close(release)
	mustServe(t, b, 2, false)
	mustServe(t, serveAsync(c, "sub"), 2, false)
}

// peerLine is a message from the rsync peer as gorsync logs it.
func peerLine(msg string) []byte {
	return []byte("2026/11/16 05:40:56 wire.go:96: rsync: " + msg + "\n")
}

func moduleRel(p string) (string, bool) { return relUnder("/", "/"+strings.TrimPrefix(p, "/")) }

// R3: once the peer reports a problem, folders the stream leaves are held
// until the run ends. Then the folder the message names is listed live and
// the rest is served from the cache.
func TestRsyncListingHoldsAfterPeerMessage(t *testing.T) {
	c := newListCache()
	release := make(chan struct{})
	c.start(context.Background(), "", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
		l := newRsyncListing(scope, emit, complete, moduleRel)
		for _, fi := range fileInfos(".", "a/", "b/", "c/", "a/1", "b/1") {
			l.add(fi)
		}
		l.Write(peerLine(`rsync: [sender] link_stat "b/2" (in mod) failed: Permission denied (13)`))
		l.add(fileInfos("c/1")[0])
		<-release
		return l.finish("RLIST mod/", &rsyncclient.Result{XferErrors: 1, IOErrors: 1})
	})
	mustServe(t, serveAsync(c, "a"), 1, false) // released before the message
	b := serveAsync(c, "b")
	mustBlock(t, b) // the stream left b after the message
	close(release)
	mustServe(t, b, 1, true)                   // b/2 is missing from b: live
	mustServe(t, serveAsync(c, "c"), 1, false) // held, then released
	mustServe(t, serveAsync(c, ""), 3, false)
}

// A folder rsync could not open is listed live, and so is its parent.
func TestRsyncListingUnopenableFolder(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
		l := newRsyncListing(scope, emit, complete, moduleRel)
		for _, fi := range fileInfos(".", "a/", "photos/", "a/1") {
			l.add(fi)
		}
		l.Write(peerLine(`rsync: [sender] opendir "photos" (in mod) failed: Permission denied (13)`))
		l.Write(peerLine(`rsync error: some files/attrs were not transferred (see previous errors) (code 23) at main.c(1338) [sender=3.5.1]`))
		return l.finish("RLIST mod/", &rsyncclient.Result{XferErrors: 2, IOErrors: 1})
	})
	waitDone(t, c)
	mustServe(t, serveAsync(c, "photos"), 1, true)
	mustServe(t, serveAsync(c, ""), 1, true)
	mustServe(t, serveAsync(c, "a"), 1, false)
}

// Problems that cannot be pinned to folders the list holds drop the whole
// run: every folder is then listed live.
func TestRsyncListingWholeRunFails(t *testing.T) {
	for name, tc := range map[string]struct {
		msgs []string
		res  rsyncclient.Result
	}{
		"flag without a message (gorsync's sender)": {nil, rsyncclient.Result{IOErrors: 1}},
		"message without a path":                    {[]string{"rsync: [sender] read errors mapping x"}, rsyncclient.Result{XferErrors: 1}},
		"path outside the list":                     {[]string{`rsync: [sender] opendir "zz/q" (in mod) failed: Permission denied (13)`}, rsyncclient.Result{XferErrors: 1, IOErrors: 1}},
		"more errors counted than seen":             {[]string{`rsync: [sender] opendir "a" (in mod) failed: Permission denied (13)`}, rsyncclient.Result{XferErrors: 2, IOErrors: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			c := newListCache()
			c.start(context.Background(), "", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
				l := newRsyncListing(scope, emit, complete, moduleRel)
				for _, fi := range fileInfos(".", "a/", "a/1") {
					l.add(fi)
				}
				for _, m := range tc.msgs {
					l.Write(peerLine(m))
				}
				err := l.finish("RLIST mod/", &tc.res)
				var unread *unreadDirs
				if err == nil || errors.As(err, &unread) {
					t.Errorf("finish = %v, want a whole-run failure", err)
				}
				return err
			})
			waitDone(t, c)
			mustServe(t, serveAsync(c, "a"), 1, true)
			mustServe(t, serveAsync(c, ""), 1, true)
		})
	}
}

// A file deleted while rsync listed is rightly missing: not a problem.
func TestRsyncListingVanishedFileIsFine(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
		l := newRsyncListing(scope, emit, complete, moduleRel)
		for _, fi := range fileInfos(".", "a/", "a/1") {
			l.add(fi)
		}
		l.Write(peerLine(`file has vanished: "a/2" (in mod)`))
		return l.finish("RLIST mod/", &rsyncclient.Result{XferErrors: 1, IOErrors: 2})
	})
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 1, false)
	mustServe(t, serveAsync(c, ""), 1, false)
}

// Only the peer's messages count, not gorsync's own log lines.
func TestPeerMsgsParsing(t *testing.T) {
	var p peerMsgs
	for _, line := range []string{
		`2026/11/16 05:40:56 clientmaincmd.go:30: dest: "/tmp/rsync-rlist-1/", sources: ["mod/"]`,
		`2026/11/16 05:40:56 wire.go:100: info: skipping non-regular file "x"`,
		`2026/11/16 05:40:56 wire.go:96: rsync: rsync: [sender] opendir "a" (in mod) failed: Permission denied (13)`,
		`rsync: plain`,
	} {
		p.Write([]byte(line + "\n"))
	}
	want := []string{`[sender] opendir "a" (in mod) failed: Permission denied (13)`, "plain"}
	if got := p.problems(); !slices.Equal(got, want) {
		t.Errorf("problems = %q, want %q", got, want)
	}
}
