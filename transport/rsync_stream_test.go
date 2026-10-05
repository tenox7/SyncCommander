package transport

import (
	"context"
	"path"
	"slices"
	"strings"
	"testing"

	rsyncpkg "github.com/gokrazy/rsync"

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
		add, finish := fileListStream(scope, emit, complete)
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
