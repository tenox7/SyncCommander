package transport

import (
	"context"
	"sync"
	"testing"

	"sc/model"
)

// pausedRun is a recursive listing that sends first, then waits for the
// test before it sends then. A test that fails early still lets it end.
func pausedRun(t *testing.T, first, then func(emit func(string, []model.FileEntry), complete func(string))) (recursiveLister, <-chan struct{}, func()) {
	sent, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	return func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		first(emit, complete)
		close(sent)
		<-release
		then(emit, complete)
		return nil
	}, sent, free
}

func dirs(names ...string) []model.FileEntry {
	var es []model.FileEntry
	for _, n := range names {
		es = append(es, model.FileEntry{Name: n, IsDir: true})
	}
	return es
}

func files(names ...string) []model.FileEntry {
	var es []model.FileEntry
	for _, n := range names {
		es = append(es, model.FileEntry{Name: n})
	}
	return es
}

// W-H2: a write while a run was going dropped the folders it wrote into,
// but the run went on sending them. What it sent after the write was kept
// and, once the run ended, served as the whole folder: the base without
// the folder the copy wrote into, so the tree showed that folder on one
// side only, and a mirror the other way deleted it. Now a folder a write
// changed is listed live, at once and after the run; folders the write did
// not change are still served from the run.
func TestListCacheWriteDuringARunIsListedLive(t *testing.T) {
	c := newListCache()
	run, sent, release := pausedRun(t, func(emit func(string, []model.FileEntry), complete func(string)) {
		emit("", dirs("a"))
		emit("a", files("a1.txt"))
		complete("a")
	}, func(emit func(string, []model.FileEntry), complete func(string)) {
		emit("", dirs("b", "c"))
		emit("b", files("b1.txt"))
		complete("b")
		emit("c", files("c1.txt"))
		complete("c")
		complete("")
	})
	c.start(context.Background(), "", run)
	<-sent
	c.invalidateAncestors("a/new.txt")
	mustServe(t, serveAsync(c, ""), 1, true)
	mustServe(t, serveAsync(c, "a"), 1, true)
	release()
	waitDone(t, c)
	mustServe(t, serveAsync(c, ""), 1, true)
	mustServe(t, serveAsync(c, "a"), 1, true)
	mustServe(t, serveAsync(c, "b"), 1, false)
	mustServe(t, serveAsync(c, "c"), 1, false)
}

// W-H2: a folder removed or moved while the run is going is not served from
// the run, nor is anything under it, though the run sent it after.
func TestListCacheFolderRemovedDuringARunIsListedLive(t *testing.T) {
	for name, write := range map[string]func(*listCache){
		"removed": func(c *listCache) { c.forgetRemoved("a") },
		"moved":   func(c *listCache) { c.forgetRenamed("a", "z") },
	} {
		t.Run(name, func(t *testing.T) {
			c := newListCache()
			run, sent, release := pausedRun(t, func(emit func(string, []model.FileEntry), _ func(string)) {
				emit("", dirs("a", "b"))
				emit("a", dirs("x"))
			}, func(emit func(string, []model.FileEntry), complete func(string)) {
				emit("", dirs("c"))
				emit("a/x", files("y.txt"))
				complete("a/x")
				complete("a")
				emit("b", files("b1.txt"))
				complete("b")
			})
			c.start(context.Background(), "", run)
			<-sent
			write(c)
			release()
			waitDone(t, c)
			for _, dir := range []string{"", "a", "a/x"} {
				mustServe(t, serveAsync(c, dir), 1, true)
			}
			mustServe(t, serveAsync(c, "b"), 1, false)
		})
	}
}

// A run that replaces the one a write went into (a rescan) starts with
// nothing marked: its listing of the written folder is served.
func TestListCacheFreshRunAfterAWriteIsServed(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), _ func(string)) error {
		emit("", dirs("a"))
		<-ctx.Done()
		return ctx.Err()
	})
	c.invalidateAncestors("a/new.txt")
	c.start(model.WithFreshListing(context.Background()), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("", dirs("a"))
		emit("a", files("a1.txt", "new.txt"))
		complete("a")
		return nil
	})
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 2, false)
	mustServe(t, serveAsync(c, ""), 1, false)
}
