package transport

import (
	"context"
	"errors"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

func waitDone(t *testing.T, c *listCache) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, _, done := c.lookup(""); done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("preload never finished")
		}
		time.Sleep(time.Millisecond)
	}
}

// A preload that fails midway must not leave its partial listing behind as if
// it were complete: mirror would read the missing files as destination-only.
func TestListCacheFailedPreloadDropsPartialEntries(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), _ func(string)) error {
		emit("", []model.FileEntry{{Name: "a"}})
		emit("d", []model.FileEntry{{Name: "b"}})
		return errors.New("connection reset")
	})
	waitDone(t, c)
	if _, hit, _, _ := c.lookup("d"); hit {
		t.Fatal("partial listing served as a cache hit after a failed preload")
	}
	lived := false
	entries, err := c.serve(context.Background(), "d", func(context.Context, string) ([]model.FileEntry, error) {
		lived = true
		return []model.FileEntry{{Name: "live"}}, nil
	})
	if err != nil || !lived || len(entries) != 1 {
		t.Fatalf("serve = %v, %v (live=%v), want the live listing", entries, err, lived)
	}
}

func TestListCacheSuccessfulPreloadServesHits(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), _ func(string)) error {
		g := &emitGrouper{emit: emit}
		g.add(model.FileEntry{RelPath: "d", Name: "d", IsDir: true})
		g.add(model.FileEntry{RelPath: "d/f", Name: "f"})
		g.add(model.FileEntry{RelPath: "empty", Name: "empty", IsDir: true})
		g.finish()
		return nil
	})
	waitDone(t, c)
	for dir, want := range map[string]int{"": 2, "d": 1, "empty": 0} {
		entries, err := c.serve(context.Background(), dir, func(context.Context, string) ([]model.FileEntry, error) {
			t.Fatalf("dir %q went live although it was preloaded", dir)
			return nil, nil
		})
		if err != nil || len(entries) != want {
			t.Errorf("dir %q: %d entries, err %v, want %d", dir, len(entries), err, want)
		}
	}
}

type serveResult struct {
	entries []model.FileEntry
	lived   bool
	err     error
}

// serveAsync runs serve in the background; the live fallback answers with one
// entry and records that it ran.
func serveAsync(c *listCache, dir string) <-chan serveResult {
	ch := make(chan serveResult, 1)
	go func() {
		var r serveResult
		r.entries, r.err = c.serve(context.Background(), dir, func(context.Context, string) ([]model.FileEntry, error) {
			r.lived = true
			return []model.FileEntry{{Name: "live"}}, nil
		})
		ch <- r
	}()
	return ch
}

func mustServe(t *testing.T, ch <-chan serveResult, want int, lived bool) {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil || len(r.entries) != want || r.lived != lived {
			t.Fatalf("serve = %d entries, err %v, live=%v; want %d entries, live=%v", len(r.entries), r.err, r.lived, want, lived)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
	}
}

func mustBlock(t *testing.T, ch <-chan serveResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("serve returned %d entries (live=%v) for an incomplete dir", len(r.entries), r.lived)
	case <-time.After(50 * time.Millisecond):
	}
}

// A dir the emitter has completed is served while the preload still runs;
// everything else keeps waiting for the end of the run.
func TestListCacheServesCompletedDirMidRun(t *testing.T) {
	c := newListCache()
	release := make(chan struct{})
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}, {Name: "2"}})
		emit("b", []model.FileEntry{{Name: "3"}})
		complete("a")
		<-release
		return nil
	})
	mustServe(t, serveAsync(c, "a"), 2, false)
	b, unknown := serveAsync(c, "b"), serveAsync(c, "unknown")
	mustBlock(t, b)
	mustBlock(t, unknown)
	close(release)
	mustServe(t, b, 1, false)
	mustServe(t, unknown, 1, true)
}

// A dir with entries emitted but not yet completed is neither a hit nor
// served by await until complete arrives.
func TestListCachePartialDirWaitsForComplete(t *testing.T) {
	c := newListCache()
	emitted, more := make(chan struct{}), make(chan struct{})
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}})
		close(emitted)
		<-more
		emit("a", []model.FileEntry{{Name: "2"}})
		complete("a")
		<-more
		return nil
	})
	<-emitted
	if _, hit, _, _ := c.lookup("a"); hit {
		t.Fatal("incomplete dir reported as a hit")
	}
	a := serveAsync(c, "a")
	mustBlock(t, a)
	more <- struct{}{}
	mustServe(t, a, 2, false)
	more <- struct{}{}
	waitDone(t, c)
}

// Depth-first input completes each dir, innermost first, right after its last
// descendant was flushed; an empty dir ends up as a cached hit.
func TestEmitGrouperCompletesDirsDepthFirst(t *testing.T) {
	c := newListCache()
	var log []string
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		g := &emitGrouper{
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
		for _, p := range []string{"a/", "a/sub/", "a/sub/1", "a/x", "b/", "b/1", "c/"} {
			rel := strings.TrimSuffix(p, "/")
			g.add(model.FileEntry{RelPath: rel, Name: path.Base(rel), IsDir: strings.HasSuffix(p, "/")})
		}
		g.finish()
		return nil
	})
	waitDone(t, c)
	want := []string{
		"emit :a", "emit a:sub", "emit a/sub:1", "done a/sub", "emit a:x", "done a",
		"emit :b", "emit b:1", "done b", "emit :c", "done c",
	}
	if !slices.Equal(log, want) {
		t.Fatalf("sequence:\n got %q\nwant %q", log, want)
	}
	mustServe(t, serveAsync(c, "c"), 0, false)
}

// D9: a fresh listing asks the server even when the cache holds the folder.
func TestListCacheFreshListingGoesLive(t *testing.T) {
	c := newListCache()
	ctx := context.Background()
	c.start(ctx, "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("d", []model.FileEntry{{RelPath: "d/old", Name: "old"}})
		complete("d")
		return nil
	})
	waitDone(t, c)
	live := func(context.Context, string) ([]model.FileEntry, error) {
		return []model.FileEntry{{RelPath: "d/new", Name: "new"}}, nil
	}
	if got, _ := c.serve(ctx, "d", live); len(got) != 1 || got[0].Name != "old" {
		t.Errorf("cached listing = %+v", got)
	}
	if got, _ := c.serve(model.WithFreshListing(ctx), "d", live); len(got) != 1 || got[0].Name != "new" {
		t.Errorf("fresh listing = %+v, want the server's", got)
	}
}

// W-H4: a rescan's recursive listing was ignored while an older one was
// still running, and List went on serving the old run, which may have
// listed a folder before the change the rescan is to show. Now a fresh run
// replaces it: the old run is stopped, and what it still sends is dropped.
func TestListCacheFreshPreloadReplacesARunInFlight(t *testing.T) {
	c := newListCache()
	release, oldDone := make(chan struct{}), make(chan error, 1)
	c.start(context.Background(), "", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("d", []model.FileEntry{{RelPath: "d/old", Name: "old"}})
		<-release
		emit("d", []model.FileEntry{{RelPath: "d/late", Name: "late"}})
		complete("d")
		oldDone <- ctx.Err()
		return ctx.Err()
	})
	newRan := make(chan struct{})
	c.start(model.WithFreshListing(context.Background()), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("d", []model.FileEntry{{RelPath: "d/new", Name: "new"}})
		complete("d")
		close(newRan)
		return nil
	})
	select {
	case <-newRan:
	case <-time.After(5 * time.Second):
		t.Error("the fresh recursive listing did not run while the old one was in flight")
	}
	close(release)
	if err := <-oldDone; err == nil {
		t.Error("the old run was not stopped")
	}
	waitDone(t, c)
	got, _ := c.serve(context.Background(), "d", func(context.Context, string) ([]model.FileEntry, error) {
		return []model.FileEntry{{RelPath: "d/live", Name: "live"}}, nil
	})
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	if !slices.Equal(names, []string{"new"}) {
		t.Errorf("d is served as %q, want the fresh run's [new]", names)
	}
}

// invalidateAncestors walks up to the base. A path that starts with "/"
// ended at "/", whose folder is "/": the walk never ended, and held the
// cache locked, so every List of the backend hung behind it. A restic
// RemoveAll("/") did that.
func TestInvalidateAncestorsEnds(t *testing.T) {
	for _, p := range []string{"/", "/a/b", ".", "a/b", ""} {
		c := newListCache()
		c.fill("", nil)
		done := make(chan struct{})
		go func() { c.invalidateAncestors(p); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("invalidateAncestors(%q) did not end", p)
		}
		if _, hit, _, _ := c.lookup(""); hit {
			t.Errorf("invalidateAncestors(%q) kept the base's listing", p)
		}
	}
}
