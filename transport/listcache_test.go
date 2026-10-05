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
