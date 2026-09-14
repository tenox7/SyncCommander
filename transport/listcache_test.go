package transport

import (
	"context"
	"errors"
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
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry)) error {
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
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry)) error {
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
