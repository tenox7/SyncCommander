package transport

import (
	"context"
	"fmt"
	"testing"

	"sc/model"
)

// W-M8: the first batch of a folder is the cache's as it came, and a
// second one goes after it. The batch was copied: a folder of 1M files,
// which the stream sends as one batch, was held twice while it came.
func TestListCacheKeepsTheBatchItIsGiven(t *testing.T) {
	c := newListCache()
	batch := make([]model.FileEntry, 3, 8)
	for i := range batch {
		batch[i] = model.FileEntry{RelPath: fmt.Sprintf("d/f%d", i), Name: fmt.Sprintf("f%d", i)}
	}
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("d", batch)
		emit("d", []model.FileEntry{{RelPath: "d/g", Name: "g"}})
		complete("d")
		return nil
	})
	waitDone(t, c)
	got, hit, _, _ := c.lookup("d")
	if !hit || len(got) != 4 {
		t.Fatalf("d holds %d entries (hit %v); want 4", len(got), hit)
	}
	for i, want := range []string{"f0", "f1", "f2", "g"} {
		if got[i].Name != want {
			t.Errorf("entry %d of d is %q; want %q", i, got[i].Name, want)
		}
	}
	if &got[0] != &batch[0] {
		t.Error("the cache holds a copy of d's first batch; want the batch itself")
	}
}
