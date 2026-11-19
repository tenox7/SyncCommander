package transport

import (
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// cacheKeys is every folder the cache holds a listing of, or a mark for.
func cacheKeys(c *listCache) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	held := maps.Clone(c.complete)
	for k := range c.entries {
		held[k] = true
	}
	return slices.Sorted(maps.Keys(held))
}

// W-M8: a delete or move drops the listings under the path it takes, and
// no other. a/p came with no word that it was whole, a/empty with nothing
// in it, a/q/r/s with no listing of a/q or a/q/r, and a/b/c's parent
// listing went with a write: each goes with a.
func TestListCacheDropsTheTreeAndNoMore(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		for _, d := range []string{"a", "a/b", "a/b/c", "a//x", "a/q/r/s", "ab", "ab/c", "x", "x/a", "x/a/b"} {
			emit(d, files("f.txt"))
			complete(d)
		}
		emit("a/p", files("f.txt"))
		complete("a/empty")
		complete("x/a/empty")
		return nil
	})
	waitDone(t, c)
	c.invalidate("a/b")
	c.invalidateTree("a")
	want := []string{"ab", "ab/c", "x", "x/a", "x/a/b", "x/a/empty"}
	if got := cacheKeys(c); !slices.Equal(got, want) {
		t.Errorf("after a went, the cache holds %q; want %q", got, want)
	}
	c.fill("a/b/c", files("g.txt")) // listed again after the delete
	c.invalidateTree("a/b")
	c.invalidateTree("x/a")
	want = []string{"ab", "ab/c", "x"}
	if got := cacheKeys(c); !slices.Equal(got, want) {
		t.Errorf("after a/b and x/a went, the cache holds %q; want %q", got, want)
	}
}

// A delete of a file, with 200k folders in the cache, looks at none of
// them: it looked at each, under the lock, for each file, and 100k deletes
// with 1M folders took 10^11 steps (W-M8). The bound is 25 looks at all of
// them for 500 deletes, timed here and now.
func TestListCacheDropsAFileInTimeOfItsOwn(t *testing.T) {
	const folders, deletes = 200_000, 500
	c := newListCache()
	for i := range folders {
		c.fill(fmt.Sprintf("d%03d/s%03d", i/1000, i%1000), nil)
	}
	runtime.GC()
	c.mu.Lock()
	t0 := time.Now()
	n := 0
	for k := range c.entries {
		if k == "d000/s000/x" || strings.HasPrefix(k, "d000/s000/x/") {
			n++
		}
	}
	walk := time.Since(t0)
	c.mu.Unlock()
	t0 = time.Now()
	for i := range deletes {
		c.invalidateTree(fmt.Sprintf("d%03d/s%03d/file%d.txt", i%200, i%1000, i))
	}
	took := time.Since(t0)
	t.Logf("%d deletes of a file: %v, %.1f looks at all the folders", deletes, took, float64(took)/float64(walk))
	if took > 25*walk {
		t.Errorf("%d deletes of a file took %v: %.0f looks at all %d folders (%v each); want 25 at most", deletes, took, float64(took)/float64(walk), folders, walk)
	}
	if got := len(cacheKeys(c)); got != folders+n {
		t.Errorf("the cache holds %d folders after deletes of files; want %d", got, folders)
	}
}
