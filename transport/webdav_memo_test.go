package transport

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// W-M8: a delete forgets the folders under the path it took, from the memo
// of folders that are there, and no other. a/q and a/q/r were never known:
// a/q/r/s goes with a all the same.
func TestWebDAVForgetsTheFoldersUnderADeleteAndNoMore(t *testing.T) {
	b := &WebDAVBackend{}
	for _, d := range []string{"a", "a/b/c", "a//x", "a/q/r/s", "ab", "ab/c", "x/a"} {
		b.knowDir(d)
	}
	b.forgetDirs("a/b/c/f.txt") // a file: no folder under it
	if !b.knowsDir("a/b/c") {
		t.Error("a delete of a/b/c/f.txt forgot a/b/c")
	}
	b.forgetDirs("/a/")
	for d, want := range map[string]bool{"a": false, "a/b/c": false, "a//x": false, "a/q/r/s": false, "ab": true, "ab/c": true, "x/a": true} {
		if got := b.knowsDir(d); got != want {
			t.Errorf("after a delete of a, %s known = %v, want %v", d, got, want)
		}
	}
	b.forgetDirs("")
	for _, d := range []string{"ab", "ab/c", "x/a"} {
		if b.knowsDir(d) {
			t.Errorf("%s outlived a delete of the base", d)
		}
	}
}

// A delete of a file, with 200k folders known, looks at none of them: it
// looked at each for each file, and 100k deletes with 1M folders known took
// 10^11 steps (W-M8). The bound is 25 looks at all of them for 500
// deletes, timed here and now on a map of the same names.
func TestWebDAVForgetsAFileInTimeOfItsOwn(t *testing.T) {
	const folders, deletes = 200_000, 500
	b := &WebDAVBackend{}
	names := make(map[string]struct{}, folders)
	for i := range folders {
		d := fmt.Sprintf("d%03d/s%03d", i/1000, i%1000)
		b.knowDir(d)
		names[d] = struct{}{}
	}
	runtime.GC()
	t0 := time.Now()
	n := 0
	for k := range names {
		if k == "d000/s000/x" || strings.HasPrefix(k, "d000/s000/x/") {
			n++
		}
	}
	walk := time.Since(t0)
	t0 = time.Now()
	for i := range deletes {
		b.forgetDirs(fmt.Sprintf("d%03d/s%03d/file%d.txt", i%200, i%1000, i))
	}
	took := time.Since(t0)
	t.Logf("%d deletes of a file: %v, %.1f looks at all the folders", deletes, took, float64(took)/float64(walk))
	if took > 25*walk {
		t.Errorf("%d deletes of a file took %v: %.0f looks at all %d folders (%v each); want 25 at most", deletes, took, float64(took)/float64(walk), folders+n, walk)
	}
	for _, d := range []string{"d000/s000", "d199/s999"} {
		if !b.knowsDir(d) {
			t.Errorf("deletes of files forgot %s", d)
		}
	}
}
