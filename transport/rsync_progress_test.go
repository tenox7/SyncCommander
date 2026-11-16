package transport

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// R12: a full receive writes rsync's temp file and renames it at the end,
// so the file under the name is the old version until then. The progress
// counted that file from the start, as if the copy were mostly done at
// once; it also counted sc's own part file, and found no temp file for a
// name with [ in it (the pattern was globbed). A receive in place (--append)
// still counts the file's growth beyond the placeholder.
func TestDownloadProgressCountsWhatRsyncWrites(t *testing.T) {
	for _, tc := range []struct {
		what, name string
		inplace    bool
		skip       int64
		before     map[string]int // files there at the start, by name, and their sizes
		during     map[string]int // written while the receive runs
		want       int64
	}{
		{"full receive over an old version", "f.txt", false, 0,
			map[string]int{"f.txt": 5000, model.PartName("f.txt"): 4000}, map[string]int{".f.txt.1234567": 300}, 300},
		{"full receive of a name with [ ]", "a [1].txt", false, 0,
			nil, map[string]int{".a [1].txt.99": 700}, 700},
		{"append in place", "g.txt", true, 1000,
			map[string]int{"g.txt": 1000}, map[string]int{"g.txt": 1500}, 500},
	} {
		dir := t.TempDir()
		for name, n := range tc.before {
			writeFile(t, filepath.Join(dir, name), strings.Repeat("x", n))
		}
		var counter atomic.Int64
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			tailDirSize(stop, dir, tc.name, tc.inplace, NewCappedAdder(&counter, 1<<62), nil, tc.skip)
			close(done)
		}()
		time.Sleep(250 * time.Millisecond)
		if n := counter.Load(); n != 0 {
			t.Errorf("%s: before rsync wrote anything the progress is %d bytes", tc.what, n)
		}
		for name, n := range tc.during {
			writeFile(t, filepath.Join(dir, name), strings.Repeat("y", n))
		}
		close(stop)
		<-done
		if n := counter.Load(); n != tc.want {
			t.Errorf("%s: the progress is %d bytes, want %d", tc.what, n, tc.want)
		}
	}
}
