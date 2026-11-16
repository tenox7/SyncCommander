package transport

import (
	"os"
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
// still counts the file's growth beyond the placeholder. A temp file a
// killed run left is not this receive's, nor is the temp file of another
// name that starts with this one.
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
		{"full receive next to a killed run's temp file", "h.txt", false, 0,
			map[string]int{".h.txt.111": 9000}, map[string]int{".h.txt.222": 300}, 300},
		{"full receive of a name another name starts with", "a", false, 0,
			nil, map[string]int{".a.b.123": 500, ".a.456": 300}, 300},
	} {
		dir := t.TempDir()
		hourAgo := time.Now().Add(-time.Hour)
		for name, n := range tc.before {
			writeFile(t, filepath.Join(dir, name), strings.Repeat("x", n))
			if err := os.Chtimes(filepath.Join(dir, name), hourAgo, hourAgo); err != nil {
				t.Fatal(err)
			}
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
		// The folder is read at most once a second (F9): give it time.
		for deadline := time.Now().Add(3 * time.Second); counter.Load() < tc.want && time.Now().Before(deadline); {
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(300 * time.Millisecond)
		close(stop)
		<-done
		if n := counter.Load(); n != tc.want {
			t.Errorf("%s: the progress is %d bytes, want %d", tc.what, n, tc.want)
		}
	}
}

// F9: the progress of an rsync download read the whole folder ten times a
// second for every file, to find rsync's temp file there: in a folder of
// 500,000 names, a full read each time. It reads the folder only to find
// the temp file, at most once a second, and not at all for a file that is
// done before the first look.
func TestDownloadProgressReadsTheFolderSeldom(t *testing.T) {
	var reads atomic.Int64
	orig := readDirNames
	readDirNames = func(dir string) ([]string, error) {
		reads.Add(1)
		return orig(dir)
	}
	defer func() { readDirNames = orig }()
	dir := t.TempDir()
	tail := func(name string, during func()) int64 {
		var counter atomic.Int64
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			tailDirSize(stop, dir, name, false, NewCappedAdder(&counter, 1<<62), nil, 0)
			close(done)
		}()
		during()
		close(stop)
		<-done
		return counter.Load()
	}

	got := tail("big.bin", func() {
		f, err := os.Create(filepath.Join(dir, ".big.bin.4821993"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		for range 20 { // 2 s of a temp file that grows
			if _, err := f.Write(make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	if n := reads.Load(); n > 2 {
		t.Errorf("%d folder reads in 2 s of one download, want at most 2", n)
	}
	if got == 0 {
		t.Error("the growing temp file was not counted")
	}

	reads.Store(0)
	tail("small.bin", func() { time.Sleep(20 * time.Millisecond) })
	if n := reads.Load(); n != 0 {
		t.Errorf("%d folder reads for a file done before the first look, want 0", n)
	}
}
