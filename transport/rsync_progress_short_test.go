package transport

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// R15: a file received in under a second reads the folder no more. Each
// one that ran past the first look, a tenth of a second in, read all of
// it once: a copy of 100k small files into one folder read some 10^10
// names. Such a file is credited whole when it is done.
func TestDownloadProgressLeavesTheFolderOfAShortReceive(t *testing.T) {
	var reads atomic.Int64
	orig := readDirNames
	readDirNames = func(dir string) ([]string, error) {
		reads.Add(1)
		return orig(dir)
	}
	defer func() { readDirNames = orig }()
	dir := t.TempDir()
	for i := range 3 {
		name := fmt.Sprintf("f%d.bin", i)
		var counter atomic.Int64
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			tailDirSize(stop, dir, name, false, NewCappedAdder(&counter, 1<<62), nil, 0)
			close(done)
		}()
		if err := os.WriteFile(filepath.Join(dir, "."+name+".123"), make([]byte, 100), 0o644); err != nil {
			t.Fatal(err)
		}
		time.Sleep(400 * time.Millisecond)
		close(stop)
		<-done
	}
	if n := reads.Load(); n != 0 {
		t.Errorf("%d folder reads for 3 receives of 0.4 s each; want 0", n)
	}
}
