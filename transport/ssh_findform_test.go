package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// S18: the lister is found by a probe that names no path: GNU find where
// find has what the find form uses, perl elsewhere. sc read the error of
// the listing instead, and one that named a folder with "unreadable",
// "printf" or "executable" in it, a folder that was not there, switched
// it to perl for good. A first stat finds it too, and a first listing
// that works: then the probe is not sent again, and where find is not GNU
// find the listing runs again in perl.
func TestShellListerIsFoundByAProbe(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	addr := startExecSSHServer(t)
	want := "perl"
	if gnuFindHere() {
		want = "find"
	}
	for _, first := range []string{"listing", "missing", "stat"} {
		t.Run(first, func(t *testing.T) {
			b, err := OpenBackend("scp://x:pw@"+addr+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			sh := &b.(*SCPBackend).sshShell
			ctx := context.Background()
			missing := []string{"unreadable", "printf", "executable"}
			if first == "listing" {
				missing = nil
			}
			for _, name := range missing {
				if first == "missing" {
					if names, err := listNames(ctx, b, name); err == nil {
						t.Errorf("List(%q) of a folder that is not there = %q and no error", name, names)
					}
				} else if _, err := sh.Stat(ctx, name); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("Stat(%q) of a name that is not there = %v; want not there", name, err)
				}
			}
			if names, err := listNames(ctx, b, "docs"); err != nil || !slices.Equal(names, []string{"a.txt"}) {
				t.Errorf("List(docs) = %q, %v; want [a.txt]", names, err)
			}
			if got := listerOf(sh); got != want {
				t.Errorf("after the names that are not there sc lists with %s; want %s", got, want)
			}
		})
	}
}

// S18: a file deleted while the lister lists its folder is left out. find
// said "No such file or directory" and exited 1, and the whole recursive
// listing was thrown away: one temporary file that went away during a
// listing of a large tree, and the scan listed each folder by itself. The
// files of the folders here go and come back while the tree is listed
// again and again.
func TestShellListingLeavesOutAFileDeletedWhileItLists(t *testing.T) {
	root := t.TempDir()
	const dirs, files = 6, 300
	for d := range dirs {
		dir := filepath.Join(root, fmt.Sprintf("d%d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for f := range files {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", f)), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	addr := startExecSSHServer(t)
	for _, lister := range []string{"find", "perl"} {
		t.Run(lister, func(t *testing.T) {
			b := openShellBackend(t, "scp", addr, root, lister).(*SCPBackend)
			defer b.Close()
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					dir := filepath.Join(root, fmt.Sprintf("d%d", i%dirs))
					for f := range files {
						os.Remove(filepath.Join(dir, fmt.Sprintf("f%d", f)))
					}
					for f := range files {
						os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", f)), nil, 0o644)
					}
					select {
					case <-stop:
						return
					default:
					}
				}
			}()
			ctx := context.Background()
			failed, runs := 0, 0
			for end := time.Now().Add(4 * time.Second); runs < 25 && time.Now().Before(end); runs++ {
				err := b.findRecursive(ctx, "", func(string, []model.FileEntry) {}, func(string) {})
				if err != nil {
					if failed == 0 {
						t.Errorf("a listing failed: %v", err)
					}
					failed++
				}
			}
			close(stop)
			wg.Wait()
			if failed > 0 {
				t.Errorf("%d of %d listings failed while files went and came back", failed, runs)
			}
		})
	}
}
