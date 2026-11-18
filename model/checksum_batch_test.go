package model_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// batchingBackend sums many files with one call, as the ssh shell does,
// and counts its calls and the sums it is asked for one file at a time. Its
// batches have no sum for the file missing names.
type batchingBackend struct {
	model.Backend
	missing string
	perFile atomic.Int64
	mu      sync.Mutex
	batches [][]string
}

func (b *batchingBackend) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.Backend.Capabilities(ctx)
	caps.BatchSums = true
	return caps, err
}

func (b *batchingBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	b.perFile.Add(1)
	return b.Backend.Checksum(ctx, relPath, algo)
}

func (b *batchingBackend) ChecksumFiles(ctx context.Context, rels []string, algo string) (map[string]string, error) {
	b.mu.Lock()
	b.batches = append(b.batches, slices.Clone(rels))
	b.mu.Unlock()
	sums := map[string]string{}
	for _, rel := range rels {
		if rel == b.missing {
			continue
		}
		sum, err := b.Backend.Checksum(ctx, rel, algo)
		if err != nil {
			return sums, err
		}
		sums[rel] = sum
	}
	return sums, nil
}

// S19: a backend that sums many files with one request was asked for each
// file by itself, a round trip for each: hours for a million small files
// at 50 ms. The scan gives it the files in batches of at most 1024, each
// file once, the files outside any folder too, and sums by itself only a
// file a batch has no sum for, and a file with nothing to batch with.
func TestScanSumsInBatches(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	var files []string
	for i := range 1100 {
		files = append(files, fmt.Sprintf("big/f%04d.txt", i))
	}
	files = append(files, "small/a.txt", "small/b.txt", "top1.txt", "top2.txt", "one/only.txt")
	for _, rel := range files {
		for _, root := range []string{left, right} {
			p := filepath.Join(root, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte("body of "+rel), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(p, at, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	bb := &batchingBackend{Backend: transport.NewLocalBackend(right), missing: "small/b.txt"}
	sc := model.NewScanner(transport.NewLocalBackend(left), bb, 4, 1, true)
	sc.Scan(context.Background(), model.CompareOpts{Size: true, ModTime: true, Checksum: true})

	st := readChecksumState(sc)
	for _, rel := range files {
		if st.status[rel] != model.AttrEqual {
			t.Errorf("%s is %s by checksum, want equal", rel, statusName(st.status[rel]))
		}
	}
	asked := map[string]int{}
	for _, batch := range bb.batches {
		if len(batch) > 1024 {
			t.Errorf("a batch of %d files, want 1024 at most", len(batch))
		}
		for _, rel := range batch {
			asked[rel]++
		}
	}
	for _, rel := range files {
		if want := map[bool]int{true: 0, false: 1}[rel == "one/only.txt"]; asked[rel] != want {
			t.Errorf("%s was in %d batches, want %d", rel, asked[rel], want)
		}
	}
	if n := len(bb.batches); n != 4 {
		t.Errorf("%d batches, want 4: big in 1024 and 76, small, the two top files", n)
	}
	if n := bb.perFile.Load(); n != 2 {
		t.Errorf("%d sums asked one file at a time, want 2: small/b.txt, which its batch had none for, and one/only.txt", n)
	}
	if p := sc.Progress(); p.ChecksumDone != int64(len(files)) || p.ChecksumFiles != int64(len(files)) {
		t.Errorf("the scan counts %d of %d files summed, want %d of %d", p.ChecksumDone, p.ChecksumFiles, len(files), len(files))
	}
	// A folder shows a sum in flight while a batch of its files is: none
	// once the scan is done.
	sc.ReadTree(func(root *model.TreeNode) {
		var walk func(n *model.TreeNode)
		walk = func(n *model.TreeNode) {
			for side := range n.ChecksumInFlight {
				if c := n.ChecksumInFlight[side].Load(); c != 0 {
					t.Errorf("%q shows %d sums in flight after the scan", n.RelPath, c)
				}
			}
			for _, c := range n.Children {
				walk(c)
			}
		}
		walk(root)
	})
}
