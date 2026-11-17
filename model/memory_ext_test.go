package model_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// F13: a scanned tree took about 850 bytes for each file on both sides; now
// about 600. The fake tree's names are short, so this is the tree itself.
func TestScannedTreeBytesPerNode(t *testing.T) {
	lb, err := transport.TryOpenBackend("fake://medium", false, 4)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := transport.TryOpenBackend("fake://medium", false, 4)
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	scanner := model.NewScanner(lb, rb, 4, 8, true)
	scanner.Scan(context.Background(), model.CompareOpts{TimeGrace: true, IgnoreTZDST: true})
	model.PropagateStatus(scanner.Tree(), &model.CompareOpts{Size: true, ModTime: true})
	runtime.GC()
	runtime.ReadMemStats(&after)
	p := scanner.Progress()
	nodes := p.TotalFiles + p.TotalDirs
	perNode := int64(after.HeapAlloc-before.HeapAlloc) / nodes
	runtime.KeepAlive(scanner)
	t.Logf("%d bytes per node for %d nodes", perNode, nodes)
	if perNode > 700 {
		t.Errorf("the tree takes %d bytes per node, want at most 700", perNode)
	}
}

// sumCounter counts the checksums a backend computes.
type sumCounter struct {
	model.Backend
	sums atomic.Int64
}

func (c *sumCounter) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	c.sums.Add(1)
	return c.Backend.Checksum(ctx, relPath, algo)
}

// F13: a checksum keeps the time of the file it summed, in 8 bytes now. A
// rescan that finds the files as they were keeps their checksums and sums
// nothing again.
func TestRescanKeepsTheChecksumsOfUnchangedFiles(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 123456789, time.UTC)
	for _, side := range []string{"l", "r"} {
		p := filepath.Join(root, side, "d", "a.txt")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("the same bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	lb := &sumCounter{Backend: transport.NewLocalBackend(filepath.Join(root, "l"))}
	rb := &sumCounter{Backend: transport.NewLocalBackend(filepath.Join(root, "r"))}
	ctx := context.Background()
	opts := model.CompareOpts{Size: true, ModTime: true, Checksum: true, TimeGrace: true}
	sc := model.NewScanner(lb, rb, 1, 1, true)
	sc.Scan(ctx, opts)
	var d *model.TreeNode
	sc.ReadTree(func(root *model.TreeNode) {
		for _, c := range root.Children {
			if c.Name == "d" {
				d = c
			}
		}
	})
	if d == nil {
		t.Fatal("no folder d in the tree")
	}
	if lb.sums.Load()+rb.sums.Load() == 0 {
		sc.ChecksumNode(ctx, d)
	}
	summed := lb.sums.Load() + rb.sums.Load()
	if summed != 2 {
		t.Fatalf("%d checksums computed, want one on each side", summed)
	}
	sc.RescanNode(ctx, d, opts, nil)
	if again := lb.sums.Load() + rb.sums.Load() - summed; again != 0 {
		t.Errorf("a rescan of files that did not change summed %d of them again", again)
	}
}
