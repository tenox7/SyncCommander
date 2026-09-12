package ui

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sc/model"
)

// copyNode works from a snapshot of the tree rather than the live nodes;
// every file the enumeration picked must still land on the destination.
func TestCopyNodeCopiesWholeSubtree(t *testing.T) {
	src := openBackendOrSkip(t, "fake://x?dirs=3&files=4&depth=3")
	dstDir := t.TempDir()
	dst := openBackendOrSkip(t, dstDir)

	m := newTestModel(t, src, dst)
	m.scanner.Scan(context.Background(), false, false, true, true)

	var subtree *model.TreeNode
	for _, c := range m.scanner.Tree().Children {
		if c.IsDir {
			subtree = c
			break
		}
	}
	if subtree == nil {
		t.Fatal("fake tree has no top-level directory")
	}
	want := int64(len(model.CollectCopyFiles(subtree, m.cmpOpts, true)))

	msg := m.copyNode(subtree, true, false)()
	if _, ok := msg.(copyDoneMsg); !ok {
		t.Fatalf("copy returned %T, want copyDoneMsg", msg)
	}
	if failed := m.copyProgress.Failed.Load(); failed != 0 {
		t.Fatalf("%d file(s) failed to copy", failed)
	}

	if got := m.copyProgress.Total.Load(); got != want {
		t.Fatalf("copy enumerated %d entries, want %d", got, want)
	}

	var got int64
	err := filepath.WalkDir(filepath.Join(dstDir, subtree.RelPath), func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			got++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%d files landed on the destination, want %d", got, want)
	}
}
