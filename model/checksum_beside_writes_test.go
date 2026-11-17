package model_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// F27: a write may run beside a checksum pass (D11). A delete or a touch
// lists its folder again (RelistDir), and a rename renames its node
// (RenameNode). Both change nodes under the lock: a relist their entries
// and their presence, a rename also their path. The pass read the entry
// and the path of the folder it fetches sums for and of each file it
// sums, and a file's presence, without the lock. Run with -race: those
// reads were data races. Once the writes stop, a pass sums every file.
func TestChecksumPassBesideRelistsAndRenames(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, root := range []string{left, right} {
		dir := filepath.Join(root, "d", "e")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for i := range 20 {
			p := filepath.Join(dir, fmt.Sprintf("f%02d.txt", i))
			if err := os.WriteFile(p, []byte("same body"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(p, at, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The left side fetches a folder's sums in bulk, with none for f00:
	// that one it sums by itself. The right side sums each file.
	lb := &prefetchingBackend{Backend: transport.NewLocalBackend(left), missing: "d/e/f00.txt"}
	s := model.NewScanner(lb, transport.NewLocalBackend(right), 4, 4, true)
	opts := model.CompareOpts{Size: true, ModTime: true}
	ctx := context.Background()
	s.Scan(ctx, opts)
	var d, f01 *model.TreeNode
	s.ReadTree(func(root *model.TreeNode) {
		d = childNamed(root, "d")
		f01 = childNamed(childNamed(d, "e"), "f01.txt")
	})
	if f01 == nil {
		t.Fatal("the scan did not list d/e/f01.txt")
	}

	passes := make(chan struct{})
	go func() {
		defer close(passes)
		for range 10 {
			s.ChecksumNode(ctx, d) // a folder: the left side fetches e's sums
			for range 20 {
				s.ChecksumNode(ctx, f01) // one file
			}
		}
	}()
	for done := false; !done; {
		select {
		case <-passes:
			done = true
		default:
			s.RelistDir(ctx, "d", opts)   // e's entries
			s.RelistDir(ctx, "d/e", opts) // the files' entries and presence
			s.RenameNode(f01, "g01.txt", "d/e/g01.txt", "d/e/f01.txt", opts)
			s.RenameNode(f01, "f01.txt", "d/e/f01.txt", "d/e/g01.txt", opts)
		}
	}

	s.ChecksumNode(ctx, d)
	st := readChecksumState(s)
	for i := range 20 {
		rel := fmt.Sprintf("d/e/f%02d.txt", i)
		if got := st.status[rel]; got != model.AttrEqual {
			t.Errorf("%s is %s after a pass with no write beside it, want equal", rel, statusName(got))
		}
	}
	if len(st.pending) > 0 {
		t.Errorf("folders still marked as being summed: %q", st.pending)
	}
}

func childNamed(n *model.TreeNode, name string) *model.TreeNode {
	if n == nil {
		return nil
	}
	for _, c := range n.Children {
		if c.Name == name {
			return c
		}
	}
	return nil
}
