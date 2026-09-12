package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sc/model"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func childNamed(n *model.TreeNode, name string) *model.TreeNode {
	for _, c := range n.Children {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// A shallow scan leaves nested dirs unlisted, and both the copy set and the
// mirror-delete set are read straight off the in-memory tree — so an unlisted
// subtree copies nothing and mirrors away nothing. EnsureSubtreeListed is what
// the copy path runs first to close that hole.
func TestShallowScanHidesSubtreeUntilListed(t *testing.T) {
	l, r := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(l, "a/b/c/deep.txt"), "hello")
	writeFile(t, filepath.Join(r, "a/orphan.txt"), "stale")
	writeFile(t, filepath.Join(r, "a/orphandir/y.txt"), "stale")

	s := model.NewScanner(NewLocalBackend(l), NewLocalBackend(r), 4, 8, false)
	s.Scan(context.Background(), false, false, false, false)

	node := childNamed(s.Tree(), "a")
	if node == nil {
		t.Fatal("dir a missing from the scanned tree")
	}
	opts := &model.CompareOpts{Size: true, ModTime: true}

	if got := len(model.CollectCopyFiles(node, opts, true)); got != 0 {
		t.Fatalf("shallow tree yields %d files to copy; test needs an unlisted subtree", got)
	}
	if f, d := model.CountMirrorDeletes(node, true); f != 0 || d != 0 {
		t.Fatalf("shallow tree yields %d/%d mirror deletes; test needs an unlisted subtree", f, d)
	}

	if !s.EnsureSubtreeListed(context.Background(), node, false, false, false) {
		t.Fatal("EnsureSubtreeListed reported failure")
	}

	files := model.CollectCopyFiles(node, opts, true)
	if len(files) != 1 || files[0].RelPath != "a/b/c/deep.txt" {
		t.Errorf("copy set = %v, want [a/b/c/deep.txt]", relPaths(files))
	}
	f, d := model.CountMirrorDeletes(node, true)
	if f != 2 || d != 1 {
		t.Errorf("mirror deletes = %d files, %d dirs; want 2, 1", f, d)
	}
	if got := relPaths(model.CollectMirrorDeletes(node, true)); len(got) != 2 {
		t.Errorf("mirror delete roots = %v, want orphan.txt and orphandir", got)
	}
}

func relPaths(nodes []*model.TreeNode) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.RelPath
	}
	return out
}
