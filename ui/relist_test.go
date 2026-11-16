package ui

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sc/model"
	"sc/transport"
)

// U12: the re-list after a delete, and r on a file, listed the folder on
// both sides. Inside a folder on one side only, it failed on the other
// side, logged an error, and the tree kept the row of the deleted file.
// Here the other side holds a file of the folder's name: a row of its own.
func TestRelistInsideAOneSidedFolder(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	for p, body := range map[string]string{
		filepath.Join(left, "only", "f.txt"): "f",
		filepath.Join(left, "only", "g.txt"): "g",
		filepath.Join(right, "only"):         "a file",
	} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Scan errors reach the log as in sc itself (main.go).
	logErr := model.LogErr
	model.LogErr = func(proto, msg string) { transport.Log.Add(proto, transport.DirErr, msg) }
	t.Cleanup(func() { model.LogErr = logErr })
	m := scannedModel(t, left, right)
	file := func(rel string) (f *model.TreeNode) {
		m.readTree(func(root *model.TreeNode) {
			f = firstNode(root, func(n *model.TreeNode) bool { return n.RelPath == rel && !n.IsDir })
		})
		return f
	}
	f, g := file("only/f.txt"), file("only/g.txt")
	if f == nil || g == nil {
		t.Fatal("the scan did not list only/")
	}
	settled(m)
	errs := transport.Log.ErrCount()

	m.deleting = true
	m.Update(m.deleteNode(f, model.PresenceLeftOnly)())
	if file("only/f.txt") != nil {
		t.Error("the deleted file is still in the tree")
	}

	if err := os.WriteFile(filepath.Join(left, "only", "g.txt"), []byte("grown"), 0o644); err != nil {
		t.Fatal(err)
	}
	m.scanner.RescanNode(context.Background(), g, *m.cmpOpts, nil)
	var size int64
	m.readTree(func(*model.TreeNode) {
		if e := g.Sides[model.SideLeft].Entry; e != nil {
			size = e.Size
		}
	})
	if size != 5 {
		t.Errorf("r on only/g.txt shows %d bytes, want 5", size)
	}
	if got := transport.Log.ErrCount() - errs; got != 0 {
		t.Errorf("%d errors logged: %q", got, transport.Log.Lines())
	}
}
