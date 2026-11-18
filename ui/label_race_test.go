//go:build race

package ui

import (
	"path/filepath"
	"testing"

	"sc/model"
)

// U25: the log label of a rescan and of a listing, and the rescan of the
// cursor, read node.RelPath without the tree lock, and a rename writes it
// from its own goroutine: a rescan that waited behind a scan starts while
// a rename may run. Under -race this test saw those reads beside
// RenameNode's writes. It runs only under -race.
func TestRescanLabelsAreReadUnderTheLock(t *testing.T) {
	root := t.TempDir()
	for _, side := range []string{"L", "R"} {
		writeTestFile(t, filepath.Join(root, side, "d", "f.txt"), "x")
	}
	m := scannedModel(t, filepath.Join(root, "L"), filepath.Join(root, "R"))
	settled(m)
	cursorTo(t, m, "d", true)
	var d *model.TreeNode
	m.readTree(func(tree *model.TreeNode) {
		d = firstNode(tree, func(n *model.TreeNode) bool { return n.RelPath == "d" })
	})
	if d == nil {
		t.Fatal("no d in the tree")
	}
	opts := *m.cmpOpts
	renamed := make(chan struct{})
	go func() { // a rename in flight, back and forth
		defer close(renamed)
		for i := range 2000 {
			if i%2 == 0 {
				m.scanner.RenameNode(d, "e", "e", "d", opts)
			} else {
				m.scanner.RenameNode(d, "d", "d", "e", opts)
			}
		}
	}()
	// The loop does not sync with the renames, only through the tree lock:
	// a failed receive orders nothing.
	for done := false; !done; {
		select {
		case <-renamed:
			done = true
		default:
		}
		m.rescanNode(d, nil)
		m.listNode(d)
		m.rescanCursor()
	}
	m.cancelOps()
}
