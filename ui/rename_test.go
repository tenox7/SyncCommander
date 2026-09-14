package ui

import (
	"context"
	"errors"
	"testing"

	"sc/model"
)

type failingRename struct{ model.Backend }

func (failingRename) Rename(context.Context, string, string) error { return errors.New("read-only") }

func firstBothFile(n *model.TreeNode) *model.TreeNode {
	for _, c := range n.Children {
		if !c.IsDir && c.Compare.Presence == model.PresenceBoth {
			return c
		}
		if c.IsDir {
			if f := firstBothFile(c); f != nil {
				return f
			}
		}
	}
	return nil
}

// When only one side of a both-side rename succeeds, the tree must show what
// is really there: the new name on the renamed side, the old on the other.
func TestRenameHalfFailureRefreshesParent(t *testing.T) {
	left := openBackendOrSkip(t, "fake://tiny")
	right := failingRename{openBackendOrSkip(t, "fake://tiny")}
	m := newTestModel(t, left, right)
	m.scanner.Scan(context.Background(), model.CompareOpts{})
	node := firstBothFile(m.scanner.Tree())
	if node == nil {
		t.Fatal("no file present on both sides")
	}
	oldName, parent := node.Name, node.Parent
	m.openRename(node)
	m.input.ed.set(oldName + ".renamed")
	msg := m.input.Confirm()().(renameDoneMsg)
	if msg.err == nil {
		t.Fatal("rename reported success although the right side failed")
	}
	var leftOnly, rightOnly bool
	for _, c := range parent.Children {
		switch {
		case c.Name == oldName+".renamed" && c.Left != nil && c.Right == nil:
			leftOnly = true
		case c.Name == oldName && c.Left == nil && c.Right != nil:
			rightOnly = true
		}
	}
	if !leftOnly || !rightOnly {
		t.Fatalf("tree not refreshed after a half-failed rename: renamed-left=%v old-right=%v", leftOnly, rightOnly)
	}
}
