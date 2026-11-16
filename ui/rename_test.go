package ui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
)

type failingRename struct{ model.Backend }

func (failingRename) Rename(context.Context, string, string) error { return errors.New("read-only") }

// renameOnce lets the first rename through and fails every later one, so the
// undo of a half-failed both-side rename fails.
type renameOnce struct {
	model.Backend
	n int
}

func (r *renameOnce) Rename(ctx context.Context, oldRel, newRel string) error {
	r.n++
	if r.n > 1 {
		return errors.New("gone away")
	}
	return r.Backend.Rename(ctx, oldRel, newRel)
}

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

// renameBothSides renames the first both-side file and returns its old name,
// its parent and the result.
func renameBothSides(t *testing.T, left, right model.Backend) (oldName string, parent *model.TreeNode, msg renameDoneMsg) {
	t.Helper()
	m := newTestModel(t, left, right)
	m.scanner.Scan(context.Background(), model.CompareOpts{})
	node := firstBothFile(m.scanner.Tree())
	if node == nil {
		t.Fatal("no file present on both sides")
	}
	oldName, parent = node.Name, node.Parent
	m.openRename(node)
	m.input.ed.set(oldName + ".renamed")
	return oldName, parent, m.input.Confirm()().(renameDoneMsg)
}

// sideNames reports which sides hold a child of that name.
func sideNames(parent *model.TreeNode, name string) (left, right bool) {
	for _, c := range parent.Children {
		if c.Name == name {
			left, right = c.Sides[model.SideLeft].Entry != nil, c.Sides[model.SideRight].Entry != nil
		}
	}
	return left, right
}

// When the right side of a both-side rename fails, the left rename is undone
// so the sides stay in step.
func TestRenameRightFailureUndoesLeft(t *testing.T) {
	left := openBackendOrSkip(t, "fake://tiny")
	right := failingRename{openBackendOrSkip(t, "fake://tiny")}
	oldName, parent, msg := renameBothSides(t, left, right)
	if msg.err == nil {
		t.Fatal("rename reported success although the right side failed")
	}
	if l, r := sideNames(parent, oldName); !l || !r {
		t.Fatalf("old name not back on both sides: left=%v right=%v", l, r)
	}
	if l, r := sideNames(parent, oldName+".renamed"); l || r {
		t.Fatalf("new name survived the undo: left=%v right=%v", l, r)
	}
}

// When the undo fails too, the tree must show what is really there: the new
// name on the renamed side, the old on the other.
func TestRenameHalfFailureRefreshesParent(t *testing.T) {
	left := &renameOnce{Backend: openBackendOrSkip(t, "fake://tiny")}
	right := failingRename{openBackendOrSkip(t, "fake://tiny")}
	oldName, parent, msg := renameBothSides(t, left, right)
	if msg.err == nil {
		t.Fatal("rename reported success although the right side failed")
	}
	if l, r := sideNames(parent, oldName+".renamed"); !l || r {
		t.Fatalf("renamed name: left=%v right=%v, want left only", l, r)
	}
	if l, r := sideNames(parent, oldName); l || !r {
		t.Fatalf("old name: left=%v right=%v, want right only", l, r)
	}
}

// F28: e onto a name that exists on a side being renamed is refused before
// any backend call; the dialog stays open and says why.
func TestRenameRefusesTakenName(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	for _, side := range []string{left, right} {
		writeTestFile(t, filepath.Join(side, "a.txt"), "AAA")
		writeTestFile(t, filepath.Join(side, "b.txt"), "BBB")
	}
	m := scannedModel(t, left, right)
	m.openRename(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "a.txt" }))
	m.input.ed.set("b.txt")
	if cmd := m.input.Confirm(); cmd != nil {
		cmd()
	}
	if !m.input.IsOpen() || !strings.Contains(m.input.err, "already exists on both sides") {
		t.Errorf("dialog open=%v err=%q, want it open with the reason", m.input.IsOpen(), m.input.err)
	}
	for _, side := range []string{left, right} {
		if b, _ := os.ReadFile(filepath.Join(side, "b.txt")); string(b) != "BBB" {
			t.Errorf("%s/b.txt = %q after a refused rename", filepath.Base(side), b)
		}
	}
}

// A name used only on the other side is free: renaming a right-only file to
// the name of a left-only one joins the two rows.
func TestRenameOntoOtherSidesNameIsAllowed(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "y.txt"), "same")
	writeTestFile(t, filepath.Join(right, "x.txt"), "same")
	m := scannedModel(t, left, right)
	m.openRename(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "x.txt" }))
	m.input.ed.set("y.txt")
	cmd := m.input.Confirm()
	if cmd == nil || m.input.IsOpen() {
		t.Fatalf("rename refused: %q", m.input.err)
	}
	if msg := cmd().(renameDoneMsg); msg.err != nil {
		t.Fatal(msg.err)
	}
	if _, err := os.Stat(filepath.Join(right, "y.txt")); err != nil {
		t.Error("right x.txt was not renamed to y.txt")
	}
}
