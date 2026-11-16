package ui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sc/model"
	"sc/transport"
)

type failingRemove struct{ model.Backend }

func (failingRemove) Remove(context.Context, string) error    { return errors.New("read-only") }
func (failingRemove) RemoveAll(context.Context, string) error { return errors.New("read-only") }

// A both-side delete where one side fails must count the failure where the
// user sees it (popup counter, status bar FAIL count) and leave the tree
// showing the side that still has the file.
func TestDeleteHalfFailureIsCounted(t *testing.T) {
	left := openBackendOrSkip(t, "fake://tiny")
	right := failingRemove{openBackendOrSkip(t, "fake://tiny")}
	m := newTestModel(t, left, right)
	m.scanner.Scan(context.Background(), model.CompareOpts{})
	node := firstBothFile(m.scanner.Tree())
	if node == nil {
		t.Fatal("no file present on both sides")
	}
	name, parent := node.Name, node.Parent
	failedBefore := transport.Log.FailedCount()
	if _, ok := m.deleteNode(node, model.PresenceBoth)().(deleteDoneMsg); !ok {
		t.Fatal("delete did not finish")
	}
	if got := m.deleteProgress.Failed.Load(); got != 1 {
		t.Errorf("Failed = %d, want 1", got)
	}
	if got := transport.Log.FailedCount() - failedBefore; got != 1 {
		t.Errorf("status bar FAIL count grew by %d, want 1", got)
	}
	if l, r := sideNames(parent, name); l || !r {
		t.Fatalf("after a half-failed delete: left=%v right=%v, want right only", l, r)
	}
}

// U1: a delete confirmed as "left side only" must not run on both sides when
// a rescan lands before Y. The dialog asks again instead.
func TestDeleteAsksAgainWhenSidesChangeUnderTheDialog(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "x.txt"), "moved")
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	m := scannedModel(t, left, right)
	node := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "x.txt" })
	m.leftPanel.jumpTo(node)
	press(m, "d")
	if !m.confirm.IsOpen() || m.confirm.choiceMode {
		t.Fatal("d on a left-only file did not open the one-side dialog")
	}

	// The move workflow: the copy to the right lands and its rescan merges
	// while the dialog is still open.
	writeTestFile(t, filepath.Join(right, "x.txt"), "moved")
	m.scanner.RefreshTopLevel(context.Background(), *m.cmpOpts)
	if m.presence(node) != model.PresenceBoth {
		t.Fatal("the rescan did not make x.txt present on both sides")
	}

	if cmd := press(m, "y"); cmd != nil {
		runFirst(cmd)
	}
	for _, side := range []string{left, right} {
		if _, err := os.Stat(filepath.Join(side, "x.txt")); err != nil {
			t.Errorf("Y on a stale dialog deleted %s/x.txt", filepath.Base(side))
		}
	}
	if !m.confirm.IsOpen() || !m.confirm.choiceMode {
		t.Fatal("the dialog did not ask again with the new sides")
	}

	if cmd := press(m, "a"); cmd != nil {
		runFirst(cmd)
	}
	if _, err := os.Stat(filepath.Join(left, "x.txt")); !os.IsNotExist(err) {
		t.Error("a (left only) did not delete the left file")
	}
	if _, err := os.Stat(filepath.Join(right, "x.txt")); err != nil {
		t.Error("a (left only) deleted the right file")
	}
}
