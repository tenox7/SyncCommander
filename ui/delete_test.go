package ui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
	if _, ok := runFirst(m.deleteNode(node, model.PresenceBoth)).(opDoneMsg); !ok {
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
	m.left.panel.jumpTo(node)
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

// logSeen counts the log's problems so far as seen: errors other tests
// logged would pop the log open over the test's keys.
func logSeen(m *Model) {
	m.logView.lastSeenErrs, m.logView.lastSeenFatal = problems(), transport.Log.FatalCount()
}

// T1: in the dialog of a file on both sides, l deletes the right copy
// alone and b both; no test pressed either. While the delete runs, its
// popup names the sides.
func TestDeleteChoiceKeysPickTheSides(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	for _, side := range []string{left, right} {
		writeTestFile(t, filepath.Join(side, "r.txt"), "R")
		writeTestFile(t, filepath.Join(side, "b.txt"), "B")
	}
	m := scannedModel(t, left, right)
	logSeen(m)
	for _, c := range []struct{ name, key, label string }{{"r.txt", "l", "RIGHT"}, {"b.txt", "b", "BOTH"}} {
		m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == c.name }))
		press(m, "d")
		if !m.confirm.IsOpen() || !m.confirm.choiceMode {
			t.Fatalf("d on %s did not open the both-sides dialog", c.name)
		}
		cmd := press(m, c.key)
		if cmd == nil {
			t.Fatalf("%s in the dialog of %s started nothing", c.key, c.name)
		}
		if !strings.Contains(m.View(), "DELETE "+c.label) {
			t.Errorf("while %s is deleted, the screen shows no DELETE %s popup", c.name, c.label)
		}
		m.Update(runFirst(cmd))
	}
	for _, f := range []struct {
		side, name string
		want       bool
	}{{left, "r.txt", true}, {right, "r.txt", false}, {left, "b.txt", false}, {right, "b.txt", false}} {
		if _, err := os.Stat(filepath.Join(f.side, f.name)); (err == nil) != f.want {
			t.Errorf("%s/%s: on disk %v, want %v", filepath.Base(f.side), f.name, err == nil, f.want)
		}
	}
}

// The recursive delete dialog says what the folder holds: an empty folder
// says so, and a folder the scan did not list says its counts are only
// what is known ("not fully scanned").
func TestDeleteDialogSaysWhatTheFolderHolds(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "big", "a.txt"), "a")
	writeTestFile(t, filepath.Join(left, "big", "sub", "b.txt"), "b")
	for _, d := range []string{filepath.Join(left, "empty"), right} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		deep       bool
		name, want string
	}{
		{true, "big", "2 files, 1 folder"},
		{true, "empty", "empty folder"},
		{false, "big", "0+ files, 0+ folders (not fully scanned)"},
	} {
		lb, rb := openBackendOrSkip(t, left), openBackendOrSkip(t, right)
		m := NewModel(lb, rb, lb.BasePath(), rb.BasePath(), &model.CompareOpts{Size: true, ModTime: true, TimeGrace: true}, false, c.deep, 2, 8, false, false, false)
		m.width, m.height = 120, 40
		m.layoutPanels()
		m.scanner.Scan(context.Background(), *m.cmpOpts)
		m.refreshTreeNow()
		logSeen(m)
		m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == c.name }))
		press(m, "d")
		if !m.confirm.IsOpen() || !slices.Contains(m.confirm.lines, c.want) {
			t.Errorf("deep=%v, d on %s: dialog %q, want a line %q", c.deep, c.name, m.confirm.lines, c.want)
		}
		press(m, "esc")
	}
}
