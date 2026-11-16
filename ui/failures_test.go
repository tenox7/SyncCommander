package ui

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// settled hides the log with every line logged so far seen, as by a user
// who read them: the next failure is what opens it.
func settled(m *Model) {
	m.autoOpenLog()
	m.logView.visible = false
}

// U9: the log opened by itself on ERR and FATAL lines only, but the UI
// logs its own failures as FAIL. A failed delete went back to the tree
// with only fail:1 in the status bar to show it; a failed rename the same.
func TestFailedDeleteOpensTheLog(t *testing.T) {
	left := openBackendOrSkip(t, "fake://tiny")
	right := failingRemove{openBackendOrSkip(t, "fake://tiny")}
	m := newTestModel(t, left, right)
	m.scanner.Scan(context.Background(), model.CompareOpts{})
	node := firstBothFile(m.scanner.Tree())
	if node == nil {
		t.Fatal("no file present on both sides")
	}
	settled(m)
	m.Update(runFirst(m.deleteNode(node, model.PresenceBoth)))
	if !m.logView.visible {
		t.Fatal("a delete failed and the log stayed closed")
	}
	// Closed, it stays closed until something new fails, also after the
	// five seconds in which it never opens again.
	press(m, "esc")
	m.logView.closedAt = time.Now().Add(-time.Minute)
	m.autoOpenLog()
	if m.logView.visible {
		t.Error("the log opened again with nothing new in it")
	}
}

// U9: a failed touch (t) logged nothing at all: the code had no error
// branch. Here the older side's file is gone after the scan.
func TestFailedTouchIsLoggedAndShown(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	for _, dir := range []string{left, right} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(right, "a.txt"), old, old); err != nil {
		t.Fatal(err)
	}
	m := scannedModel(t, left, right)
	if err := os.Remove(filepath.Join(right, "a.txt")); err != nil {
		t.Fatal(err)
	}
	settled(m)
	mark, failed := len(transport.Log.Lines()), transport.Log.FailedCount()
	cursorTo(t, m, "a.txt", false)
	cmd := press(m, "t")
	if cmd == nil {
		t.Fatal("t did nothing")
	}
	m.Update(cmd())
	if !slices.ContainsFunc(transport.Log.Lines()[mark:], func(l string) bool {
		return strings.Contains(l, " touch FAIL right a.txt: ")
	}) {
		t.Errorf("the failed touch is not in the log: %q", transport.Log.Lines()[mark:])
	}
	if got := transport.Log.FailedCount() - failed; got != 1 {
		t.Errorf("FAIL count grew by %d, want 1", got)
	}
	if !m.logView.visible {
		t.Error("a touch failed and the log stayed closed")
	}
}
