package ui

import (
	"context"
	"errors"
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
