package ui

import (
	"testing"

	"sc/model"
)

// closeRecorder is a backend that remembers it was closed.
type closeRecorder struct {
	model.Backend
	closed bool
}

func (c *closeRecorder) Close() error {
	c.closed = true
	return c.Backend.Close()
}

// The program closes the backends the panes hold when it ends. It closed
// the ones it started with, which a reopen (a new location in a pane)
// replaces: the new ones were never closed, and an rclone remote finishes
// its work, such as dropbox's batched uploads, only when it is.
func TestModelCloseClosesWhatThePanesHoldNow(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	started := m.right.backend
	if _, errMsg := m.reopenBackends(m.left.arg, "fake://small"); errMsg != "" {
		t.Fatal(errMsg)
	}
	m.cancelOps()
	if m.right.backend == started {
		t.Fatal("the reopen kept the right pane's backend")
	}
	left, right := &closeRecorder{Backend: m.left.backend}, &closeRecorder{Backend: m.right.backend}
	m.left.backend, m.right.backend = left, right
	m.Close()
	if !left.closed || !right.closed {
		t.Errorf("Close closed left=%v right=%v; want both backends the panes hold", left.closed, right.closed)
	}
}
