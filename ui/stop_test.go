package ui

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

// stuckBackend lists and checksums as its backend does until stuck is set;
// then each listing and checksum hangs until it is stopped.
type stuckBackend struct {
	model.Backend
	stuck atomic.Bool
}

func (s *stuckBackend) List(ctx context.Context, dir string) ([]model.FileEntry, error) {
	if s.stuck.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.Backend.List(ctx, dir)
}

func (s *stuckBackend) Checksum(ctx context.Context, rel, algo string) (string, error) {
	if s.stuck.Load() {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return s.Backend.Checksum(ctx, rel, algo)
}

// runCmd runs cmd and every command it batches, each in its own
// goroutine, and sends their messages on the channel it returns.
func runCmd(cmd tea.Cmd) <-chan tea.Msg {
	out := make(chan tea.Msg, 16)
	var run func(tea.Cmd)
	run = func(c tea.Cmd) {
		if c == nil {
			return
		}
		go func() {
			msg := c()
			if batch, ok := msg.(tea.BatchMsg); ok {
				for _, c := range batch {
					run(c)
				}
				return
			}
			out <- msg
		}()
	}
	run(cmd)
	return out
}

// waitFor returns the first message of type T from msgs, or false after d.
func waitFor[T tea.Msg](msgs <-chan tea.Msg, d time.Duration) (T, bool) {
	timeout := time.After(d)
	for {
		select {
		case msg := <-msgs:
			if m, ok := msg.(T); ok {
				return m, true
			}
		case <-timeout:
			var zero T
			return zero, false
		}
	}
}

func loggedSince(mark int, text string) bool {
	return slices.ContainsFunc(transport.Log.Lines()[mark:], func(l string) bool { return strings.Contains(l, text) })
}

// U10: no key stopped a scan or a checksum pass; the only ways out were to
// quit or to reopen. x, which stops a copy or a delete, now stops them too.
func TestXStopsAScanAndAChecksum(t *testing.T) {
	ctx := context.Background()
	right := &stuckBackend{Backend: openBackendOrSkip(t, "fake://tiny")}
	m := newTestModel(t, openBackendOrSkip(t, "fake://tiny"), right)
	m.scanner.Scan(ctx, *m.cmpOpts)
	m.refreshTreeNow()
	file := firstBothFile(m.scanner.Tree())
	if file == nil {
		t.Fatal("no file present on both sides")
	}
	right.stuck.Store(true)
	settled(m)
	for _, c := range []struct {
		what  string
		start func() tea.Cmd
		busy  func() bool
	}{
		{"scan", m.startScan, m.scanning},
		{"checksum", func() tea.Cmd { return m.checksumNode(file) }, m.checksumming},
	} {
		msgs := runCmd(c.start())
		if _, done := waitFor[opDoneMsg](msgs, 300*time.Millisecond); done {
			t.Fatalf("the %s ended by itself; it was to hang", c.what)
		}
		mark := len(transport.Log.Lines())
		press(m, "x")
		msg, done := waitFor[opDoneMsg](msgs, 5*time.Second)
		if !done {
			t.Fatalf("x did not stop the %s", c.what)
		}
		m.Update(msg)
		if c.busy() {
			t.Errorf("still running the %s after x", c.what)
		}
		if m.logView.visible {
			t.Fatalf("stopping the %s opened the log: a stop is not a failure", c.what)
		}
		if !loggedSince(mark, "user canceled "+c.what) {
			t.Errorf("the log does not say the %s was canceled", c.what)
		}
	}
}

// U10: after a delete, the parent folder was listed again under
// context.Background(): x did nothing until that listing returned, up to
// the two-minute list timeout. Now x stops it, and the folder that was not
// listed again is marked so: its rows are not what is there, and a copy or
// a mirror lists it again before it acts.
func TestXStopsTheListingAfterADelete(t *testing.T) {
	right := &stuckBackend{Backend: openBackendOrSkip(t, "fake://tiny")}
	m := newTestModel(t, openBackendOrSkip(t, "fake://tiny"), right)
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	file := firstBothFile(m.scanner.Tree())
	if file == nil {
		t.Fatal("no file present on both sides")
	}
	right.stuck.Store(true) // the delete goes through; the listing after it hangs
	settled(m)
	m.deleting = true
	msgs := runCmd(m.deleteNode(file, model.PresenceRightOnly))
	if _, done := waitFor[deleteDoneMsg](msgs, 300*time.Millisecond); done {
		t.Fatal("the delete ended by itself; its listing was to hang")
	}
	press(m, "x")
	msg, done := waitFor[deleteDoneMsg](msgs, 5*time.Second)
	if !done {
		t.Fatal("x did not stop the listing after the delete")
	}
	m.Update(msg)
	var listErr bool
	m.readTree(func(*model.TreeNode) { listErr = file.Parent.ListErr })
	if !listErr {
		t.Fatal("the folder the delete changed was not listed again, and is not marked so")
	}
	// Listed again, it is no longer marked.
	right.stuck.Store(false)
	m.scanner.RelistDir(context.Background(), file.Parent.RelPath, *m.cmpOpts)
	m.readTree(func(*model.TreeNode) { listErr = file.Parent.ListErr })
	if listErr {
		t.Error("the folder is still marked after a listing that went through")
	}
}

// A rescan queued behind the scan (after a copy or a rename) goes with it:
// it would wait on the same server. Its folder is marked as not listed, so
// its rows do not pass for what is there.
func TestXDropsTheRescanQueuedBehindTheScan(t *testing.T) {
	right := &stuckBackend{Backend: openBackendOrSkip(t, "fake://tiny")}
	m := newTestModel(t, openBackendOrSkip(t, "fake://tiny"), right)
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	var dir *model.TreeNode
	m.readTree(func(root *model.TreeNode) {
		dir = firstNode(root, func(n *model.TreeNode) bool { return n.IsDir && n.RelPath != "" })
	})
	if dir == nil {
		t.Fatal("no folder in the tree")
	}
	right.stuck.Store(true)
	settled(m)
	msgs := runCmd(m.rescanNode(m.scanner.Tree(), nil))
	if m.queueRescan(dir, nil) != nil {
		t.Fatal("the rescan started beside the running one")
	}
	press(m, "x")
	msg, done := waitFor[opDoneMsg](msgs, 5*time.Second)
	if !done {
		t.Fatal("x did not stop the rescan")
	}
	m.Update(msg)
	if m.scanning() {
		t.Error("the queued rescan started after x")
	}
	var listErr bool
	m.readTree(func(*model.TreeNode) { listErr = dir.ListErr })
	if !listErr {
		t.Error("the folder of the dropped rescan is not marked as not listed")
	}
}
