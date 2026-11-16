package ui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
	"sc/transport"
)

// hangingWrites renames and touches as a server does: not once its context
// is done, which the local backend ignores. Once hang is set, each rename
// and touch waits until it is stopped.
type hangingWrites struct {
	model.Backend
	hang atomic.Bool
}

func (h *hangingWrites) Rename(ctx context.Context, oldRel, newRel string) error {
	if h.hang.Load() {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return h.Backend.Rename(ctx, oldRel, newRel)
}

func (h *hangingWrites) SetTimes(ctx context.Context, rel string, mtime, atime, btime time.Time) error {
	if h.hang.Load() {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return h.Backend.SetTimes(ctx, rel, mtime, atime, btime)
}

// firstResult returns the first message from msgs that is not a tick, or
// false after d.
func firstResult(msgs <-chan tea.Msg, d time.Duration) (tea.Msg, bool) {
	timeout := time.After(d)
	for {
		select {
		case msg := <-msgs:
			if _, tick := msg.(tickMsg); !tick {
				return msg, true
			}
		case <-timeout:
			return nil, false
		}
	}
}

// U14 (D11): rename and touch were no operations. Nothing showed them, x
// did not stop them (they ran under context.Background()), and every key
// worked beside them: a second rename, a copy, a reopen. Now each is a
// write like a copy: the status bar shows it with x=stop, it holds the
// keys, and x stops it. A stop is not a failure, and a rename stopped
// half done, renamed on the left only, is undone there.
func TestXStopsARenameAndATouch(t *testing.T) {
	for _, c := range []struct {
		what, state string
		start       func(m *Model) tea.Cmd
	}{
		{"rename", "RENAME", func(m *Model) tea.Cmd {
			press(m, "e")
			if !m.input.IsOpen() {
				t.Fatal("e did not ask for the new name")
			}
			m.input.ed.set("b.txt")
			return press(m, "enter")
		}},
		{"touch", "TOUCH", func(m *Model) tea.Cmd { return press(m, "t") }},
	} {
		root := t.TempDir()
		left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
		at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
		for _, f := range []struct {
			path  string
			mtime time.Time
		}{{filepath.Join(left, "d", "a.txt"), at.Add(10 * time.Minute)}, {filepath.Join(right, "d", "a.txt"), at}} {
			writeTestFile(t, f.path, "a")
			if err := os.Chtimes(f.path, f.mtime, f.mtime); err != nil {
				t.Fatal(err)
			}
		}
		l, r := &hangingWrites{Backend: openBackendOrSkip(t, left)}, &hangingWrites{Backend: openBackendOrSkip(t, right)}
		m := newTestModel(t, l, r)
		m.scanner.Scan(context.Background(), *m.cmpOpts)
		m.refreshTreeNow()
		settled(m)
		cursorTo(t, m, "d", true)
		press(m, "enter")
		cursorTo(t, m, "a.txt", false)

		r.hang.Store(true) // the right side hangs; the left one renames
		msgs := runCmd(c.start(m))
		if _, done := firstResult(msgs, 300*time.Millisecond); done {
			t.Fatalf("the %s ended by itself; it was to hang", c.what)
		}
		if !m.busy() {
			t.Fatalf("a running %s is not an operation: the UI is idle", c.what)
		}
		if v := ansi.Strip(m.View()); !strings.Contains(v, c.state) || !strings.Contains(v, "x=stop") {
			t.Errorf("the status bar does not show the %s and its stop key:\n%s", c.what, v[strings.LastIndex(v, "\n")+1:])
		}
		// It holds the keys: no second write, scan, reopen or dialog
		// starts beside it.
		for _, k := range []string{">", "<", "d", "t", "e", "c", "r", "R", "u", "b", "y", "s", "=", "enter"} {
			if cmd := press(m, k); cmd != nil || m.openModal() != nil || m.scanning() {
				t.Fatalf("%s started something during the %s", k, c.what)
			}
		}

		mark := len(transport.Log.Lines())
		press(m, "x")
		msg, done := firstResult(msgs, 5*time.Second)
		if !done {
			t.Fatalf("x did not stop the %s", c.what)
		}
		m.Update(msg)
		if m.busy() {
			t.Errorf("still busy after x stopped the %s", c.what)
		}
		if !loggedSince(mark, "user canceled "+c.what) {
			t.Errorf("the log does not say the %s was canceled", c.what)
		}
		if m.logView.visible {
			t.Errorf("stopping the %s opened the log: a stop is not a failure", c.what)
		}
		for _, side := range []string{left, right} {
			if _, err := os.Stat(filepath.Join(side, "d", "a.txt")); err != nil {
				t.Errorf("after the stopped %s: %v", c.what, err)
			}
			if _, err := os.Stat(filepath.Join(side, "d", "b.txt")); !os.IsNotExist(err) {
				t.Errorf("the stopped %s left %s/d/b.txt: the sides are apart", c.what, filepath.Base(side))
			}
		}
	}
}

// U14: a rescan queued behind the scan, after a copy or a rename, outlived
// a reopen. Once the new root was scanned, it rescanned a folder of the old
// tree on the new scanner: "rescan start: sub/deep", then a listing error
// for a path that does not exist, and the log popped up.
func TestReopenDropsTheRescanQueuedBehindTheScan(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	writeTestFile(t, filepath.Join(left, "sub", "deep", "a.txt"), "a")
	writeTestFile(t, filepath.Join(right, "sub", "b.txt"), "b")
	m := scannedModel(t, left, right)
	settled(m)
	if m.deepRescanAll() == nil { // a scan runs; it is never let finish
		t.Fatal("the scan did not start")
	}
	cursorTo(t, m, "sub", true)
	press(m, "enter")
	cursorTo(t, m, "deep", true)
	cmd := press(m, ">")
	if cmd == nil {
		t.Fatal("> copied nothing")
	}
	m.Update(runFirst(cmd))
	if m.pendingRescan == nil {
		t.Fatal("the copy's rescan did not wait behind the scan")
	}

	cursorTo(t, m, "sub", true)
	mark := len(transport.Log.Lines())
	cmd = press(m, "b")
	if cmd == nil || m.left.arg != filepath.Join(left, "sub") {
		t.Fatalf("b did not go into sub: %q", m.left.arg)
	}
	if m.pendingRescan != nil {
		t.Error("the reopen kept the rescan queued behind the old scan")
	}
	_, next := m.Update(runFirst(cmd))
	if next != nil {
		m.Update(runFirst(next))
		t.Error("the new root's scan was followed by a rescan of the old tree")
	}
	if loggedSince(mark, "rescan start") || m.logView.visible {
		t.Error("the reopen rescanned a folder of the old tree, and the log popped up")
	}
}
