package ui

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transfer"
	"sc/transport"
)

// x stopped a copy, and the log opened: "mirror delete skipped: copy
// canceled" was an ERR line, and "COPY finished with 2 failure(s)" a FAIL
// line for the two files in flight. A stopped delete logged a FAIL line.
// What x cut off did not fail.

// errLost is what a backend says when the cancel closes its connection.
var errLost = errors.New("connection lost")

// cutWrites takes no file and deletes nothing: each copy and delete
// waits for the operation to stop, then fails as a closed connection makes
// it.
type cutWrites struct{ model.Backend }

func (cutWrites) CopyFrom(ctx context.Context, _ string, _ io.Reader, _ model.PutParams) error {
	<-ctx.Done()
	return errLost
}

func (cutWrites) Remove(ctx context.Context, _ string) error {
	<-ctx.Done()
	return errLost
}

func (cutWrites) RemoveAll(ctx context.Context, _ string) error {
	<-ctx.Done()
	return errLost
}

// noProblemsSince fails t when the lines logged since mark hold an ERR,
// FAIL or FATAL line, or the log is open.
func noProblemsSince(t *testing.T, m *Model, mark int, what string) {
	t.Helper()
	for _, l := range transport.Log.Lines()[mark:] {
		if f := strings.Fields(l); len(f) > 2 && (f[2] == "ERR" || f[2] == "FAIL" || f[2] == "FATAL") {
			t.Errorf("x on a %s logged %q", what, l)
		}
	}
	if m.logView.visible {
		t.Errorf("x on a %s opened the log", what)
	}
}

func TestXStopsACopyWithNoError(t *testing.T) {
	right := cutWrites{openBackendOrSkip(t, t.TempDir())}
	m := newTestModel(t, openBackendOrSkip(t, "fake://x?dirs=1&files=4&depth=1"), right)
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	settled(m)
	mark := len(transport.Log.Lines())
	msgs := runCmd(m.runCopy(m.scanner.Tree(), true, transfer.Plan{Mirror: true}))
	for deadline := time.Now().Add(5 * time.Second); m.copyProgress.InFlight.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no file went in flight")
		}
	}
	press(m, "x")
	msg, done := waitFor[copyDoneMsg](msgs, 5*time.Second)
	if !done {
		t.Fatal("x did not stop the copy")
	}
	m.Update(msg)
	noProblemsSince(t, m, mark, "copy")
	if n := m.copyProgress.Failed.Load(); n != 0 {
		t.Errorf("failed=%d after x, want 0", n)
	}
	for _, want := range []string{"user canceled transfer", "mirror delete skipped: copy canceled", "COPY canceled: "} {
		if !loggedSince(mark, want) {
			t.Errorf("the log does not say %q", want)
		}
	}
}

func TestXStopsADeleteWithNoFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("only on the right"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := newTestModel(t, openBackendOrSkip(t, t.TempDir()), cutWrites{openBackendOrSkip(t, dir)})
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	var file *model.TreeNode
	m.readTree(func(root *model.TreeNode) {
		file = firstNode(root, func(n *model.TreeNode) bool { return n.Name == "a.txt" })
	})
	if file == nil {
		t.Fatal("a.txt is not in the tree")
	}
	settled(m)
	mark := len(transport.Log.Lines())
	msgs := runCmd(m.deleteNode(file, model.PresenceRightOnly))
	if _, done := waitFor[opDoneMsg](msgs, 300*time.Millisecond); done {
		t.Fatal("the delete ended by itself; it was to hang")
	}
	press(m, "x")
	msg, done := waitFor[opDoneMsg](msgs, 5*time.Second)
	if !done {
		t.Fatal("x did not stop the delete")
	}
	m.Update(msg)
	noProblemsSince(t, m, mark, "delete")
	if n := m.deleteProgress.Failed.Load(); n != 0 {
		t.Errorf("failed=%d after x, want 0", n)
	}
	if !loggedSince(mark, "right a.txt: canceled") {
		t.Error("the log does not say the delete was canceled")
	}
}

// A fatal abort cancels the operation too, but it is no stop of the
// user's: what it cuts off fails.
func TestAFatalAbortIsNoUserCancel(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	var user bool
	msg := runFirst(m.start(opCopy, func(ctx context.Context, done opDoneMsg) doneMsg {
		transport.TriggerFatalAbort(ctx)
		user = transport.UserCanceled(ctx)
		return done
	}))
	m.Update(msg)
	if user {
		t.Error("a fatal abort passed for the user's x")
	}
}
