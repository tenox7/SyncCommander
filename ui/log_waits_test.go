package ui

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/transfer"
	"sc/transport"
)

// dialogModel is a scanned model with a.txt on both sides under the
// cursor, and every log line so far seen.
func dialogModel(t *testing.T) *Model {
	t.Helper()
	root := t.TempDir()
	for _, side := range []string{"L", "R"} {
		writeTestFile(t, filepath.Join(root, side, "a.txt"), "a")
	}
	m := scannedModel(t, filepath.Join(root, "L"), filepath.Join(root, "R"))
	settled(m)
	cursorTo(t, m, "a.txt", false)
	return m
}

// errorOnTick logs line at dir while a scan runs, and lets the tick see
// it, as the tick does while any work runs.
func errorOnTick(m *Model, dir transport.Dir, line string) {
	runningOp(m, opScan, nil)
	transport.Log.Add("scan", dir, line)
	m.Update(tickMsg{})
}

func logOnTop(m *Model) bool {
	d, ok := m.openModal().(*LogDialog)
	return ok && d == m.logView
}

// U18: the log opened by itself over any dialog and took the keys typed
// for it. An error came while a new name was typed: s saved a log file
// into the working folder, e switched the log's filter, and the field got
// none of it. Now the log waits for the dialog to close, then opens.
func TestTheLogWaitsWhileANameIsTyped(t *testing.T) {
	t.Chdir(t.TempDir()) // where s saves a log
	m := dialogModel(t)
	press(m, "e")
	if !m.input.IsOpen() {
		t.Fatal("e did not ask for the new name")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyCtrlU})
	press(m, "n")
	errOnly := m.logView.errOnly
	errorOnTick(m, transport.DirErr, "u18: an error while a name is typed")
	for _, k := range []string{"s", "e", "w"} {
		press(m, k)
	}
	if m.logView.visible {
		t.Error("the log opened over the rename dialog")
	}
	if got := m.input.ed.String(); got != "nsew" {
		t.Errorf("the field holds %q, want nsew", got)
	}
	if logs, _ := filepath.Glob("sc-log-*.log"); len(logs) > 0 {
		t.Errorf("s saved %v", logs)
	}
	if m.logView.errOnly != errOnly {
		t.Error("e switched the log's filter")
	}
	press(m, "esc")
	if !logOnTop(m) {
		t.Errorf("the dialog closed and the log did not open on the error: %T is on top", m.openModal())
	}
}

// U18: no dialog the user opened goes under the log; the log opens once
// it is closed.
func TestTheLogWaitsForEveryDialog(t *testing.T) {
	for _, k := range []string{"/", "d", "y", "=", "?", "i", "o"} {
		m := dialogModel(t)
		press(m, k)
		want := m.openModal()
		if want == nil || logOnTop(m) {
			t.Fatalf("%s opened %T", k, want)
		}
		errorOnTick(m, transport.DirErr, "u18: an error while a dialog is open")
		if got := m.openModal(); got != want {
			t.Errorf("%s: %T came over %T", k, got, want)
		}
		press(m, "esc")
		if !logOnTop(m) {
			t.Errorf("%s: the dialog closed and the log did not open: %T is on top", k, m.openModal())
		}
	}
}

// U18: a fatal error waits for the dialog too, and then opens the log
// with errors only, as it does with no dialog open. With the log open
// alone, a fatal error turns it to errors only at once, as before.
func TestAFatalErrorWaitsForTheDialog(t *testing.T) {
	m := dialogModel(t)
	press(m, "~")
	errorOnTick(m, transport.DirFatal, "u18: a fatal error with the log open")
	if !logOnTop(m) || !m.logView.errOnly {
		t.Errorf("the log open alone: on top %v, errors only %v; want both", logOnTop(m), m.logView.errOnly)
	}

	m = dialogModel(t)
	press(m, "e")
	errorOnTick(m, transport.DirFatal, "u18: a fatal error while a name is typed")
	if m.openModal() != modal(m.input) {
		t.Errorf("%T came over the rename dialog", m.openModal())
	}
	press(m, "esc")
	if !logOnTop(m) || !m.logView.errOnly {
		t.Errorf("after the dialog: log on top %v, errors only %v; want both", logOnTop(m), m.logView.errOnly)
	}
}

// U18: x stopped nothing while the log was up, and an error opens the
// log. Now x stops a write under the log, and the scan and checksums
// when no write runs.
func TestXStopsWhatRunsUnderTheLog(t *testing.T) {
	t.Run("a write", func(t *testing.T) {
		root := t.TempDir()
		for _, side := range []string{"L", "R"} {
			writeTestFile(t, filepath.Join(root, side, "a.txt"), "a")
		}
		l := &hangingWrites{Backend: openBackendOrSkip(t, filepath.Join(root, "L"))}
		r := &hangingWrites{Backend: openBackendOrSkip(t, filepath.Join(root, "R"))}
		m := newTestModel(t, l, r)
		m.scanner.Scan(context.Background(), *m.cmpOpts)
		m.refreshTreeNow()
		settled(m)
		cursorTo(t, m, "a.txt", false)
		l.hang.Store(true)
		press(m, "e")
		m.input.ed.set("b.txt")
		msgs := runCmd(press(m, "enter"))
		if _, done := firstResult(msgs, 200*time.Millisecond); done || !m.writing() {
			t.Fatal("the rename did not hang")
		}
		press(m, "~")
		if !logOnTop(m) {
			t.Fatal("~ did not open the log during the rename")
		}
		press(m, "x")
		msg, done := firstResult(msgs, 5*time.Second)
		if !done {
			t.Error("x under the log did not stop the rename")
			m.stopWrite()
			msg, done = firstResult(msgs, 5*time.Second)
		}
		if done {
			m.Update(msg)
		}
		if m.writing() {
			t.Error("still writing after the stop")
		}
	})
	t.Run("a scan", func(t *testing.T) {
		m := dialogModel(t)
		msgs := runCmd(m.startOp(opScan, func(ctx context.Context) { <-ctx.Done() }))
		press(m, "~")
		if !logOnTop(m) {
			t.Fatal("~ did not open the log during the scan")
		}
		press(m, "X")
		msg, done := firstResult(msgs, 5*time.Second)
		if !done {
			t.Error("X under the log did not stop the scan")
			m.stopScanOps()
			msg, done = firstResult(msgs, 5*time.Second)
		}
		if done {
			m.Update(msg)
		}
		if m.scanning() {
			t.Error("still scanning after the stop")
		}
		if !logOnTop(m) {
			t.Error("the stop closed the log")
		}
	})
}

// U18: + and - change a copy's files in flight under the log too.
func TestPlusMinusWorkUnderTheLog(t *testing.T) {
	m, left, _ := recordedModel(t)
	m.copyProgress.Reset(2, true)
	m.copyProgress.Sem.Store(transfer.NewDynSem(2))
	runningOp(m, opCopy, nil)
	press(m, "~")
	if !logOnTop(m) {
		t.Fatal("~ did not open the log during the copy")
	}
	for _, c := range []struct {
		k    string
		want int
	}{{"+", 3}, {"=", 4}, {"-", 3}} {
		press(m, c.k)
		if m.copyParallel != c.want || left.n != c.want {
			t.Errorf("after %s: %d files (pool %d), want %d", c.k, m.copyParallel, left.n, c.want)
		}
	}
	if !logOnTop(m) {
		t.Error("+ or - closed the log")
	}
}
