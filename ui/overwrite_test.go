package ui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// overwriteFixture writes d/newer.txt, newer on the right, and d/plain.txt,
// older and smaller on the right, and returns the two roots.
func overwriteFixture(t *testing.T) (left, right string) {
	t.Helper()
	root := t.TempDir()
	left, right = filepath.Join(root, "left"), filepath.Join(root, "right")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	later := at.Add(10 * time.Minute)
	for _, f := range []struct {
		path, body string
		mtime      time.Time
	}{
		{filepath.Join(left, "d", "newer.txt"), "source", at},
		{filepath.Join(right, "d", "newer.txt"), "dest!!", later},
		{filepath.Join(left, "d", "plain.txt"), "source", later},
		{filepath.Join(right, "d", "plain.txt"), "dest", at},
	} {
		writeTestFile(t, f.path, f.body)
		if err := os.Chtimes(f.path, f.mtime, f.mtime); err != nil {
			t.Fatal(err)
		}
	}
	return left, right
}

// F16: > and < overwrote files at once, even when the file on the
// destination was newer or larger: one wrong key put an old draft over the
// new one. Now the copy says how many such files there are and names the
// first: y writes over them, s skips them and copies the rest, Esc cancels.
func TestFolderCopyAsksBeforeOverwritingNewerFiles(t *testing.T) {
	for _, tc := range []struct {
		key         string
		copied      bool
		overwritten bool
	}{{"esc", false, false}, {"s", true, false}, {"y", true, true}} {
		left, right := overwriteFixture(t)
		m := scannedModel(t, left, right)
		m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "d" && n.IsDir }))
		press(m, ">")
		if !m.confirm.IsOpen() || m.pendingCopy == nil {
			t.Fatalf("%s: > over a newer file did not ask", tc.key)
		}
		body := strings.Join(m.confirm.lines, "\n")
		for _, want := range []string{"1 files are newer or larger on right:", "d/newer.txt  (newer)"} {
			if !strings.Contains(body, want) {
				t.Errorf("%s: the dialog lacks %q:\n%s", tc.key, want, body)
			}
		}
		if strings.Contains(body, "plain.txt") {
			t.Errorf("%s: the dialog names a file older on right:\n%s", tc.key, body)
		}
		if m.confirm.keys != "y=overwrite them  s=skip them  Esc=cancel" {
			t.Fatalf("%s: the dialog offers %q", tc.key, m.confirm.keys)
		}
		if v := ansi.Strip(m.confirm.View(80, 24)); maxLineWidth(v) > 80 {
			t.Errorf("%s: the dialog is %d wide on an 80-column screen", tc.key, maxLineWidth(v))
		}
		cmd := press(m, tc.key)
		if tc.copied {
			if cmd == nil {
				t.Fatalf("%s started no copy", tc.key)
			}
			if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
				t.Fatalf("%s: the copy did not run", tc.key)
			}
		} else if cmd != nil || m.confirm.IsOpen() || m.pendingCopy != nil {
			t.Fatalf("%s did not cancel", tc.key)
		}
		if got := readTestFile(t, filepath.Join(right, "d", "newer.txt")); (got == "source") != tc.overwritten {
			t.Errorf("after %s, d/newer.txt on right holds %q", tc.key, got)
		}
		if got := readTestFile(t, filepath.Join(right, "d", "plain.txt")); (got == "source") != tc.copied {
			t.Errorf("after %s, d/plain.txt on right holds %q", tc.key, got)
		}
	}
}

// F16, one file: the question is about that file alone, so there is
// nothing to skip.
func TestFileCopyAsksBeforeOverwritingANewerFile(t *testing.T) {
	left, right := overwriteFixture(t)
	m := scannedModel(t, left, right)
	cursorTo(t, m, "d", true)
	press(m, "enter")
	cursorTo(t, m, "newer.txt", false)
	press(m, ">")
	if !m.confirm.IsOpen() || m.confirm.keys != "y=overwrite it  Esc=cancel" {
		t.Fatalf("> on a file newer on right: dialog open %v, keys %q", m.confirm.IsOpen(), m.confirm.keys)
	}
	if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "It is newer on right:") {
		t.Errorf("the dialog does not say why:\n%s", body)
	}
	if press(m, "s") != nil || !m.confirm.IsOpen() {
		t.Fatal("s answered a question about one file")
	}
	cmd := press(m, "y")
	if cmd == nil {
		t.Fatal("y started no copy")
	}
	if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
		t.Fatal("the copy did not run")
	}
	if got := readTestFile(t, filepath.Join(right, "d", "newer.txt")); got != "source" {
		t.Errorf("after y, d/newer.txt on right holds %q", got)
	}
}

// F16: in the dialog for a folder on both sides, Enter deleted it on both
// sides: the most destructive answer sat on the key that opens a folder.
// Now Enter does nothing there; b deletes on both sides.
func TestBothSidesDeleteNeedsB(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "proj", "a.txt"), "a")
	writeTestFile(t, filepath.Join(right, "proj", "a.txt"), "a")
	m := scannedModel(t, left, right)
	cursorTo(t, m, "proj", true)
	press(m, "d")
	if !m.confirm.IsOpen() || m.pendingDelete == nil {
		t.Fatal("d did not ask")
	}
	if v := ansi.Strip(m.confirm.View(80, 24)); !strings.Contains(v, "b=both sides") || strings.Contains(v, "↵") {
		t.Errorf("the dialog's keys:\n%s", v)
	}
	if cmd := press(m, "enter"); cmd != nil || !m.confirm.IsOpen() {
		t.Fatal("enter answered the question")
	}
	for _, dir := range []string{left, right} {
		if _, err := os.Stat(filepath.Join(dir, "proj")); err != nil {
			t.Fatalf("after enter: %v", err)
		}
	}
	cmd := press(m, "b")
	if cmd == nil {
		t.Fatal("b deleted nothing")
	}
	m.Update(runFirst(cmd))
	for _, dir := range []string{left, right} {
		if _, err := os.Stat(filepath.Join(dir, "proj")); !os.IsNotExist(err) {
			t.Errorf("after b, %s/proj: %v", filepath.Base(dir), err)
		}
	}
}

func readTestFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
