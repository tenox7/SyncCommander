package ui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cursorTo moves the cursor to the folder or file row named name.
func cursorTo(t *testing.T, m *Model, name string, dir bool) {
	t.Helper()
	for range 50 {
		press(m, "up")
	}
	for range 50 {
		if n := m.activePanel().CursorNode(); n != nil && n.Name == name && n.IsDir == dir {
			return
		}
		press(m, "down")
	}
	t.Fatalf("no row %s (folder: %v)", name, dir)
}

// U13: b on a folder that is on one side only moved that side alone. The
// other stayed at the old root, where every name now showed as on that
// side only: a mirror from the top row offered to delete all of it. Now b
// goes only into a folder that is a folder on both sides; otherwise the
// location dialog shows where both sides would go, and why not, and
// nothing is reopened. b on a file still does nothing.
func TestDescendNeedsTheFolderOnBothSides(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	for _, p := range []string{
		filepath.Join(left, "photos", "a.jpg"),
		filepath.Join(left, "both", "x.txt"),
		filepath.Join(right, "both", "y.txt"),
		filepath.Join(left, "clash", "z.txt"),
		filepath.Join(right, "clash"),
		filepath.Join(right, "other.txt"),
	} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	m := scannedModel(t, left, right)
	titles := func() (string, string) { return m.activePanel().title, m.inactivePanel().title }

	press(m, "b") // the top row stands for both roots
	if m.openDlg.visible {
		t.Errorf("b on the top row opened the location dialog: %q", m.openDlg.errMsg)
	}

	for _, c := range []struct{ name, why string }{
		{"photos", "photos is not a folder on the right"},
		// The file clash on the right is a row of its own.
		{"clash", "clash is not a folder on the right"},
	} {
		cursorTo(t, m, c.name, true)
		press(m, "b")
		if l, r := titles(); l != left || r != right {
			t.Errorf("b on %s: the panels show %s | %s, want them where they were: %s | %s", c.name, l, r, left, right)
		}
		if !m.openDlg.visible || !strings.Contains(m.openDlg.errMsg, c.why) {
			t.Errorf("b on %s: dialog shown=%v saying %q, want it to say %q", c.name, m.openDlg.visible, m.openDlg.errMsg, c.why)
		}
		if l, r := m.openDlg.left.String(), m.openDlg.right.String(); l != filepath.Join(left, c.name) || r != filepath.Join(right, c.name) {
			t.Errorf("b on %s: the dialog offers %s | %s, want both sides in %s", c.name, l, r, c.name)
		}
		m.openDlg.Close()
	}

	cursorTo(t, m, "clash", false)
	press(m, "b") // a file: nothing to go into
	if l, r := titles(); m.openDlg.visible || l != left || r != right {
		t.Errorf("b on the file clash: dialog shown=%v, panels %s | %s", m.openDlg.visible, l, r)
	}

	cursorTo(t, m, "both", true)
	press(m, "b")
	if l, r := titles(); l != filepath.Join(left, "both") || r != filepath.Join(right, "both") {
		t.Errorf("b on both: the panels show %s | %s, want both sides in both", l, r)
	}
}
