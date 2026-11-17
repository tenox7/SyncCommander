package ui

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"sc/model"
)

// U11: the top row stands for the base of each side. On it, d opened a
// recursive delete of both bases (Enter: both sides; restic:// sent it as
// a DELETE of the repository), e a rename of the bases, and t set the
// older base folder's times to the newer one's. They now do nothing there,
// as b does; on the row below they still work.
func TestTopRowIsNotDeletedRenamedOrTouched(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	writeTestFile(t, filepath.Join(left, "a.txt"), "a")
	writeTestFile(t, filepath.Join(right, "a.txt"), "a")
	older, newer := time.Unix(1600000000, 0), time.Unix(1700000000, 0)
	for dir, mt := range map[string]time.Time{left: older, right: newer} {
		if err := os.Chtimes(dir, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	m := scannedModel(t, left, right)
	if n := m.activePanel().CursorNode(); n == nil || !m.isTopRow(n) {
		t.Fatal("the cursor does not start on the top row")
	}

	for _, k := range []string{"d", "e", "t"} {
		cmd := press(m, k)
		if m.confirm.IsOpen() {
			t.Errorf("%s on the top row opened %q %q", k, m.confirm.title, m.confirm.lines)
			m.confirm.Close()
		}
		if m.input.IsOpen() {
			t.Errorf("%s on the top row opened the input %q", k, m.input.title)
			m.input.Close()
		}
		if cmd != nil {
			t.Errorf("%s on the top row started an operation", k)
			m.Update(runFirst(cmd)) // the operation runs and ends
		}
	}
	for dir, mt := range map[string]time.Time{left: older, right: newer} {
		if fi, err := os.Stat(dir); err != nil || !fi.ModTime().Equal(mt) {
			t.Errorf("%s: %v, %v, want its time %v kept", filepath.Base(dir), fi.ModTime(), err, mt)
		}
		if names, _ := filepath.Glob(filepath.Join(dir, "*")); !slices.Equal(names, []string{filepath.Join(dir, "a.txt")}) {
			t.Errorf("%s holds %q, want a.txt alone", filepath.Base(dir), names)
		}
	}

	// The row below the top row: each key still opens its dialog.
	m.activePanel().jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "a.txt" }))
	press(m, "d")
	if !m.confirm.IsOpen() {
		t.Error("d on a.txt opened no dialog")
	}
	m.confirm.Close()
	m.pendingDelete = nil
	press(m, "e")
	if !m.input.IsOpen() {
		t.Error("e on a.txt opened no input")
	}
	m.input.Close()
}
