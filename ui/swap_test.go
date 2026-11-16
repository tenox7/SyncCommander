package ui

import (
	"os"
	"path/filepath"
	"testing"
)

// U6: s swapped the backends and the panels but not what they were opened
// with. The next reopen, u for the parent folder or b to go into a folder,
// built both sides from the old order: the swap was undone without notice,
// and the next > copied the other way. The location dialog showed the old
// order too.
func TestSwapSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	a, b := filepath.Join(root, "pa", "A"), filepath.Join(root, "pb", "B")
	for _, dir := range []string{filepath.Join(a, "sub"), filepath.Join(b, "sub")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	m := scannedModel(t, a, b)
	// want checks the open arguments (in the location dialog) and the
	// panel titles: left, then right.
	want := func(step, left, right string) {
		t.Helper()
		press(m, "y")
		l, r := m.openDlg.left.String(), m.openDlg.right.String()
		m.openDlg.Close()
		if l != left || r != right {
			t.Errorf("%s: the sides were opened with %s | %s, want %s | %s", step, l, r, left, right)
		}
		if lt, rt := m.activePanel().title, m.inactivePanel().title; lt != left || rt != right {
			t.Errorf("%s: the panels show %s | %s, want %s | %s", step, lt, rt, left, right)
		}
	}
	want("at the start", a, b)

	press(m, "s")
	want("after s", b, a)

	press(m, "down") // row 0 is the root
	if n := m.activePanel().CursorNode(); n == nil || n.Name != "sub" {
		t.Fatalf("the cursor is not on sub: %v", n)
	}
	press(m, "b")
	want("after s, b into sub", filepath.Join(b, "sub"), filepath.Join(a, "sub"))

	press(m, "u")
	want("then u", b, a)

	press(m, "u")
	want("then u again", filepath.Dir(b), filepath.Dir(a))
}
