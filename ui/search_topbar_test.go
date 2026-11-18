package ui

import (
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// searchModel has three names that start with "match", in d, in e and at
// the top, on both sides, scanned.
func searchModel(t *testing.T) *Model {
	t.Helper()
	root := t.TempDir()
	for _, side := range []string{"L", "R"} {
		for _, f := range []string{"match1.txt", "d/match2.txt", "e/match3.txt", "other.txt"} {
			writeTestFile(t, filepath.Join(root, side, f), "x")
		}
	}
	m := scannedModel(t, filepath.Join(root, "L"), filepath.Join(root, "R"))
	settled(m)
	return m
}

// search presses /, types query over what the dialog starts with, and
// presses Enter. It returns where the cursor is.
func search(m *Model, query string) string {
	press(m, "/")
	m.Update(tea.KeyMsg{Type: tea.KeyCtrlU})
	m.Update(key(query))
	press(m, "enter")
	var at string
	m.readTree(func(*model.TreeNode) {
		if n := m.activePanel().CursorFile(); n != nil {
			at = n.RelPath
		}
	})
	return at
}

// U24: each search went to the first match: there was no next. / now
// starts with the last search, and Enter goes to the next match after
// the cursor, then to the first again.
func TestSearchGoesToTheNextMatch(t *testing.T) {
	m := searchModel(t)
	var got []string
	for i := range 4 {
		if i > 0 {
			press(m, "/")
			if s := m.input.ed.String(); s != "^match" {
				t.Errorf("search %d starts with %q, want the last search", i+1, s)
			}
			press(m, "esc")
		}
		got = append(got, search(m, "^match"))
	}
	if got[0] == got[1] || got[1] == got[2] || got[0] == got[2] || got[3] != got[0] {
		t.Errorf("four searches went to %q, want three matches and then the first again", got)
	}
	for _, n := range got {
		if !strings.Contains(n, "match") {
			t.Errorf("a search went to %q", n)
		}
	}
}

// U24: on a file's attribute row, the next match is the one after that
// file, not the first.
func TestSearchFromAnAttributeRow(t *testing.T) {
	m := searchModel(t)
	first := search(m, "^match")
	cursorTo(t, m, filepath.Base(first), false)
	press(m, "enter") // the file's attributes
	press(m, "down")
	if r, _ := m.activePanel().CursorRow(); r.Attr == nil {
		t.Fatal("the cursor is not on an attribute row")
	}
	if got := search(m, "^match"); got == first || got == "" {
		t.Errorf("the search from an attribute row of %s went to %q, want the next match", first, got)
	}
}

// U24: a pattern that is not a regular expression, or that no name
// matches, closed the dialog and nothing said why the cursor did not
// move. The dialog now stays open and says so.
func TestSearchSaysWhatWentWrong(t *testing.T) {
	m := searchModel(t)
	for _, c := range []struct{ query, says string }{
		{"[a", "missing closing ]"},
		{"^nothing", "no name matches"},
	} {
		search(m, c.query)
		if !m.input.IsOpen() || !strings.Contains(m.input.err, c.says) {
			t.Errorf("search %q: open %v, says %q; want it open and saying %q", c.query, m.input.IsOpen(), m.input.err, c.says)
		}
		press(m, "esc")
	}
	if at := search(m, "^other"); at != "other.txt" || m.input.IsOpen() {
		t.Errorf("a search that matches went to %q, open %v", at, m.input.IsOpen())
	}
}

// U24: the top bar's halves wrapped onto a second line when their text
// was longer than a panel, and joined with + they broke the screen.
func TestTopBarIsOneLine(t *testing.T) {
	stats := &TreeStats{LeftFiles: 12345678, RightFiles: 1, LeftDirs: 123456, RightDirs: 2, LeftSize: 9 << 40, RightSize: 1}
	for _, w := range []int{1, 2, 10, 40} {
		for _, left := range []bool{true, false} {
			bar := ansi.Strip(RenderPanelTopBar(stats, left, "⠋ checksumming...", w))
			if strings.Contains(bar, "\n") || ansi.StringWidth(bar) > max(w, 2) {
				t.Errorf("width %d: the bar is %q", w, bar)
			}
		}
	}
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	m.width, m.height = 80, 12
	m.layoutPanels()
	m.cachedStats = stats
	runningOp(m, opChecksum, nil)
	screen := strings.Split(ansi.Strip(m.View()), "\n")
	if len(screen) != m.height {
		t.Errorf("the screen has %d lines, want %d:\n%s", len(screen), m.height, strings.Join(screen, "\n"))
	}
	if top := screen[0]; strings.Count(top, "checksumming") != 2 {
		t.Errorf("the top line does not hold both halves: %q", top)
	}
}
