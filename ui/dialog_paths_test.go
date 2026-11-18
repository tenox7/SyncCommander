package ui

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// part1 and part2 are archive parts: long names, alike up to the end;
// longDir is a folder's.
const (
	part1   = "backup-2026-11-18-photos-library-volume-part0001.tar"
	part2   = "backup-2026-11-18-photos-library-volume-part0002.tar"
	longDir = "backup-2026-11-18-photos-library-volume-dir0001"
)

// pathsModel has d/sub/part1, part2 and longDir/x.txt on both sides,
// only-left.txt on the left and extra.txt on the right, every folder
// open.
func pathsModel(t *testing.T, width int) *Model {
	t.Helper()
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	for _, side := range []string{left, right} {
		writeTestFile(t, filepath.Join(side, "d", "sub", part1), "a")
		writeTestFile(t, filepath.Join(side, "d", "sub", part2), "b")
		writeTestFile(t, filepath.Join(side, longDir, "x.txt"), "c")
	}
	writeTestFile(t, filepath.Join(left, "only-left.txt"), "l")
	writeTestFile(t, filepath.Join(right, "extra.txt"), "x")
	m := scannedModel(t, left, right)
	m.width = width
	m.layoutPanels()
	settled(m)
	press(m, "}")
	return m
}

func (m *Model) cursorOn(t *testing.T, name string) {
	t.Helper()
	node := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == name })
	if node == nil {
		t.Fatalf("no %s in the tree", name)
	}
	m.activePanel().jumpTo(node)
	if m.activePanel().CursorNode() != node {
		t.Fatalf("the cursor is not on %s", name)
	}
}

// U20: the delete and copy dialogs named the node alone, over a screen
// that hides the tree: which folder, and which of two long names alike up
// to the end, was not shown, and the top folder showed as "//". Now they
// give the whole path, and the folder it is in on each side.
func TestDialogsNameTheWholePath(t *testing.T) {
	m := pathsModel(t, 120)
	inL, inR := "  in L: "+m.left.backend.BasePath(), "  in R: "+m.right.backend.BasePath()
	for _, c := range []struct {
		name, key string
		want      []string
		not       string
	}{
		{part1, "d", []string{"d/sub/" + part1, inL, inR}, ""},
		{"sub", "d", []string{"d/sub/", inL, inR}, ""},
		{"only-left.txt", "d", []string{"only-left.txt", inL}, inR},
		{"extra.txt", "d", []string{"extra.txt", inR}, inL},
		{"", ">", []string{"the top folder", inL, inR}, "//"},
	} {
		if c.name == "" {
			for range 50 {
				press(m, "up")
			}
		} else {
			m.cursorOn(t, c.name)
		}
		press(m, c.key)
		if !m.confirm.IsOpen() {
			t.Fatalf("%s on %q opened no dialog", c.key, c.name)
		}
		for _, w := range c.want {
			if !slices.Contains(m.confirm.lines, w) {
				t.Errorf("%s on %q: no line %q in %q", c.key, c.name, w, m.confirm.lines)
			}
		}
		if c.not != "" && slices.Contains(m.confirm.lines, c.not) {
			t.Errorf("%s on %q: a line %q in %q", c.key, c.name, c.not, m.confirm.lines)
		}
		press(m, "esc")
	}
}

// U20: a dialog line longer than the screen went past its edge and was
// cut there. It now wraps inside the box, and the whole path is shown.
func TestConfirmDialogWrapsALongPath(t *testing.T) {
	const width = 60
	m := pathsModel(t, width)
	m.cursorOn(t, part1)
	press(m, "d")
	screen := ansi.Strip(m.View())
	var inside strings.Builder
	for _, line := range strings.Split(screen, "\n") {
		if w := ansi.StringWidth(line); w > width {
			t.Errorf("a line of %d cells on a screen of %d: %q", w, width, line)
		}
		inside.WriteString(strings.Trim(line, " │╭╮╰╯─"))
	}
	if !strings.Contains(inside.String(), "d/sub/"+part1) {
		t.Errorf("the dialog does not show the whole path:\n%s", screen)
	}
	press(m, "esc")
}

// U20: the panel cut long names on the right, where archive parts
// differ: part0001.tar and part0002.tar showed alike. Now the middle is
// cut, and each row keeps its name's end: a file's, a folder's with its
// /, and the top row's folder.
func TestPanelCutsLongNamesInTheMiddle(t *testing.T) {
	m := pathsModel(t, 100)
	for _, p := range []*Panel{m.left.panel, m.right.panel} {
		top := "/L"
		if p.side == model.SideRight {
			top = "/R"
		}
		ends := []string{"-part0001.tar", "-part0002.tar", "-dir0001/", top}
		got := make([]bool, len(ends))
		for i, line := range strings.Split(ansi.Strip(p.View()), "\n") {
			if w := ansi.StringWidth(line); w > p.width {
				t.Errorf("a row of %d cells in a panel of %d: %q", w, p.width, line)
			}
			for j, end := range ends {
				if strings.Contains(line, "…") && strings.Contains(line, end) && (j < 3 || i == 0) {
					got[j] = true
				}
			}
		}
		for j, end := range ends {
			if !got[j] {
				t.Errorf("panel %v: no row keeps the end %q:\n%s", p.side, end, ansi.Strip(p.View()))
			}
		}
	}
}

func TestCutMiddle(t *testing.T) {
	for _, c := range []struct {
		s    string
		w    int
		want string
	}{
		{"abcdefghij", 10, "abcdefghij"},
		{"abcdefghij", 20, "abcdefghij"},
		{"abcdefghij", 9, "abcd…ghij"},
		{"abcdefghij", 6, "ab…hij"},
		{"abcdefghij", 5, "ab…ij"},
		{"abcdefghij", 2, "…j"},
		{"abcdefghij", 1, "…"},
		{"abcdefghij", 0, ""},
		{"日本語のファイル名です.txt", 9, "日本….txt"},
	} {
		if got := cutMiddle(c.s, c.w); got != c.want {
			t.Errorf("cutMiddle(%q, %d) = %q, want %q", c.s, c.w, got, c.want)
		}
	}
	// Each cut fits, starts as the name starts and ends as it ends.
	for _, s := range []string{"abcdefghij", "日本語のファイル名です.txt", "ab日本cd", "a日b本c語d", part1} {
		n := ansi.StringWidth(s)
		for w := 0; w <= n+1; w++ {
			got := cutMiddle(s, w)
			if ansi.StringWidth(got) > w {
				t.Errorf("cutMiddle(%q, %d) = %q: %d cells", s, w, got, ansi.StringWidth(got))
			}
			head, tail, cut := strings.Cut(got, "…")
			if w >= n {
				cut = got != s
			} else {
				cut = !cut || !strings.HasPrefix(s, head) || !strings.HasSuffix(s, tail)
			}
			if cut && w > 0 {
				t.Errorf("cutMiddle(%q, %d) = %q is not the name's start and end", s, w, got)
			}
		}
	}
}

// U20: a wrapped line keeps its indent, so the pieces of a folder's path
// stay under its "in L:".
func TestConfirmDialogKeepsTheIndentOfAWrappedLine(t *testing.T) {
	var d ConfirmDialog
	d.Open("title", []string{"", "  in L: /aaaa/bbbb/cccc/dddd/eeee/ffff/gggg"}, false)
	textAt := func(line string) int { // the cell the text starts at, inside the box
		_, in, ok := strings.Cut(line, "│")
		if !ok || strings.Trim(in, " │") == "" {
			return -1
		}
		return ansi.StringWidth(in) - ansi.StringWidth(strings.TrimLeft(in, " "))
	}
	var pieces []string
	at := -1
	for _, line := range strings.Split(ansi.Strip(d.View(30, 14)), "\n") {
		if strings.Contains(line, "in L:") {
			at = textAt(line)
		}
		if at >= 0 && strings.Contains(line, "/") {
			pieces = append(pieces, line)
			if c := textAt(line); c != at {
				t.Errorf("a piece starts at cell %d, its first line at %d: %q", c, at, line)
			}
		}
	}
	if len(pieces) < 2 {
		t.Errorf("the line did not wrap in a box of 30: %q", pieces)
	}
}
