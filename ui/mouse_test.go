package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

func mouse(m *Model, b tea.MouseButton, x, y int) {
	m.Update(tea.MouseMsg{X: x, Y: y, Button: b, Action: tea.MouseActionPress})
}

func TestWheelMovesCursorOnlyWhereArrowsScroll(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	mouse(m, tea.MouseButtonWheelDown, 0, 0)
	mouse(m, tea.MouseButtonWheelDown, 0, 0)
	mouse(m, tea.MouseButtonWheelUp, 0, 0)
	if m.left.panel.cursor != 1 || m.right.panel.cursor != 1 {
		t.Fatalf("cursors %d/%d after wheel down, down, up; want 1/1", m.left.panel.cursor, m.right.panel.cursor)
	}
	press(m, "?")
	mouse(m, tea.MouseButtonWheelDown, 0, 0)
	if m.left.panel.cursor != 1 {
		t.Fatal("wheel moved the cursor under the help dialog")
	}
}

// A dialog gives the mouse to the terminal, so its text can be selected
// and copied; closing it takes the mouse back for the panels. The log also
// opens by itself on a tick, so every message is followed by the check.
// With -mouse=false sc never takes the mouse.
func TestDialogsGiveTheMouseToTheTerminal(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	m.SetMouse(true)
	want := func(what string, cmd tea.Cmd, msg tea.Msg) {
		t.Helper()
		if cmd == nil {
			t.Errorf("%s sent nothing, want %T", what, msg)
		} else if got := cmd(); fmt.Sprintf("%T", got) != fmt.Sprintf("%T", msg) {
			t.Errorf("%s sent %T, want %T", what, got, msg)
		}
	}
	for _, k := range []string{"i", "~", "?", "="} {
		want(k+" (opens a dialog)", press(m, k), tea.DisableMouse())
		if cmd := press(m, "down"); cmd != nil {
			t.Errorf("%s: a key inside the dialog sent %T", k, cmd())
		}
		want(k+" then esc (closes it)", press(m, "esc"), tea.EnableMouseCellMotion())
	}
	m.logView.Open() // as autoOpenLog does on a tick
	_, cmd := m.Update(tickMsg(time.Now()))
	want("a tick under a log that opened by itself", cmd, tea.DisableMouse())
	press(m, "esc")

	m.SetMouse(false)
	if cmd := press(m, "i"); cmd != nil {
		t.Errorf("with -mouse=false, a dialog sent %T", cmd())
	}
}

// arrowCol finds the arrow in the rendered row, so the test follows the
// layout instead of restating it.
func arrowCol(p *Panel, i int) int {
	line := ansi.Strip(p.renderRow(p.rows[i]))
	return utf8.RuneCountInString(line[:strings.IndexAny(line, "▶▼")])
}

func rowOf(p *Panel, n *model.TreeNode) int {
	i := 0
	for p.rows[i].Node != n {
		i++
	}
	return i
}

func topDir(m *Model) *model.TreeNode {
	return firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.IsDir && n.Depth == 1 })
}

func TestClickSelectsRowAndArrowToggles(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	dir := topDir(m)
	mouse(m, tea.MouseButtonLeft, 5, 0)
	if m.left.panel.cursor != 0 {
		t.Fatal("a click on the top bar moved the cursor")
	}
	for _, p := range []*Panel{m.right.panel, m.left.panel} {
		x0 := 0
		if p == m.right.panel {
			x0 = m.left.panel.width
		}
		i := rowOf(p, dir)
		y, col := i-p.offset+1, arrowCol(p, i)
		mouse(m, tea.MouseButtonLeft, x0+col+2, y)
		if m.activePanel() != p || p.CursorNode() != dir || m.inactivePanel().cursor != i || dir.Expanded {
			t.Fatalf("side %d name click: active=%v cursor on %v expanded=%v", p.side, m.activePanel() == p, p.CursorNode(), dir.Expanded)
		}
		for _, x := range []int{0, col, col + 1} {
			for _, want := range []bool{true, false} {
				mouse(m, tea.MouseButtonLeft, x0+x, y)
				if dir.Expanded != want || p.CursorNode() != dir {
					t.Fatalf("side %d: click at col %d left expanded=%v cursor on %v", p.side, x, dir.Expanded, p.CursorNode())
				}
			}
		}
	}
}

// A double-click is two selecting clicks: the second toggles, a third starts
// over, and a click after an arrow click only selects.
func TestDoubleClickOnNameToggles(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	dir := topDir(m)
	p := m.left.panel
	i := rowOf(p, dir)
	arrow, y := arrowCol(p, i), i-p.offset+1
	for n, want := range []bool{false, true, true, false} {
		mouse(m, tea.MouseButtonLeft, arrow+2, y)
		if dir.Expanded != want {
			t.Fatalf("name click %d: expanded=%v, want %v", n+1, dir.Expanded, want)
		}
	}
	mouse(m, tea.MouseButtonLeft, arrow+2, y)
	m.lastSelect = m.lastSelect.Add(-doubleClickTime)
	mouse(m, tea.MouseButtonLeft, arrow+2, y)
	if dir.Expanded {
		t.Fatal("two clicks doubleClickTime apart toggled")
	}
	mouse(m, tea.MouseButtonLeft, arrow, y)
	mouse(m, tea.MouseButtonLeft, arrow+2, y)
	if !dir.Expanded {
		t.Fatal("a name click right after an arrow click toggled back")
	}
}
