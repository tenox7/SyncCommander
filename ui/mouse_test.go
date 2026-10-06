package ui

import (
	"strings"
	"testing"
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
	if m.leftPanel.cursor != 1 || m.rightPanel.cursor != 1 {
		t.Fatalf("cursors %d/%d after wheel down, down, up; want 1/1", m.leftPanel.cursor, m.rightPanel.cursor)
	}
	press(m, "?")
	mouse(m, tea.MouseButtonWheelDown, 0, 0)
	if m.leftPanel.cursor != 1 {
		t.Fatal("wheel moved the cursor under the help dialog")
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
	if m.leftPanel.cursor != 0 {
		t.Fatal("a click on the top bar moved the cursor")
	}
	for _, p := range []*Panel{m.rightPanel, m.leftPanel} {
		x0 := 0
		if p == m.rightPanel {
			x0 = m.leftPanel.width
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
	p := m.leftPanel
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
