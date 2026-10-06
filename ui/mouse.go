package ui

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
)

const doubleClickTime = 500 * time.Millisecond

// handleMouse turns the wheel into up/down and a left click into a cursor
// move, or an expand/collapse on a row's guides or arrow or a double-click.
func (m *Model) handleMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	if msg.Action != tea.MouseActionPress {
		return m, nil
	}
	switch msg.Button {
	case tea.MouseButtonWheelUp:
		return m.wheel(tea.KeyUp)
	case tea.MouseButtonWheelDown:
		return m.wheel(tea.KeyDown)
	case tea.MouseButtonLeft:
		return m, m.click(msg.X, msg.Y)
	}
	return m, nil
}

// wheel acts as the arrow key only where up/down scroll a list; in the open
// dialog they switch fields.
func (m *Model) wheel(k tea.KeyType) (tea.Model, tea.Cmd) {
	switch m.openModal().(type) {
	case nil, *LogDialog, *DiffView, *SettingsDialog:
		return m.handleKey(tea.KeyMsg{Type: k})
	}
	return m, nil
}

// click puts the cursor on the clicked row and activates its panel. A click on
// the row's guides or arrow, or the second of two selecting clicks on it within
// doubleClickTime, also expands or collapses it, like Enter.
func (m *Model) click(x, y int) tea.Cmd {
	if m.openModal() != nil || m.transferring() {
		return nil
	}
	p := m.leftPanel
	if x >= p.width {
		p, x = m.rightPanel, x-p.width
	}
	i := p.offset + y - 1 // the top bar is screen row 0
	if y < 1 || y > p.height || i >= len(p.rows) {
		return nil
	}
	double := i == p.cursor && time.Since(m.lastSelect) < doubleClickTime
	if p != m.activePanel() {
		m.switchPanel()
	}
	p.cursor = i
	m.syncPanels()
	onArrow := false
	m.readTree(func(*model.TreeNode) { onArrow = p.onArrow(i, x) })
	if !onArrow && !double {
		m.lastSelect = time.Now()
		return nil
	}
	m.lastSelect = time.Time{}
	return m.toggleCursor()
}
