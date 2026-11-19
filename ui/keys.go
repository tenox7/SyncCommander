package ui

import (
	tea "github.com/charmbracelet/bubbletea"
)

// handleKey routes a key press: ctrl+c always quits, an open dialog owns the
// keyboard, a running write allows only its own few keys.
func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	k := msg.String()
	if k == "ctrl+c" {
		m.cancelOps()
		return m, tea.Quit
	}
	if d := m.openModal(); d != nil {
		cmd := m.handleModalKey(d, msg)
		// The log a dialog held back opens once it is closed (U18).
		if m.logWaits {
			m.autoOpenLog()
		}
		return m, cmd
	}
	if m.writing() {
		m.handleWriteKey(k)
		return m, nil
	}
	return m, m.handleTreeKey(k)
}

// handleWriteKey is the key set while a copy, a delete, a rename or a touch
// runs: the write holds the keys, so nothing starts beside it (mayStart).
// x stops it; + and - change a copy's files in flight.
func (m *Model) handleWriteKey(k string) {
	switch k {
	case "x", "X":
		m.stopWrite()
	case "~", "`":
		m.logView.Open()
	case "-":
		m.adjustCopyParallel(-1)
	case "+", "=":
		m.adjustCopyParallel(+1)
	}
}

// handleTreeKey is the idle key set: navigation, and everything that starts
// an operation or opens a dialog.
func (m *Model) handleTreeKey(k string) tea.Cmd {
	switch k {
	case "q":
		m.cancelOps()
		return tea.Quit
	case "x", "X":
		m.stopScanOps()
	case "tab":
		m.switchPanel()
	case "up", "k":
		m.move((*Panel).MoveUp)
	case "down", "j":
		m.move((*Panel).MoveDown)
	case "pgup":
		m.move((*Panel).PageUp)
	case "pgdown":
		m.move((*Panel).PageDown)
	case "enter", "right", "l":
		return m.toggleCursor()
	case "left", "h":
		m.collapseCursor()
	case "n":
		if !m.busy() {
			m.jumpToNextDiff()
		}
	case "/":
		m.openSearch()
	case "}":
		m.setExpandedAll(true)
	case "{":
		m.setExpandedAll(false)
	case "r":
		return m.rescanCursor()
	case "R":
		return m.deepRescanAll()
	case "t":
		return m.touchNode(m.activePanel().CursorNode())
	case "c":
		return m.checksumNode(m.activePanel().CursorFile())
	case "e":
		m.openRename(m.activePanel().CursorNode())
	case "d":
		m.openDelete(m.activePanel().CursorNode())
	case ">":
		return m.startCopy(true)
	case "<":
		return m.startCopy(false)
	case "=":
		m.updateChecksumLabel()
		m.settings.Open()
	case "s", "S":
		if !m.busy() {
			m.swapSides()
		}
	case "w":
		m.left.panel.wrap = !m.left.panel.wrap
		m.right.panel.wrap = !m.right.panel.wrap
	case "ctrl+l":
		return tea.ClearScreen
	case "?":
		m.help.Open()
	case "~", "`":
		m.logView.Open()
	case "i":
		m.openInfo()
	case "y":
		m.openLocations()
	case "u":
		return m.openParent()
	case "o":
		return m.openDiff(m.activePanel().CursorFile())
	case "b":
		return m.descendCursor()
	}
	return nil
}
