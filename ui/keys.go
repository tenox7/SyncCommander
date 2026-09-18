package ui

import (
	tea "github.com/charmbracelet/bubbletea"

	"sc/transport"
)

// handleKey routes a key press: ctrl+c always quits, an open dialog owns the
// keyboard, a running transfer allows only its own few keys.
func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	k := msg.String()
	if k == "ctrl+c" {
		m.cancelAll()
		return m, tea.Quit
	}
	if d := m.openModal(); d != nil {
		return m, m.handleModalKey(d, msg)
	}
	if m.transferring() {
		m.handleTransferKey(k)
		return m, nil
	}
	return m, m.handleTreeKey(k)
}

func (m *Model) handleTransferKey(k string) {
	switch k {
	case "x", "X":
		m.cancelTransfer()
	case "~", "`":
		m.logView.Open()
	case "-":
		m.adjustCopyParallel(-1)
	case "+", "=":
		m.adjustCopyParallel(+1)
	}
}

// cancelTransfer asks the running copy or delete to stop; its done message
// clears the flag.
func (m *Model) cancelTransfer() {
	cancel, op, what := m.copyProgress.Cancel.Load(), "copy", "transfer"
	if m.deleting {
		cancel, op, what = m.deleteProgress.Cancel.Load(), "delete", "delete"
	}
	if cancel == nil {
		return
	}
	transport.Log.Add(op, transport.DirIn, "user canceled "+what)
	(*cancel)()
}

// handleTreeKey is the idle key set: navigation, and everything that starts
// an operation or opens a dialog.
func (m *Model) handleTreeKey(k string) tea.Cmd {
	switch k {
	case "q":
		m.cancelOps()
		return tea.Quit
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
		m.settings.Open()
	case "s", "S":
		if !m.busy() {
			m.swapSides()
		}
	case "w":
		m.leftPanel.wrap = !m.leftPanel.wrap
		m.rightPanel.wrap = !m.rightPanel.wrap
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
