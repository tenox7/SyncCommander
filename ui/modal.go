package ui

import (
	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
)

// modal is a dialog that owns the screen and the keyboard while open.
type modal interface {
	IsOpen() bool
	View(width, height int) string
}

// openModal returns the open dialog, if any, in one fixed priority order
// shared by key routing and rendering.
func (m *Model) openModal() modal {
	for _, d := range []modal{m.diffView, m.logView, m.info, m.help, m.confirm, m.input, m.openDlg, m.settings} {
		if d.IsOpen() {
			return d
		}
	}
	return nil
}

func (m *Model) handleModalKey(d modal, msg tea.KeyMsg) tea.Cmd {
	switch d := d.(type) {
	case *DiffView:
		m.handleDiffViewKey(msg)
	case *LogDialog:
		handleLogKey(d, msg)
	case *InfoDialog:
		if k := msg.String(); k == "esc" || k == "q" || k == "i" {
			d.Close()
		}
	case *HelpDialog:
		if k := msg.String(); k == "esc" || k == "q" || k == "?" {
			d.Close()
		}
	case *ConfirmDialog:
		return m.handleConfirmKey(msg)
	case *InputDialog:
		return m.handleInputKey(msg)
	case *OpenDialog:
		return m.handleOpenDlgKey(msg)
	case *SettingsDialog:
		m.handleSettingsKey(msg)
	}
	return nil
}

func handleLogKey(d *LogDialog, msg tea.KeyMsg) {
	switch msg.String() {
	case "esc", "q", "~", "`":
		d.Close()
	case "up", "k":
		d.ScrollUp()
	case "down", "j":
		d.ScrollDown()
	case "pgup":
		d.PageUp()
	case "pgdown":
		d.PageDown()
	case "home":
		d.Home()
	case "end":
		d.End()
	case "e":
		d.ToggleErrFilter()
	}
}

func (m *Model) handleDiffViewKey(msg tea.KeyMsg) {
	switch msg.String() {
	case "esc", "q":
		m.closeDiff()
	case "up", "k":
		m.diffView.ScrollUp()
	case "down", "j":
		m.diffView.ScrollDown()
	case "pgup":
		m.diffView.PageUp()
	case "pgdown":
		m.diffView.PageDown()
	case "home":
		m.diffView.Home()
	case "end":
		m.diffView.End()
	case "n":
		m.diffView.NextDiff()
	case "p":
		m.diffView.PrevDiff()
	}
}

func (m *Model) handleSettingsKey(msg tea.KeyMsg) {
	switch msg.String() {
	case "esc", "s", "q":
		m.settings.Close()
	case "up", "k":
		m.settings.MoveUp()
	case "down", "j":
		m.settings.MoveDown()
	case "left", "h", "-":
		m.settings.Adjust(-1)
	case "right", "l", "+", "=":
		m.settings.Adjust(1)
	case " ", "enter":
		m.settings.Toggle()
	}
}

func (m *Model) handleInputKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.input.Close()
	case "enter":
		return m.input.Confirm()
	default:
		m.input.HandleKey(msg)
	}
	return nil
}

func (m *Model) handleOpenDlgKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.openDlg.Close()
	case "enter":
		leftPath, rightPath := m.openDlg.Values()
		if leftPath == "" || rightPath == "" {
			m.openDlg.SetError("both paths are required")
			return nil
		}
		cmd, errMsg := m.reopenBackends(leftPath, rightPath)
		if errMsg != "" {
			m.openDlg.SetError(errMsg)
			return nil
		}
		m.openDlg.Close()
		return cmd
	default:
		m.openDlg.HandleKey(msg)
	}
	return nil
}

// handleConfirmKey commits or drops the pending delete or copy. A both-side
// delete asks which side: a=left, l=right, enter=both.
func (m *Model) handleConfirmKey(msg tea.KeyMsg) tea.Cmd {
	k := msg.String()
	if m.confirm.choiceMode && m.pendingDelete != nil {
		switch k {
		case "a":
			return m.confirmDelete(model.PresenceLeftOnly)
		case "l":
			return m.confirmDelete(model.PresenceRightOnly)
		case "enter":
			return m.confirmDelete(model.PresenceBoth)
		case "esc", "q":
			m.cancelConfirm()
		}
		return nil
	}
	switch k {
	case "y", "Y":
		switch {
		case m.pendingDelete != nil:
			return m.confirmDelete(m.presence(m.pendingDelete))
		case m.pendingCopy != nil:
			return m.confirmCopy()
		}
		m.confirm.Close()
	case "esc", "n", "N", "q":
		m.cancelConfirm()
	}
	return nil
}

func (m *Model) cancelConfirm() {
	m.confirm.Close()
	m.pendingDelete, m.pendingCopy = nil, nil
}
