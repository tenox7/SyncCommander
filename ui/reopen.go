package ui

import (
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

// openLocations shows both open arguments for editing.
func (m *Model) openLocations() {
	m.openDlg.Open(transport.MaskURLPassword(m.leftArg), transport.MaskURLPassword(m.rightArg))
}

// openParent reopens both sides one directory up.
func (m *Model) openParent() tea.Cmd {
	leftPath, rightPath := transport.ParentPath(m.leftArg), transport.ParentPath(m.rightArg)
	if leftPath == m.leftArg && rightPath == m.rightArg {
		return nil
	}
	return m.reopenOrExplain(leftPath, rightPath)
}

// descendCursor reopens the cursor directory as the root, on every side it
// exists on.
func (m *Model) descendCursor() tea.Cmd {
	node := m.activePanel().CursorNode()
	if node == nil || !node.IsDir {
		return nil
	}
	leftPath, rightPath := m.leftArg, m.rightArg
	descend := false
	m.readTree(func(*model.TreeNode) {
		l, r := node.Entries()
		descend = l != nil || r != nil
		if l != nil {
			leftPath = childArg(leftPath, node.RelPath)
		}
		if r != nil {
			rightPath = childArg(rightPath, node.RelPath)
		}
	})
	if !descend {
		return nil
	}
	return m.reopenOrExplain(leftPath, rightPath)
}

// reopenOrExplain reopens both sides or, when one cannot be opened, shows the
// attempted locations in the URL dialog with the error, so a failed parent or
// descend key is never silent.
func (m *Model) reopenOrExplain(leftPath, rightPath string) tea.Cmd {
	cmd, errMsg := m.reopenBackends(leftPath, rightPath)
	if errMsg == "" {
		return cmd
	}
	m.openDlg.Open(transport.MaskURLPassword(leftPath), transport.MaskURLPassword(rightPath))
	m.openDlg.SetError(errMsg)
	return nil
}

// reopenBackends switches to new open arguments; an unchanged side keeps its
// connection. The change dialog shows masked arguments, so a value equal to
// the mask means "unchanged".
func (m *Model) reopenBackends(leftPath, rightPath string) (tea.Cmd, string) {
	if leftPath == transport.MaskURLPassword(m.leftArg) {
		leftPath = m.leftArg
	}
	if rightPath == transport.MaskURLPassword(m.rightArg) {
		rightPath = m.rightArg
	}
	var newLeft, newRight model.Backend
	var err error

	if leftPath != m.leftArg {
		newLeft, err = transport.TryOpenBackend(leftPath, m.insecure, m.copyParallel)
		if err != nil {
			return nil, "left: " + err.Error()
		}
	}
	if rightPath != m.rightArg {
		newRight, err = transport.TryOpenBackend(rightPath, m.insecure, m.copyParallel)
		if err != nil {
			if newLeft != nil {
				transport.CloseBackend(newLeft)
			}
			return nil, "right: " + err.Error()
		}
	}

	m.cancelOps()

	if newLeft != nil {
		oldLeft := m.left
		go func() { transport.CloseBackend(oldLeft) }()
		m.left, m.leftArg = newLeft, leftPath
	}
	if newRight != nil {
		oldRight := m.right
		go func() { transport.CloseBackend(oldRight) }()
		m.right, m.rightArg = newRight, rightPath
	}

	m.scanner = model.NewScanner(m.left, m.right, 4, m.scanParallel, m.deepScan)
	m.leftPanel.title = m.left.BasePath()
	m.rightPanel.title = m.right.BasePath()
	m.leftPanel.SetRows(nil)
	m.rightPanel.SetRows(nil)
	return m.startScan(), ""
}

// childArg extends an open argument, URL or local path, by relPath.
func childArg(arg, relPath string) string {
	if transport.IsRemote(arg) {
		return strings.TrimRight(arg, "/") + "/" + relPath
	}
	return filepath.Join(arg, filepath.FromSlash(relPath))
}
