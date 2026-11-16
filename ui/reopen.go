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
	m.openDlg.Open(transport.MaskURLPassword(m.left.arg), transport.MaskURLPassword(m.right.arg))
}

// openParent reopens both sides one directory up.
func (m *Model) openParent() tea.Cmd {
	leftPath, rightPath := transport.ParentPath(m.left.arg), transport.ParentPath(m.right.arg)
	if leftPath == m.left.arg && rightPath == m.right.arg {
		return nil
	}
	return m.reopenOrExplain(leftPath, rightPath)
}

// descendCursor reopens the cursor folder as the root of both sides. A
// folder that is not a folder on both sides is not gone into: b moved the
// side it was on and left the other at the old root, where every name then
// showed as on that side only, for a mirror to delete (U13). The location
// dialog shows where both sides would go, and why not.
func (m *Model) descendCursor() tea.Cmd {
	node := m.activePanel().CursorNode()
	if node == nil || !node.IsDir || node.IsRoot() {
		return nil
	}
	var l, r *model.FileEntry
	m.readTree(func(*model.TreeNode) { l, r = node.Entries() })
	leftPath, rightPath := childArg(m.left.arg, node.RelPath), childArg(m.right.arg, node.RelPath)
	if why := notFolderOnBoth(node.RelPath, l, r); why != "" {
		m.openDlg.Open(transport.MaskURLPassword(leftPath), transport.MaskURLPassword(rightPath))
		m.openDlg.SetError(why)
		return nil
	}
	return m.reopenOrExplain(leftPath, rightPath)
}

// notFolderOnBoth says why relPath is not a folder on both sides, "" when
// it is.
func notFolderOnBoth(relPath string, left, right *model.FileEntry) string {
	for _, s := range [...]struct {
		name string
		e    *model.FileEntry
	}{{"left", left}, {"right", right}} {
		if s.e == nil || !s.e.IsDir {
			return relPath + " is not a folder on the " + s.name + ": b goes into a folder that is on both sides"
		}
	}
	return ""
}

// Close closes the backends the panes hold now. A reopen may have replaced
// the ones the program started with, and an rclone remote finishes its work
// (dropbox's batched uploads) only when it is closed.
func (m *Model) Close() {
	transport.CloseBackend(m.left.backend)
	transport.CloseBackend(m.right.backend)
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
// connection, unless it stopped connecting (a refused login, a server that
// was gone): opening it again is how it tries again (F7). The change dialog
// shows masked arguments, so a value equal to the mask means "unchanged".
func (m *Model) reopenBackends(leftPath, rightPath string) (tea.Cmd, string) {
	if leftPath == transport.MaskURLPassword(m.left.arg) {
		leftPath = m.left.arg
	}
	if rightPath == transport.MaskURLPassword(m.right.arg) {
		rightPath = m.right.arg
	}
	var newLeft, newRight model.Backend
	var err error

	if leftPath != m.left.arg || transport.NotConnecting(m.left.backend) {
		newLeft, err = transport.TryOpenBackend(leftPath, m.insecure, m.copyParallel)
		if err != nil {
			return nil, "left: " + err.Error()
		}
	}
	if rightPath != m.right.arg || transport.NotConnecting(m.right.backend) {
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
		oldLeft := m.left.backend
		go func() { transport.CloseBackend(oldLeft) }()
		m.left.backend, m.left.arg = newLeft, leftPath
	}
	if newRight != nil {
		oldRight := m.right.backend
		go func() { transport.CloseBackend(oldRight) }()
		m.right.backend, m.right.arg = newRight, rightPath
	}

	m.scanner = model.NewScanner(m.left.backend, m.right.backend, 4, m.scanParallel, m.deepScan)
	m.left.panel.title = m.left.backend.BasePath()
	m.right.panel.title = m.right.backend.BasePath()
	m.left.panel.SetRows(nil)
	m.right.panel.SetRows(nil)
	return m.startScan(), ""
}

// childArg extends an open argument, URL or local path, by relPath.
func childArg(arg, relPath string) string {
	if transport.IsRemote(arg) {
		return strings.TrimRight(arg, "/") + "/" + relPath
	}
	return filepath.Join(arg, filepath.FromSlash(relPath))
}
