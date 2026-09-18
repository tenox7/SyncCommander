package ui

import (
	"context"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

type renameDoneMsg struct {
	err    error
	rescan *model.TreeNode
}

type touchDoneMsg struct{}

func (m *Model) openRename(node *model.TreeNode) {
	if node == nil {
		return
	}
	var oldName string
	m.readTree(func(*model.TreeNode) { oldName = node.Name })
	m.input.Open("Rename: "+oldName, oldName, func(newName string) tea.Cmd {
		if newName == "" || newName == oldName {
			return nil
		}
		if strings.ContainsAny(newName, "/\\") {
			transport.Log.Add("rename", transport.DirErr, oldName+": a name cannot contain a path separator")
			return nil
		}
		return m.renameNode(node, newName)
	})
}

// renameNode renames the node on every side it exists on. When a side fails
// the parent is re-listed, so the tree shows what is really there instead of
// the old row.
func (m *Model) renameNode(node *model.TreeNode, newName string) tea.Cmd {
	left, right, scanner, opts := m.left, m.right, m.scanner, *m.cmpOpts
	var oldRel string
	var presence model.Presence
	m.readTree(func(*model.TreeNode) { oldRel, presence = node.RelPath, node.Compare.Presence })
	newRel := newName
	if dir := model.DirOf(oldRel); dir != "" {
		newRel = dir + "/" + newName
	}
	return func() tea.Msg {
		ctx := context.Background()
		var err error
		switch presence {
		case model.PresenceLeftOnly:
			err = left.Rename(ctx, oldRel, newRel)
		case model.PresenceRightOnly:
			err = right.Rename(ctx, oldRel, newRel)
		default:
			err = renameBoth(ctx, left, right, oldRel, newRel)
		}
		if err != nil {
			transport.Log.Add("rename", transport.DirFail, oldRel+" -> "+newRel+": "+err.Error())
			refreshParent(scanner, oldRel, opts)
			return renameDoneMsg{err: err}
		}
		if scanner.RenameNode(node, newName, newRel, oldRel, opts) {
			return renameDoneMsg{rescan: node}
		}
		return renameDoneMsg{}
	}
}

// renameBoth renames on both sides, undoing the left rename when the right
// one fails so the sides stay in step. A failed undo is logged and the parent
// refresh shows the split.
func renameBoth(ctx context.Context, left, right model.Backend, oldRel, newRel string) error {
	if err := left.Rename(ctx, oldRel, newRel); err != nil {
		return err
	}
	err := right.Rename(ctx, oldRel, newRel)
	if err == nil {
		return nil
	}
	if uerr := left.Rename(ctx, newRel, oldRel); uerr != nil {
		transport.Log.Add("rename", transport.DirErr, "undo left "+newRel+" -> "+oldRel+": "+uerr.Error())
	}
	return err
}

func (m *Model) finishRename(msg renameDoneMsg) tea.Cmd {
	if msg.err != nil {
		m.autoOpenLog()
	}
	m.refreshTreeNow()
	if msg.rescan == nil {
		return nil
	}
	return m.queueRescan(msg.rescan, nil)
}

// touchNode copies the newer side's timestamps onto the older side of a file
// present on both.
func (m *Model) touchNode(node *model.TreeNode) tea.Cmd {
	if node == nil || m.presence(node) != model.PresenceBoth {
		return nil
	}
	left, right, scanner, opts := m.left, m.right, m.scanner, *m.cmpOpts
	return func() tea.Msg {
		var l, r *model.FileEntry
		var relPath string
		scanner.ReadTree(func(*model.TreeNode) { l, r = node.Entries(); relPath = node.RelPath })
		if l == nil || r == nil {
			return touchDoneMsg{}
		}
		newer, older := l, r
		olderBackend, touched := right, model.SideRight
		if r.ModTime.After(l.ModTime) {
			newer, older = r, l
			olderBackend, touched = left, model.SideLeft
		}
		if err := olderBackend.SetTimes(context.Background(), older.RelPath, newer.ModTime, newer.ATime, newer.BirthTime); err == nil {
			// Touch only changes metadata; the file body is unchanged. Roll the
			// cached CRC fingerprint forward to the new mtime so the preserving
			// merge below treats CRC as still valid.
			scanner.MutateTree(func(*model.TreeNode) { node.Sides[touched].ChecksumModTime = newer.ModTime })
		}
		refreshParent(scanner, relPath, opts)
		return touchDoneMsg{}
	}
}
