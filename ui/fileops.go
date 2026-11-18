package ui

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

// renameDoneMsg is how a rename reports back: its error, and the node to
// rescan when the rename joined it with a row of the other side.
type renameDoneMsg struct {
	opDoneMsg
	err    error
	rescan *model.TreeNode
}

// undoTimeout bounds the undo of a half-done rename, which runs also after
// x: a server that hangs on it does not hold the keys for ever.
const undoTimeout = 2 * time.Minute

func (m *Model) openRename(node *model.TreeNode) {
	if node == nil || m.isTopRow(node) {
		return
	}
	var oldName string
	m.readTree(func(*model.TreeNode) { oldName = node.Name })
	m.input.Open("Rename: "+oldName, oldName, func(newName string) tea.Cmd {
		if newName == "" || newName == oldName {
			return nil
		}
		if !utf8.ValidString(oldName) && strings.ContainsRune(newName, utf8.RuneError) {
			// The editor shows each byte that is not UTF-8 as U+FFFD, and
			// the new name would hold that sign in place of the byte.
			m.input.SetError("the name has bytes that are not text, shown as \ufffd: type the new name in full")
			return nil
		}
		if strings.ContainsAny(newName, "/\\") {
			transport.Log.Add("rename", transport.DirErr, oldName+": a name cannot contain a path separator")
			return nil
		}
		if !model.IsFileName(newName) {
			// "." and ".." name the folder itself and its parent (W-C2).
			m.input.SetError(fmt.Sprintf("%q is no file name", newName))
			return nil
		}
		var taken string
		m.readTree(func(*model.TreeNode) { taken = renameTaken(node, newName) })
		if taken != "" {
			m.input.SetError(taken)
			return nil
		}
		return m.renameNode(node, newName)
	})
}

// renameTaken says where newName is already used next to node, on a side
// the rename writes to (the sides node exists on), or "" when it is free
// there. The rename would replace that entry: a file, or a whole folder on
// WebDAV. A name used only on the other side is fine: the rename then joins
// the two rows into one.
func renameTaken(node *model.TreeNode, newName string) string {
	if node.Parent == nil {
		return ""
	}
	var taken [2]bool
	for _, sib := range node.Parent.Children {
		if sib == node || sib.Name != newName {
			continue
		}
		for s := range taken {
			taken[s] = taken[s] || node.Sides[s].Entry != nil && sib.Sides[s].Entry != nil
		}
	}
	switch {
	case taken[model.SideLeft] && taken[model.SideRight]:
		return newName + " already exists on both sides"
	case taken[model.SideLeft]:
		return newName + " already exists on the left"
	case taken[model.SideRight]:
		return newName + " already exists on the right"
	}
	return ""
}

// renameNode starts the rename of the node on every side it exists on. When
// a side fails the parent is re-listed, so the tree shows what is really
// there instead of the old row. A rename that x stopped is not a failure:
// the log says the user canceled it.
func (m *Model) renameNode(node *model.TreeNode, newName string) tea.Cmd {
	left, right, scanner, opts := m.left.backend, m.right.backend, m.scanner, *m.cmpOpts
	var oldRel string
	var presence model.Presence
	m.readTree(func(*model.TreeNode) { oldRel, presence = node.RelPath, node.Compare.Presence })
	newRel := newName
	if dir := model.DirOf(oldRel); dir != "" {
		newRel = dir + "/" + newName
	}
	return m.start(opRename, func(ctx context.Context, done opDoneMsg) doneMsg {
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
			if ctx.Err() == nil {
				transport.Log.Add("rename", transport.DirFail, oldRel+" -> "+newRel+": "+err.Error())
			}
			refreshParent(ctx, scanner, oldRel, opts)
			return renameDoneMsg{opDoneMsg: done, err: err}
		}
		if scanner.RenameNode(node, newName, newRel, oldRel, opts) {
			return renameDoneMsg{opDoneMsg: done, rescan: node}
		}
		return renameDoneMsg{opDoneMsg: done}
	})
}

// renameBoth renames on both sides, undoing the left rename when the right
// one fails so the sides stay in step. The undo runs also when x stopped the
// rename: a stop must not leave the sides apart. A failed undo is logged and
// the parent refresh shows the split.
func renameBoth(ctx context.Context, left, right model.Backend, oldRel, newRel string) error {
	if err := left.Rename(ctx, oldRel, newRel); err != nil {
		return err
	}
	err := right.Rename(ctx, oldRel, newRel)
	if err == nil {
		return nil
	}
	undo, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoTimeout)
	defer cancel()
	if uerr := left.Rename(undo, newRel, oldRel); uerr != nil {
		transport.Log.Add("rename", transport.DirErr, "undo left "+newRel+" -> "+oldRel+": "+uerr.Error())
	}
	return err
}

// touchNode starts the touch: it copies the newer side's timestamps onto the
// older side of a file present on both. A touch that x stopped is not a
// failure: the log says the user canceled it.
func (m *Model) touchNode(node *model.TreeNode) tea.Cmd {
	if node == nil || m.isTopRow(node) || m.presence(node) != model.PresenceBoth {
		return nil
	}
	left, right, scanner, opts := m.left.backend, m.right.backend, m.scanner, *m.cmpOpts
	return m.start(opTouch, func(ctx context.Context, done opDoneMsg) doneMsg {
		var l, r *model.FileEntry
		var relPath string
		scanner.ReadTree(func(*model.TreeNode) { l, r = node.Entries(); relPath = node.RelPath })
		if l == nil || r == nil {
			return done
		}
		newer, older := l, r
		olderBackend, touched, side := right, model.SideRight, "right"
		if r.ModTime.After(l.ModTime) {
			newer, older = r, l
			olderBackend, touched, side = left, model.SideLeft, "left"
		}
		if newer.ModTime.IsZero() {
			// Neither side has a time to give: a zero one was set, which FTP
			// wrote as year 1 (L25).
			transport.Log.Add("touch", transport.DirWarn, older.RelPath+": neither side has a time to give the other")
			return done
		}
		if err := olderBackend.SetTimes(ctx, older.RelPath, newer.ModTime, newer.ATime.Time(), newer.BirthTime.Time()); err != nil {
			// A failed touch logged nothing (U9).
			if ctx.Err() == nil {
				transport.Log.Add("touch", transport.DirFail, side+" "+older.RelPath+": "+err.Error())
			}
		} else {
			// Touch only changes metadata; the file body is unchanged. Roll the
			// cached CRC fingerprint forward to the new mtime so the preserving
			// merge below treats CRC as still valid.
			scanner.MutateTree(func(*model.TreeNode) { node.Sides[touched].ChecksumModTime = model.NanoOf(newer.ModTime) })
		}
		refreshParent(ctx, scanner, relPath, opts)
		return done
	})
}
