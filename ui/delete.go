package ui

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transfer"
	"sc/transport"
)

type DeleteProgress struct {
	Total  atomic.Int64
	Done   atomic.Int64
	Failed atomic.Int64 // sides whose delete failed
	File   atomic.Value
	Side   atomic.Value
	Start  atomic.Int64
}

func (p *DeleteProgress) reset(total int64, file, side string) {
	p.Done.Store(0)
	p.Failed.Store(0)
	p.Total.Store(total)
	p.File.Store(file)
	p.Side.Store(side)
	p.Start.Store(time.Now().UnixNano())
}

// deleteSide is one side a delete runs on.
type deleteSide struct {
	backend model.Backend
	name    string
}

// pendingDeleteInfo is a delete waiting in its dialog. presence is the
// node's presence when the dialog opened: the sides the user was asked about.
type pendingDeleteInfo struct {
	node     *model.TreeNode
	presence model.Presence
}

// openDelete asks before deleting the node; a both-side node also asks which
// side.
func (m *Model) openDelete(node *model.TreeNode) {
	if node == nil {
		return
	}
	m.readTree(func(*model.TreeNode) {
		m.pendingDelete = &pendingDeleteInfo{node: node, presence: node.Compare.Presence}
		m.buildDeleteConfirm(node)
	})
}

func (m *Model) buildDeleteConfirm(node *model.TreeNode) {
	if node.Compare.Presence != model.PresenceBoth {
		sides := "← left side only"
		if node.Compare.Presence == model.PresenceRightOnly {
			sides = "right side only →"
		}
		if !node.IsDir {
			m.confirm.Open("Delete "+node.Name+"?", []string{"", sides}, false)
			return
		}
		m.confirm.Open("⚠ RECURSIVE DELETE", []string{"", node.Name + "/", describeSubtree(node), sides}, true)
		return
	}

	if !node.IsDir {
		m.confirm.OpenChoice("Delete "+node.Name+"?", []string{""}, false)
		return
	}
	m.confirm.OpenChoice("⚠ RECURSIVE DELETE", []string{"", node.Name + "/", describeSubtree(node)}, true)
}

// describeSubtree summarises what a recursive delete would remove.
func describeSubtree(node *model.TreeNode) string {
	files, dirs, complete := model.CountDescendants(node)
	return describeTally(transfer.Tally{Files: files, Dirs: dirs, Complete: complete})
}

// describeTally says what a folder holds.
func describeTally(t transfer.Tally) string {
	switch {
	case !t.Complete:
		return fmt.Sprintf("%d+ files, %d+ folders (not fully scanned)", t.Files, t.Dirs)
	case t.Files == 0 && t.Dirs == 0:
		return "empty folder"
	}
	return fmt.Sprintf("%d files, %d folders", t.Files, t.Dirs)
}

// confirmDelete runs the pending delete on side. When a rescan changed the
// node's sides while the dialog was open, the answer was about other sides
// than the node has now: nothing is deleted and the dialog asks again.
func (m *Model) confirmDelete(side model.Presence) tea.Cmd {
	pd := m.pendingDelete
	if m.presence(pd.node) != pd.presence {
		m.openDelete(pd.node)
		m.confirm.lines = append(m.confirm.lines, "", "⚠ This changed while the dialog was open. Check the sides again.")
		return nil
	}
	node := pd.node
	m.pendingDelete = nil
	m.confirm.Close()
	return m.deleteNode(node, side)
}

// deleteNode starts the delete: it removes the node from the chosen sides,
// then re-lists its parent. Progress counts the subtree once per side, so
// the bar moves even when a backend deletes recursively in one call.
func (m *Model) deleteNode(node *model.TreeNode, side model.Presence) tea.Cmd {
	if !m.mayStart(opDelete) {
		return nil
	}
	scanner, opts, progress := m.scanner, *m.cmpOpts, m.deleteProgress
	var relPath string
	var isDir bool
	var files, dirs int
	m.readTree(func(*model.TreeNode) {
		relPath, isDir = node.RelPath, node.IsDir
		files, dirs, _ = model.CountDescendants(node)
	})
	var sides []deleteSide
	if side != model.PresenceRightOnly {
		sides = append(sides, deleteSide{m.left.backend, "left"})
	}
	if side != model.PresenceLeftOnly {
		sides = append(sides, deleteSide{m.right.backend, "right"})
	}
	label := "BOTH"
	switch side {
	case model.PresenceLeftOnly:
		label = "LEFT"
	case model.PresenceRightOnly:
		label = "RIGHT"
	}
	perSide := int64(files + dirs)
	if isDir {
		perSide++
	}
	perSide = max(perSide, 1)
	progress.reset(perSide*int64(len(sides)), relPath, label)
	return m.start(opDelete, func(ctx context.Context, done opDoneMsg) doneMsg {
		for _, s := range sides {
			if ctx.Err() != nil {
				break
			}
			if err := removeOne(ctx, s.backend, relPath, isDir); err != nil {
				progress.Failed.Add(1)
				transport.Log.Add("delete", transport.DirFail, s.name+" "+relPath+": "+err.Error())
			} else {
				transport.Log.Add("delete", transport.DirIn, s.name+" "+relPath)
			}
			progress.Done.Add(perSide)
		}
		refreshParent(ctx, scanner, relPath, opts)
		return done
	})
}

func removeOne(ctx context.Context, backend model.Backend, relPath string, isDir bool) error {
	if isDir {
		return backend.RemoveAll(ctx, relPath)
	}
	return backend.Remove(ctx, relPath)
}
