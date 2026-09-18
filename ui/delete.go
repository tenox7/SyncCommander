package ui

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

type deleteDoneMsg struct{}

type DeleteProgress struct {
	Total  atomic.Int64
	Done   atomic.Int64
	File   atomic.Value
	Side   atomic.Value
	Start  atomic.Int64
	Cancel atomic.Pointer[context.CancelFunc]
}

func (p *DeleteProgress) reset(total int64, file, side string, cancel context.CancelFunc) {
	p.Cancel.Store(&cancel)
	p.Done.Store(0)
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

// openDelete asks before deleting the node; a both-side node also asks which
// side.
func (m *Model) openDelete(node *model.TreeNode) {
	if node == nil {
		return
	}
	m.pendingDelete = node
	m.readTree(func(*model.TreeNode) { m.buildDeleteConfirm(node) })
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
	switch {
	case !complete:
		return fmt.Sprintf("%d+ files, %d+ folders (not fully scanned)", files, dirs)
	case files == 0 && dirs == 0:
		return "empty folder"
	}
	return fmt.Sprintf("%d files, %d folders", files, dirs)
}

// confirmDelete runs the pending delete on side.
func (m *Model) confirmDelete(side model.Presence) tea.Cmd {
	node := m.pendingDelete
	m.pendingDelete = nil
	m.confirm.Close()
	m.deleting = true
	return tea.Batch(m.deleteNode(node, side), m.ensureTick())
}

func (m *Model) finishDelete() {
	m.deleting = false
	cancelStored(&m.deleteProgress.Cancel)
	m.autoOpenLog()
	m.refreshTreeNow()
}

// deleteNode removes the node from the chosen sides, then re-lists its parent.
// Progress counts the subtree once per side, so the bar moves even when a
// backend deletes recursively in one call.
func (m *Model) deleteNode(node *model.TreeNode, side model.Presence) tea.Cmd {
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
		sides = append(sides, deleteSide{m.left, "left"})
	}
	if side != model.PresenceLeftOnly {
		sides = append(sides, deleteSide{m.right, "right"})
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
	ctx, cancel := context.WithCancel(context.Background())
	ctx = transport.ContextWithFatalCancel(ctx, cancel)
	progress.reset(perSide*int64(len(sides)), relPath, label, cancel)
	return func() tea.Msg {
		for _, s := range sides {
			if ctx.Err() != nil {
				break
			}
			if err := removeOne(ctx, s.backend, relPath, isDir); err != nil {
				transport.Log.Add("delete", transport.DirErr, s.name+" "+relPath+": "+err.Error())
			} else {
				transport.Log.Add("delete", transport.DirIn, s.name+" "+relPath)
			}
			progress.Done.Add(perSide)
		}
		refreshParent(scanner, relPath, opts)
		return deleteDoneMsg{}
	}
}

func removeOne(ctx context.Context, backend model.Backend, relPath string, isDir bool) error {
	if isDir {
		return backend.RemoveAll(ctx, relPath)
	}
	return backend.Remove(ctx, relPath)
}
