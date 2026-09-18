package ui

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transfer"
	"sc/transport"
)

type copyDoneMsg struct {
	rescanRoot *model.TreeNode
	changed    *model.ChangedPaths
}

type pendingCopyInfo struct {
	node        *model.TreeNode
	leftToRight bool
}

// startCopy copies the cursor node in the given direction, asking first when
// the copy would destroy anything on the destination.
func (m *Model) startCopy(leftToRight bool) tea.Cmd {
	node := m.activePanel().CursorNode()
	blocked := model.PresenceRightOnly
	title := "⚠ COPY LEFT → RIGHT"
	if !leftToRight {
		blocked, title = model.PresenceLeftOnly, "⚠ COPY RIGHT → LEFT"
	}
	if node == nil || m.presence(node) == blocked {
		return nil
	}
	if lines, ok := m.copyConfirmLines(node, leftToRight); ok {
		m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: leftToRight}
		m.confirm.Open(title, lines, true)
		return nil
	}
	return m.runCopy(node, leftToRight, false)
}

// confirmCopy runs the copy the user just confirmed, mirror deletes included.
func (m *Model) confirmCopy() tea.Cmd {
	pc := m.pendingCopy
	m.pendingCopy = nil
	m.confirm.Close()
	return m.runCopy(pc.node, pc.leftToRight, true)
}

func (m *Model) runCopy(node *model.TreeNode, leftToRight, mirror bool) tea.Cmd {
	m.copying = true
	return tea.Batch(m.copyNode(node, leftToRight, mirror), m.ensureTick())
}

func (m *Model) finishCopy(msg copyDoneMsg) tea.Cmd {
	m.copying = false
	cancelStored(&m.copyProgress.Cancel)
	m.autoOpenLog()
	m.refreshTreeNow()
	if msg.rescanRoot == nil {
		return nil
	}
	return m.queueRescan(msg.rescanRoot, msg.changed)
}

// tickCopy advances the copy spinner only while bytes move, so a stalled
// transfer shows a frozen spinner.
func (m *Model) tickCopy() {
	if !m.copying {
		m.lastCopyBytes = 0
		return
	}
	m.copyProgress.SyncTotals()
	cur := m.copyProgress.Bytes.Load()
	if cur == m.lastCopyBytes {
		return
	}
	m.copySpinFrame = (m.copySpinFrame + 1) % len(spinnerFrames)
	m.lastCopyBytes = cur
}

// adjustCopyParallel retunes the running copy's worker count.
func (m *Model) adjustCopyParallel(delta int) {
	if !m.copying || m.copyProgress.Batched.Load() {
		return
	}
	next := min(max(m.copyParallel+delta, 1), m.parallelMax)
	if next == m.copyParallel {
		return
	}
	m.copyParallel = next
	m.copyProgress.Parallel.Store(int64(next))
	if s := m.copyProgress.Sem.Load(); s != nil {
		s.Resize(next)
	}
	transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("parallel=%d", next))
}

// copyNode hands the transfer engine a snapshot of what it needs and reports
// back with the rescan root and the changed paths.
func (m *Model) copyNode(node *model.TreeNode, leftToRight, mirror bool) tea.Cmd {
	var relPath string
	m.readTree(func(*model.TreeNode) { relPath = node.RelPath })
	src, dst := m.left, m.right
	if !leftToRight {
		src, dst = m.right, m.left
	}
	parallel := max(m.copyParallel, 1)
	req := transfer.Request{
		Src: src, Dst: dst, Scanner: m.scanner, Node: node, RelPath: relPath,
		LeftToRight: leftToRight, Mirror: mirror, Opts: *m.cmpOpts,
		Parallel: parallel, ParallelMax: max(m.parallelMax, parallel),
		Batch: m.batchTransfer, VerifyResume: m.verifyResume, Progress: m.copyProgress,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = transport.ContextWithFatalCancel(ctx, cancel)
	m.copyProgress.Reset(parallel, req.ParallelMax, leftToRight)
	m.copyProgress.Cancel.Store(&cancel)
	return func() tea.Msg {
		res := transfer.Copy(ctx, req)
		return copyDoneMsg{rescanRoot: res.RescanRoot, changed: res.Changed}
	}
}

// presence reads one node's compare presence under the read lock; the scanner
// rewrites it whenever the node's directory is re-listed.
func (m *Model) presence(node *model.TreeNode) model.Presence {
	var p model.Presence
	m.readTree(func(*model.TreeNode) { p = node.Compare.Presence })
	return p
}

// copyConfirmLines counts collisions and mirror deletes across the subtree, so
// it runs the free function of the same name under the read lock.
func (m *Model) copyConfirmLines(node *model.TreeNode, leftToRight bool) ([]string, bool) {
	var lines []string
	var ok bool
	m.readTree(func(*model.TreeNode) { lines, ok = copyConfirmLines(node, leftToRight) })
	return lines, ok
}

// copyConfirmLines builds the destructive-copy confirmation body, or reports
// false when the copy destroys nothing and needs no confirmation. Counts come
// from the in-memory tree: an unlisted dir reads as empty, so the totals are a
// lower bound until the copy itself lists the subtree — say so rather than
// quote a number the delete pass will exceed.
func copyConfirmLines(node *model.TreeNode, leftToRight bool) ([]string, bool) {
	side := "right"
	if !leftToRight {
		side = "left"
	}
	collisions := len(model.CollectTypeCollisions(node, leftToRight))
	var delFiles, delDirs int
	if node.IsDir {
		delFiles, delDirs = model.CountMirrorDeletes(node, leftToRight)
	}
	if collisions == 0 && delFiles == 0 && delDirs == 0 {
		return nil, false
	}
	name := node.Name
	if node.IsDir {
		name += "/"
	}
	lines := []string{"", name}
	if collisions > 0 {
		lines = append(lines, fmt.Sprintf("Will replace %d type conflicts (file↔dir) on %s", collisions, side))
	}
	if delFiles > 0 || delDirs > 0 {
		lines = append(lines, fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on "+side)
	}
	if model.UnlistedDir(node) != nil {
		lines = append(lines, "", "⚠ subtree not fully scanned - actual counts may be higher")
	}
	return lines, true
}
