package ui

import (
	"context"
	"fmt"
	"time"

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
	replace     []string // the type conflicts the dialog showed
	mirror      bool     // the dialog showed deletes: confirming it deletes them
	unknown     int      // files on both sides not compared yet, which the dialog asks about
}

// startCopy copies the cursor node in the given direction, asking first when
// the copy would destroy anything on the destination, or would rewrite files
// on both sides that are not compared yet (F10).
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
	lines, replace, destructive := m.copyConfirmLines(node, leftToRight)
	unknown := m.unknownCopies(node, leftToRight)
	if !destructive && unknown == 0 {
		return m.runCopy(node, leftToRight, false, nil, false)
	}
	m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: leftToRight, replace: replace, mirror: destructive, unknown: unknown}
	if unknown == 0 {
		m.confirm.Open(title, lines, true)
		return nil
	}
	if !destructive {
		lines = []string{"", m.nodeLabel(node)}
		if m.unlisted(node) {
			lines = append(lines, "", "⚠ subtree not fully scanned - actual counts may be higher")
		}
	}
	why := "their checksums are not computed yet."
	if !m.cmpOpts.Checksum {
		why = "no compare option is switched on."
	}
	lines = append(lines, "", fmt.Sprintf("%d files on both sides are not compared yet:", unknown), why, "Copy them too?")
	m.confirm.OpenKeys(title, lines, true, "y=copy them too  s=skip them  Esc=cancel")
	return nil
}

// confirmCopy runs the copy the user just confirmed: the deletes the dialog
// showed, and the files not compared yet when copyUnknown.
func (m *Model) confirmCopy(copyUnknown bool) tea.Cmd {
	pc := m.pendingCopy
	m.pendingCopy = nil
	m.confirm.Close()
	return m.runCopy(pc.node, pc.leftToRight, pc.mirror, pc.replace, copyUnknown)
}

// runCopy starts the copy. replace names the type conflicts the user agreed
// to replace; the copy leaves every other one alone. copyUnknown copies the
// files on both sides that are not compared yet.
func (m *Model) runCopy(node *model.TreeNode, leftToRight, mirror bool, replace []string, copyUnknown bool) tea.Cmd {
	m.copying = true
	m.lastCopyBytes, m.lastCopyTick, m.slotSpins, m.speed = 0, time.Time{}, nil, speedHistory{}
	return tea.Batch(m.copyNode(node, leftToRight, mirror, replace, copyUnknown), m.ensureTick())
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

// slotSpin is one in-flight row's liveness: the frame only advances on ticks
// where that file's byte counter moved, so a stalled file shows a frozen glyph.
type slotSpin struct {
	bytes int64
	frame int
}

// tickCopy samples the transferred bytes for the speed chart and advances the
// copy spinners only while bytes move, so a stalled transfer shows a frozen
// spinner. Resumed prefixes don't count as movement.
func (m *Model) tickCopy() {
	if !m.copying {
		return
	}
	m.copyProgress.SyncTotals()
	m.tickSlotSpins()
	cur := m.copyProgress.Bytes.Load() - m.copyProgress.BaseBytes.Load()
	now := time.Now()
	if !m.lastCopyTick.IsZero() {
		m.speed.add(max(cur-m.lastCopyBytes, 0), now.Sub(m.lastCopyTick))
	}
	m.lastCopyTick = now
	if cur == m.lastCopyBytes {
		return
	}
	m.copySpinFrame = (m.copySpinFrame + 1) % len(spinnerFrames)
	m.lastCopyBytes = cur
}

func (m *Model) tickSlotSpins() {
	next := make(map[string]slotSpin, len(m.slotSpins))
	for _, s := range m.copyProgress.SnapshotSlots() {
		prev, seen := m.slotSpins[s.File]
		if !seen || prev.bytes != s.Bytes {
			prev.frame = (prev.frame + 1) % len(spinnerFrames)
		}
		prev.bytes = s.Bytes
		next[s.File] = prev
	}
	m.slotSpins = next
}

// adjustCopyParallel is + and - during a copy: one more or one fewer file in
// flight. Raising starts a file at once; lowering waits, since the semaphore
// never stops a running file, it just does not refill the next one to finish.
func (m *Model) adjustCopyParallel(delta int) {
	if !m.copying || m.copyProgress.Batched.Load() {
		return
	}
	next := min(max(m.copyParallel+delta, 1), m.parallelMax)
	if next == m.copyParallel {
		return
	}
	m.setCopyParallel(next)
	m.copyProgress.SetParallel(next)
	transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("parallel=%d", next))
}

// setCopyParallel sets the copy worker count and sizes both sides' connection
// pools to match. It runs before the semaphore grows, so a new worker finds
// its connection allowed.
func (m *Model) setCopyParallel(n int) {
	m.copyParallel = n
	transport.SetParallel(m.left, n)
	transport.SetParallel(m.right, n)
}

// copyNode hands the transfer engine a snapshot of what it needs and reports
// back with the rescan root and the changed paths.
func (m *Model) copyNode(node *model.TreeNode, leftToRight, mirror bool, replace []string, copyUnknown bool) tea.Cmd {
	var relPath string
	m.readTree(func(*model.TreeNode) { relPath = node.RelPath })
	src, dst := m.left, m.right
	if !leftToRight {
		src, dst = m.right, m.left
	}
	parallel := max(m.copyParallel, 1)
	// The settings dialog may have moved the value since the pools were sized.
	m.setCopyParallel(parallel)
	req := transfer.Request{
		Src: src, Dst: dst, Scanner: m.scanner, Node: node, RelPath: relPath,
		LeftToRight: leftToRight, Mirror: mirror, Opts: *m.cmpOpts, Parallel: parallel,
		Batch: m.batchTransfer, VerifyResume: m.verifyResume, Progress: m.copyProgress,
		Replace: replace, CopyUnknown: copyUnknown,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = transport.ContextWithFatalCancel(ctx, cancel)
	m.copyProgress.Reset(parallel, leftToRight)
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

// unknownCopies counts, under the read lock, the files a copy of a folder
// would rewrite only because they are not compared yet. A single file the
// user copies is copied: that is what was asked.
func (m *Model) unknownCopies(node *model.TreeNode, leftToRight bool) int {
	n := 0
	m.readTree(func(*model.TreeNode) {
		if node.IsDir {
			n = model.CountUnknownCopies(node, m.cmpOpts, leftToRight)
		}
	})
	return n
}

// unlisted reports, under the read lock, a folder under node not listed yet.
func (m *Model) unlisted(node *model.TreeNode) bool {
	var u bool
	m.readTree(func(*model.TreeNode) { u = model.UnlistedDir(node) != nil })
	return u
}

// nodeLabel is node's name as the dialogs show it, under the read lock.
func (m *Model) nodeLabel(node *model.TreeNode) string {
	var name string
	m.readTree(func(*model.TreeNode) {
		name = node.Name
		if node.IsDir {
			name += "/"
		}
	})
	return name
}

// copyConfirmLines counts collisions and mirror deletes across the subtree, so
// it runs the free function of the same name under the read lock.
func (m *Model) copyConfirmLines(node *model.TreeNode, leftToRight bool) (lines, replace []string, ok bool) {
	m.readTree(func(*model.TreeNode) { lines, replace, ok = copyConfirmLines(node, leftToRight) })
	return lines, replace, ok
}

// maxConfirmPaths caps how many paths a confirm dialog lists by name.
const maxConfirmPaths = 5

// copyConfirmLines builds the destructive-copy confirmation body and the
// destination paths of the type conflicts it names, or reports false when the
// copy destroys nothing and needs no confirmation. Counts come from the
// in-memory tree: an unlisted dir reads as empty, so the totals are a lower
// bound until the copy itself lists the subtree — say so rather than quote a
// number the delete pass will exceed. A conflict the copy only finds then is
// not replaced: its path fails and the next copy asks.
func copyConfirmLines(node *model.TreeNode, leftToRight bool) (lines, replace []string, ok bool) {
	side := "right"
	if !leftToRight {
		side = "left"
	}
	collisions := model.CollectTypeCollisions(node, leftToRight)
	_, dst := model.CopySides(leftToRight)
	var delFiles, delDirs int
	if node.IsDir {
		delFiles, delDirs = model.CountMirrorDeletes(node, leftToRight)
	}
	if len(collisions) == 0 && delFiles == 0 && delDirs == 0 {
		return nil, nil, false
	}
	name := node.Name
	if node.IsDir {
		name += "/"
	}
	lines = []string{"", name}
	if len(collisions) > 0 {
		lines = append(lines, fmt.Sprintf("Will delete on %s, to put the other type there:", side))
		for i, c := range collisions {
			replace = append(replace, c.RelPath)
			if i < maxConfirmPaths {
				lines = append(lines, "  "+describeTwin(c, dst))
			}
		}
		if n := len(collisions) - maxConfirmPaths; n > 0 {
			lines = append(lines, fmt.Sprintf("  ... and %d more", n))
		}
	}
	if delFiles > 0 || delDirs > 0 {
		lines = append(lines, fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on "+side)
	}
	if model.UnlistedDir(node) != nil {
		lines = append(lines, "", "⚠ subtree not fully scanned - actual counts may be higher")
	}
	return lines, replace, true
}

// describeTwin names a destination entry a copy replaces with the other type.
func describeTwin(n *model.TreeNode, dst model.Side) string {
	if !n.IsDir {
		kind := "file"
		if e := n.Sides[dst].Entry; e != nil {
			kind = e.Kind()
		}
		return n.RelPath + "  (" + kind + ")"
	}
	return n.RelPath + "/  (folder: " + describeSubtree(n) + ")"
}
