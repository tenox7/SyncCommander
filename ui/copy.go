package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transfer"
	"sc/transport"
)

// copyDoneMsg is how a copy reports back: what to rescan, and the paths it
// changed.
type copyDoneMsg struct {
	opDoneMsg
	rescanRoot *model.TreeNode
	changed    *model.ChangedPaths
}

type pendingCopyInfo struct {
	node        *model.TreeNode
	leftToRight bool
	title       string
	ask         copyAsk            // the question the dialog shows now
	replace     []string           // the type conflicts the dialog showed
	mirror      bool               // the user chose to delete what is only on the destination
	newer       []newerFile        // the files newer or larger on the destination, the first few
	newerTotal  int                // how many there are
	newerRule   transfer.NewerRule // what the user chose for them
	unknown     int                // files on both sides not compared yet, which the dialog asks about
}

// copyAsk is the question a copy's dialog shows. The questions come in this
// order, each with its own keys; the first one shows what the copy destroys.
type copyAsk int

const (
	askReplace copyAsk = iota // type conflicts only: y copies, replacing them
	askMirror                 // c keeps what is only on the destination, d deletes it
	askNewer                  // y writes over the newer or larger files, s skips them
	askUnknown                // y copies the files not compared yet too, s skips them
)

// copyChoices are the user's answers that a copy runs with.
type copyChoices struct {
	mirror      bool               // delete what is only on the destination (F15)
	replace     []string           // the type conflicts to replace
	copyUnknown bool               // copy the files on both sides not compared yet (F10)
	newer       transfer.NewerRule // the files newer or larger on the destination (F16)
}

// newerFile is a file the copy would write over a newer or larger one.
type newerFile struct{ relPath, why string }

// copyMirrorKeys are the answers when the destination folder holds entries
// the source does not.
const copyMirrorKeys = "c=copy, keep them  d=copy and delete them  Esc=cancel"

// startCopy copies the cursor node in the given direction, asking first when
// the copy would destroy anything on the destination, would put an older or
// smaller file over a newer or larger one (F16), or would rewrite files on
// both sides that are not compared yet (F10). What is only on the
// destination is deleted only when the user picks that: a folder copy was
// always a mirror, with no way to copy and keep (F15).
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
	lines, replace, deletes, destructive := m.copyConfirmLines(node, leftToRight)
	newer, newerTotal := m.newerCopies(node, leftToRight)
	unknown := m.unknownCopies(node, leftToRight)
	if !destructive && newerTotal == 0 && unknown == 0 {
		return m.runCopy(node, leftToRight, copyChoices{})
	}
	m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: leftToRight, title: title, replace: replace,
		newer: newer, newerTotal: newerTotal, unknown: unknown}
	if deletes {
		m.pendingCopy.ask = askMirror
		m.confirm.OpenKeys(title, lines, true, copyMirrorKeys)
		return nil
	}
	return m.askNextCopy(lines)
}

// askNextCopy shows the copy's next question below lines, or runs the copy
// when none is left. lines show what the copy destroys, for the first
// question; a later one shows the copied node's name.
func (m *Model) askNextCopy(lines []string) tea.Cmd {
	pc := m.pendingCopy
	switch {
	case pc.newerTotal > 0 && pc.ask < askNewer:
		pc.ask = askNewer
		m.askNewerCopies(lines)
	case pc.unknown > 0 && pc.ask < askUnknown:
		pc.ask = askUnknown
		m.askUnknownCopies(lines)
	case len(lines) > 0:
		pc.ask = askReplace
		m.confirm.Open(pc.title, lines, true)
	default:
		return m.confirmCopy(false)
	}
	return nil
}

// answerCopy takes a key in a copy's dialog. Each question has its own
// keys; any other key does nothing.
func (m *Model) answerCopy(k string) tea.Cmd {
	pc := m.pendingCopy
	switch k {
	case "esc", "n", "N", "q":
		m.cancelConfirm()
		return nil
	}
	switch k = strings.ToLower(k); {
	case pc.ask == askReplace && k == "y":
		return m.confirmCopy(false)
	case pc.ask == askMirror && (k == "c" || k == "d"):
		pc.mirror = k == "d"
		return m.askNextCopy(nil)
	case pc.ask == askNewer && k == "y":
		pc.newerRule = transfer.NewerOverwrite
		return m.askNextCopy(nil)
	case pc.ask == askNewer && k == "s" && pc.node.IsDir:
		pc.newerRule = transfer.NewerKeep
		return m.askNextCopy(nil)
	case pc.ask == askUnknown && (k == "y" || k == "s"):
		return m.confirmCopy(k == "y")
	}
	return nil
}

// copyLabel is the copied node's name, and a warning when its subtree is
// not fully listed, for a question that follows another.
func (m *Model) copyLabel(node *model.TreeNode) []string {
	lines := []string{"", m.nodeLabel(node)}
	if m.unlisted(node) {
		lines = append(lines, "", "⚠ subtree not fully scanned - actual counts may be higher")
	}
	return lines
}

// askNewerCopies asks whether the copy writes over the files that are
// newer or larger on the destination, below lines, or below the copied
// node's name when lines is empty. A folder copy can skip them; a file
// copy is that one file.
func (m *Model) askNewerCopies(lines []string) {
	pc := m.pendingCopy
	if len(lines) == 0 {
		lines = m.copyLabel(pc.node)
	}
	side := "right"
	if !pc.leftToRight {
		side = "left"
	}
	if !pc.node.IsDir {
		lines = append(lines, "", fmt.Sprintf("It is %s on %s:", pc.newer[0].why, side), "the copy would put an older or smaller file over it.")
		m.confirm.OpenKeys(pc.title, lines, true, "y=overwrite it  Esc=cancel")
		return
	}
	lines = append(lines, "", fmt.Sprintf("%d files are newer or larger on %s:", pc.newerTotal, side))
	for _, f := range pc.newer {
		lines = append(lines, "  "+f.relPath+"  ("+f.why+")")
	}
	if n := pc.newerTotal - len(pc.newer); n > 0 {
		lines = append(lines, fmt.Sprintf("  ... and %d more", n))
	}
	lines = append(lines, "The copy would put older or smaller files over them.")
	m.confirm.OpenKeys(pc.title, lines, true, "y=overwrite them  s=skip them  Esc=cancel")
}

// askUnknownCopies asks whether the copy rewrites the files on both sides
// not compared yet, below lines, or below the copied node's name when lines
// is empty.
func (m *Model) askUnknownCopies(lines []string) {
	pc := m.pendingCopy
	if len(lines) == 0 {
		lines = m.copyLabel(pc.node)
	}
	why := "their checksums are not computed yet."
	if !m.cmpOpts.Checksum {
		why = "no compare option is switched on."
	}
	lines = append(lines, "", fmt.Sprintf("%d files on both sides are not compared yet:", pc.unknown), why, "Copy them too?")
	m.confirm.OpenKeys(pc.title, lines, true, "y=copy them too  s=skip them  Esc=cancel")
}

// confirmCopy runs the copy the user just confirmed: the type conflicts the
// dialog showed, the deletes and the newer files as the user chose, and the
// files not compared yet when copyUnknown.
func (m *Model) confirmCopy(copyUnknown bool) tea.Cmd {
	pc := m.pendingCopy
	m.pendingCopy = nil
	m.confirm.Close()
	return m.runCopy(pc.node, pc.leftToRight, copyChoices{mirror: pc.mirror, replace: pc.replace, copyUnknown: copyUnknown, newer: pc.newerRule})
}

// runCopy starts the copy with the user's answers.
func (m *Model) runCopy(node *model.TreeNode, leftToRight bool, c copyChoices) tea.Cmd {
	m.lastCopyBytes, m.lastCopyTick, m.slotSpins, m.speed = 0, time.Time{}, nil, speedHistory{}
	return m.copyNode(node, leftToRight, c)
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
	if !m.copying() {
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
	if !m.copying() || m.copyProgress.Batched.Load() {
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
	transport.SetParallel(m.left.backend, n)
	transport.SetParallel(m.right.backend, n)
}

// copyNode starts the copy: it hands the transfer engine a snapshot of what
// it needs, and reports back with the rescan root and the changed paths.
func (m *Model) copyNode(node *model.TreeNode, leftToRight bool, c copyChoices) tea.Cmd {
	if !m.mayStart(opCopy) {
		return nil
	}
	var relPath string
	m.readTree(func(*model.TreeNode) { relPath = node.RelPath })
	src, dst := m.left.backend, m.right.backend
	if !leftToRight {
		src, dst = m.right.backend, m.left.backend
	}
	parallel := max(m.copyParallel, 1)
	// The settings dialog may have moved the value since the pools were sized.
	m.setCopyParallel(parallel)
	req := transfer.Request{
		Src: src, Dst: dst, Scanner: m.scanner, Node: node, RelPath: relPath,
		LeftToRight: leftToRight, Mirror: c.mirror, Opts: *m.cmpOpts, Parallel: parallel,
		Batch: m.batchTransfer, VerifyResume: m.verifyResume, Progress: m.copyProgress,
		Replace: c.replace, CopyUnknown: c.copyUnknown, Newer: c.newer,
	}
	m.copyProgress.Reset(parallel, leftToRight)
	return m.start(opCopy, func(ctx context.Context, done opDoneMsg) doneMsg {
		res := transfer.Copy(ctx, req)
		return copyDoneMsg{opDoneMsg: done, rescanRoot: res.RescanRoot, changed: res.Changed}
	})
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

// newerCopies returns, under the read lock, the files a copy of node would
// write over a newer or larger file on the destination: the first few, and
// how many there are.
func (m *Model) newerCopies(node *model.TreeNode, leftToRight bool) (first []newerFile, total int) {
	m.readTree(func(*model.TreeNode) {
		visit := func(n *model.TreeNode, _ bool) {
			if why := model.OverwritesNewer(n, leftToRight, *m.cmpOpts); why != "" {
				total++
				if len(first) < maxConfirmPaths {
					first = append(first, newerFile{n.RelPath, why})
				}
			}
		}
		if node.IsDir {
			model.WalkCopyFiles(node, m.cmpOpts, leftToRight, visit)
		} else {
			visit(node, false)
		}
	})
	return first, total
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
func (m *Model) copyConfirmLines(node *model.TreeNode, leftToRight bool) (lines, replace []string, deletes, ok bool) {
	m.readTree(func(*model.TreeNode) { lines, replace, deletes, ok = copyConfirmLines(node, leftToRight) })
	return lines, replace, deletes, ok
}

// maxConfirmPaths caps how many paths a confirm dialog lists by name.
const maxConfirmPaths = 5

// copyConfirmLines builds the destructive-copy confirmation body and the
// destination paths of the type conflicts it names, and reports whether it
// shows entries only on the destination, which the user may keep or delete;
// or it reports false when the copy destroys nothing and needs no
// confirmation. Counts come from the
// in-memory tree: an unlisted dir reads as empty, so the totals are a lower
// bound until the copy itself lists the subtree — say so rather than quote a
// number the delete pass will exceed. A conflict the copy only finds then is
// not replaced: its path fails and the next copy asks.
func copyConfirmLines(node *model.TreeNode, leftToRight bool) (lines, replace []string, deletes, ok bool) {
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
	deletes = delFiles > 0 || delDirs > 0
	if len(collisions) == 0 && !deletes {
		return nil, nil, false, false
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
	if deletes {
		lines = append(lines, fmt.Sprintf("Only on %s: %d files, %d folders", side, delFiles, delDirs))
	}
	if model.UnlistedDir(node) != nil {
		lines = append(lines, "", "⚠ subtree not fully scanned - actual counts may be higher")
	}
	return lines, replace, deletes, true
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
