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

// pendingCopyInfo is a copy waiting in its dialog.
type pendingCopyInfo struct {
	node        *model.TreeNode
	leftToRight bool
	title       string
	ask         copyAsk       // the question the dialog shows now
	plan        transfer.Plan // what the copy may destroy or write over: the dialog shows it, the answers trim it
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

// mirrorKeys are the answers when the destination folder holds entries the
// source does not; what names them (deletesWhat), on the side dst. Each
// answer says in capitals what it does to them: "c=copy, keep them
// d=copy and delete them" did not say which entries "them" were, or that
// d deletes them on the destination.
func mirrorKeys(what, dst string) string {
	return "c   = copy. KEEP " + what + " on the " + dst + ".\n" +
		"d   = copy. DELETE " + what + " on the " + dst + ".\n" +
		"Esc = cancel. Nothing changes."
}

// startCopy copies the cursor node in the given direction, asking first when
// the copy would destroy anything on the destination, would put an older or
// smaller file over a newer or larger one (F16), or would rewrite files on
// both sides that are not compared yet (F10). What is only on the
// destination is deleted only when the user picks that: a folder copy was
// always a mirror, with no way to copy and keep (F15). The questions show
// the copy's plan, read from the tree once, and the copy is held to it: it
// destroys or writes over nothing the dialog did not show (D6). When the
// source holds less than the destination, the first question shows a red
// warning: that is almost always the wrong folder or the wrong way.
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
	plan := transfer.PlanCopy(m.scanner, node, leftToRight, *m.cmpOpts)
	if len(plan.Conflicts) == 0 && len(plan.Deletes) == 0 && len(plan.Newer) == 0 && len(plan.Unknown) == 0 {
		return m.runCopy(node, leftToRight, plan)
	}
	m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: leftToRight, title: title, plan: plan}
	lines := m.copyConfirmLines(node, leftToRight, plan)
	if len(plan.Deletes) > 0 {
		m.pendingCopy.ask = askMirror
		_, dst := copySideNames(leftToRight)
		what, _ := deletesWhat(plan.Deletes)
		m.confirm.OpenKeys(title, lines, true, mirrorKeys(what, dst))
		m.confirm.Warn(lessDataWarning(plan, leftToRight))
		return nil
	}
	cmd := m.askNextCopy(lines)
	if m.confirm.IsOpen() {
		m.confirm.Warn(lessDataWarning(plan, leftToRight))
	}
	return cmd
}

// lessDataWarning is the red warning of a folder copy whose source holds
// less than its destination: fewer files, fewer folders or fewer bytes, or
// no files at all while the destination holds some (a disk not mounted
// leaves an empty folder). A mirror of it deletes what the source does not
// hold. It is nil when the source holds as much, and when a folder under it
// is not listed, as what it holds is not known then.
func lessDataWarning(plan transfer.Plan, leftToRight bool) []string {
	src, dst := plan.Src, plan.Dst
	if !src.Complete {
		return nil
	}
	srcName, dstName := copySideNames(leftToRight)
	check := "Make sure that the folder and the direction are correct."
	if src.Files == 0 && dst.Files > 0 {
		return []string{
			"⚠ WARNING: THE " + strings.ToUpper(srcName) + " HAS NO FILES.",
			"The " + dstName + " has " + holdsText(dst) + ".",
			"This is almost always a mistake.",
			check,
		}
	}
	var less []string
	if src.Files < dst.Files {
		less = append(less, "FEWER FILES")
	}
	if src.Dirs < dst.Dirs {
		less = append(less, "FEWER FOLDERS")
	}
	if src.Size < dst.Size {
		less = append(less, "LESS DATA")
	}
	if len(less) == 0 {
		return nil
	}
	what := less[0]
	if n := len(less); n > 1 {
		what = strings.Join(less[:n-1], ", ") + " AND " + less[n-1]
	}
	return []string{
		"⚠ WARNING: THE " + strings.ToUpper(srcName) + " HAS " + what + " THAN THE " + strings.ToUpper(dstName) + ".",
		fmt.Sprintf("%-6s %s", sideTitle(srcName)+":", holdsText(src)),
		fmt.Sprintf("%-6s %s", sideTitle(dstName)+":", holdsText(dst)),
		check,
	}
}

// sideTitle is a side's name at the start of a line: "Left", "Right".
func sideTitle(side string) string { return strings.ToUpper(side[:1]) + side[1:] }

// holdsText says what a side holds: "3 files, 1 folder, 1.2M", with "at
// least" while a folder in it is not listed.
func holdsText(h transfer.Holds) string {
	s := plural(h.Files, "file") + ", " + plural(h.Dirs, "folder") + ", " + model.FormatSize(h.Size)
	if !h.Complete {
		s = "at least " + s
	}
	return s
}

// askNextCopy shows the copy's next question below lines, or runs the copy
// when none is left. lines show what the copy destroys, for the first
// question; a later one shows the copied node's name.
func (m *Model) askNextCopy(lines []string) tea.Cmd {
	pc := m.pendingCopy
	switch {
	case len(pc.plan.Newer) > 0 && pc.ask < askNewer:
		pc.ask = askNewer
		m.askNewerCopies(lines)
	case len(pc.plan.Unknown) > 0 && pc.ask < askUnknown:
		pc.ask = askUnknown
		m.askUnknownCopies(lines)
	case len(lines) > 0:
		pc.ask = askReplace
		m.confirm.Open(pc.title, lines, true)
	default:
		return m.confirmCopy()
	}
	return nil
}

// answerCopy takes a key in a copy's dialog. Each question has its own
// keys; any other key does nothing. An answer trims the plan: c keeps what
// is only on the destination, s takes the newer or the not compared files
// out of it.
func (m *Model) answerCopy(k string) tea.Cmd {
	pc := m.pendingCopy
	switch k {
	case "esc", "n", "N", "q":
		m.cancelConfirm()
		return nil
	}
	switch k = strings.ToLower(k); {
	case pc.ask == askReplace && k == "y":
		return m.confirmCopy()
	case pc.ask == askMirror && (k == "c" || k == "d"):
		pc.plan.Mirror = k == "d"
		return m.askNextCopy(nil)
	case pc.ask == askNewer && k == "y":
		return m.askNextCopy(nil)
	case pc.ask == askNewer && k == "s" && pc.node.IsDir:
		pc.plan.Newer, pc.plan.KeepNewer = nil, true
		return m.askNextCopy(nil)
	case pc.ask == askUnknown && k == "y":
		return m.confirmCopy()
	case pc.ask == askUnknown && k == "s":
		pc.plan.Unknown = nil
		return m.confirmCopy()
	}
	return nil
}

// copyLabel is the copied node's name, and a warning when its subtree is
// not fully listed, for a question that follows another.
func (m *Model) copyLabel(node *model.TreeNode, leftToRight, unlisted bool) []string {
	lines := append([]string{""}, m.nodeLines(node, leftToRight)...)
	if unlisted {
		lines = append(lines, unlistedWarning...)
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
		lines = m.copyLabel(pc.node, pc.leftToRight, pc.plan.Unlisted)
	}
	side := "right"
	if !pc.leftToRight {
		side = "left"
	}
	newer := pc.plan.Newer
	if !pc.node.IsDir {
		lines = append(lines, "", fmt.Sprintf("It is %s on %s:", newer[0].Why, side), "the copy would put an older or smaller file over it.")
		m.confirm.OpenKeys(pc.title, lines, true, "y=overwrite it  Esc=cancel")
		return
	}
	lines = append(lines, "", fmt.Sprintf("%d files are newer or larger on %s:", len(newer), side))
	lines = appendNamed(lines, len(newer), func(i int) string { return newer[i].RelPath + "  (" + newer[i].Why + ")" })
	lines = append(lines, "The copy would put older or smaller files over them.")
	m.confirm.OpenKeys(pc.title, lines, true, "y=overwrite them  s=skip them  Esc=cancel")
}

// askUnknownCopies asks whether the copy rewrites the files on both sides
// not compared yet, below lines, or below the copied node's name when lines
// is empty.
func (m *Model) askUnknownCopies(lines []string) {
	pc := m.pendingCopy
	if len(lines) == 0 {
		lines = m.copyLabel(pc.node, pc.leftToRight, pc.plan.Unlisted)
	}
	why := "their checksums are not computed yet."
	if !m.cmpOpts.Checksum {
		why = "no compare option is switched on."
	}
	lines = append(lines, "", fmt.Sprintf("%d files on both sides are not compared yet:", len(pc.plan.Unknown)), why, "Copy them too?")
	m.confirm.OpenKeys(pc.title, lines, true, "y=copy them too  s=skip them  Esc=cancel")
}

// confirmCopy runs the copy the user just confirmed, held to the plan as
// the answers left it.
func (m *Model) confirmCopy() tea.Cmd {
	pc := m.pendingCopy
	m.pendingCopy = nil
	m.confirm.Close()
	return m.runCopy(pc.node, pc.leftToRight, pc.plan)
}

// runCopy starts the copy, held to plan.
func (m *Model) runCopy(node *model.TreeNode, leftToRight bool, plan transfer.Plan) tea.Cmd {
	m.lastCopyBytes, m.lastCopyTick, m.slotSpins, m.speed = 0, time.Time{}, nil, speedHistory{}
	m.copyNow, m.copyETA = speedNow{}, etaClock{}
	return m.copyNode(node, leftToRight, plan)
}

// slotSpin is one in-flight row's liveness: the frame only advances on ticks
// where that file's byte counter moved, so a stalled file shows a frozen glyph.
// now is the file's speed now.
type slotSpin struct {
	bytes int64
	frame int
	now   speedNow
}

// tickCopy samples the transferred bytes for the speeds and the chart, and
// advances the copy spinners only while bytes move, so a stalled transfer
// shows a frozen spinner. Resumed prefixes don't count as movement.
func (m *Model) tickCopy() { m.tickCopyAt(time.Now()) }

func (m *Model) tickCopyAt(now time.Time) {
	if !m.copying() {
		return
	}
	m.copyProgress.SyncTotals()
	m.tickSlotSpins(now)
	cur := m.copyProgress.Bytes.Load() - m.copyProgress.BaseBytes.Load()
	m.tickSpeedNow(now, cur)
	// The first tick counts from the copy's start: what moved before it (a
	// bandwidth limit's burst of a second) is in the average, and so in the
	// chart, or the average stood above the chart's max.
	last := m.lastCopyTick
	if last.IsZero() && m.copyProgress.Start.Load() > 0 {
		last = time.Unix(0, m.copyProgress.Start.Load())
	}
	if !last.IsZero() {
		m.speed.add(max(cur-m.lastCopyBytes, 0), now.Sub(last))
	}
	m.lastCopyTick = now
	if cur == m.lastCopyBytes {
		return
	}
	m.copySpinFrame = (m.copySpinFrame + 1) % len(spinnerFrames)
	m.lastCopyBytes = cur
}

// tickSpeedNow samples the copy's bytes for its speed now and its ETA. Both
// start with the files: listing the subtree is no copy time.
func (m *Model) tickSpeedNow(now time.Time, cur int64) {
	p := m.copyProgress
	if p.Listing.Load() || p.Start.Load() <= 0 {
		return
	}
	start := time.Unix(0, p.Start.Load())
	if !m.copyNow.start.Equal(start) {
		m.copyNow, m.copyETA = speedNow{start: start}, etaClock{}
	}
	moved, dt := m.copyNow.add(now, cur)
	m.copyETA.add(now, moved, dt, p.TotalBytes.Load()-p.Bytes.Load(), now.Sub(start), m.copyNow.rate > 0)
}

// tickSlotSpins samples each file at the now the total uses, so a file
// and the total look back over the same span.
func (m *Model) tickSlotSpins(now time.Time) {
	next := make(map[string]slotSpin, len(m.slotSpins))
	for _, s := range m.copyProgress.SnapshotSlots() {
		prev, seen := m.slotSpins[s.File]
		if !s.Active {
			// A free slot's row shows the file that ended in it as it was:
			// its spinner and its speed hold still.
			if seen {
				next[s.File] = prev
			}
			continue
		}
		if !seen || prev.bytes != s.Bytes {
			prev.frame = (prev.frame + 1) % len(spinnerFrames)
		}
		prev.bytes = s.Bytes
		if s.Start > 0 {
			if start := time.Unix(0, s.Start); !prev.now.start.Equal(start) {
				prev.now = speedNow{start: start}
			}
			prev.now.add(now, s.Bytes-s.BaseBytes)
		}
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

// followConnLimit lowers the parallel copies to what a server takes, as it
// showed when it turned a connection away for having too many
// (transport.ConnLimit): a copy over it waits for a connection of its own.
// A WARN line says so, no ERR: nothing failed. + raises it again, and the
// server shows again what it takes.
func (m *Model) followConnLimit() {
	if !m.copying() || m.copyProgress.Batched.Load() {
		return
	}
	n := 0
	for _, b := range []model.Backend{m.left.backend, m.right.backend} {
		if l := transport.ConnLimit(b); l > 0 && (n == 0 || l < n) {
			n = l
		}
	}
	if n == 0 || n >= m.copyParallel {
		return
	}
	m.setCopyParallel(n)
	m.copyProgress.SetParallel(n)
	transport.Log.Add("copy", transport.DirWarn, fmt.Sprintf("parallel=%d: the server takes no more connections at a time", n))
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
func (m *Model) copyNode(node *model.TreeNode, leftToRight bool, plan transfer.Plan) tea.Cmd {
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
		LeftToRight: leftToRight, Opts: *m.cmpOpts, Parallel: parallel,
		Batch: m.batchTransfer, VerifyResume: m.verifyResume, Progress: m.copyProgress,
		Plan: plan,
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

// isTopRow reports whether node is the top row, which stands for the base
// of each side. d, e and t act on a file or folder below it, never on the
// bases: a delete there was a recursive delete of both sides, which
// restic:// sent as a DELETE of the repository, a rename renamed a base,
// and a touch set a base folder's times (U11).
func (m *Model) isTopRow(node *model.TreeNode) bool {
	var top bool
	m.readTree(func(*model.TreeNode) { top = node.IsRoot() })
	return top
}

// nodeLines names node for a copy's dialog by its whole path, and the
// folder it is copied from and the one it is copied to, as the panel
// titles show them: "in L:" and "in R:" did not say which way the copy
// goes. It takes the read lock.
func (m *Model) nodeLines(node *model.TreeNode, leftToRight bool) []string {
	from, to := "  from L: "+m.left.backend.BasePath(), "  to R:   "+m.right.backend.BasePath()
	if !leftToRight {
		from, to = "  from R: "+m.right.backend.BasePath(), "  to L:   "+m.left.backend.BasePath()
	}
	var name string
	m.readTree(func(*model.TreeNode) { name = nodeName(node) })
	return []string{name, from, to}
}

// copySideNames names a copy's source and destination sides.
func copySideNames(leftToRight bool) (src, dst string) {
	if leftToRight {
		return "left", "right"
	}
	return "right", "left"
}

// deletesWhat names the entries only on the destination by the files and
// folders they hold: "this file", "these 3 files", "these 2 files and 1
// folder". one says it names one file or one folder.
func deletesWhat(deletes []transfer.Delete) (what string, one bool) {
	files, dirs := transfer.DeleteCounts(deletes)
	switch {
	case files == 1 && dirs == 0:
		return "this file", true
	case files == 0 && dirs == 1:
		return "this folder", true
	case dirs == 0:
		return "these " + plural(files, "file"), false
	case files == 0:
		return "these " + plural(dirs, "folder"), false
	}
	return "these " + plural(files, "file") + " and " + plural(dirs, "folder"), false
}

// plural counts n things: "1 file", "3 files".
func plural(n int, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return fmt.Sprintf("%d %ss", n, thing)
}

// maxConfirmPaths caps how many paths a confirm dialog lists by name.
const maxConfirmPaths = 5

// unlistedWarning is the dialog's note when a folder under the copied node
// is not listed yet. The copy lists it as it runs, and what it finds there
// that the dialog did not show, it does not delete or write over (D6).
var unlistedWarning = []string{"", "⚠ subtree not fully scanned: the copy may find more there.", "It deletes or writes over only what the dialog shows."}

// copyConfirmLines is the first question's body: the copied node, the
// destination entries of the other type the copy replaces, and the entries
// only on the destination, each named up to maxConfirmPaths, then the
// warning when a folder under the node is not listed yet. It is empty when
// the plan has neither conflicts nor deletes. A delete is said in
// capitals.
func (m *Model) copyConfirmLines(node *model.TreeNode, leftToRight bool, plan transfer.Plan) []string {
	if len(plan.Conflicts) == 0 && len(plan.Deletes) == 0 {
		return nil
	}
	src, dst := copySideNames(leftToRight)
	lines := append([]string{""}, m.nodeLines(node, leftToRight)...)
	if n := len(plan.Conflicts); n > 0 {
		these, them := "These items have", "them"
		if n == 1 {
			these, them = "This item has", "it"
		}
		lines = append(lines, "", these+" a different type on the "+src+".", "The copy DELETES "+them+" on the "+dst+":")
		lines = appendNamed(lines, n, func(i int) string { return describeConflict(plan.Conflicts[i]) })
	}
	if len(plan.Deletes) > 0 {
		what, one := deletesWhat(plan.Deletes)
		are := "are"
		if one {
			are = "is"
		}
		lines = append(lines, "", strings.ToUpper(what[:1])+what[1:]+" "+are+" only on the "+dst+":")
		lines = appendNamed(lines, len(plan.Deletes), func(i int) string { return describeDelete(plan.Deletes[i]) })
	}
	if plan.Unlisted {
		lines = append(lines, unlistedWarning...)
	}
	return lines
}

// appendNamed appends, indented, the first maxConfirmPaths of n named
// items, and how many more there are.
func appendNamed(lines []string, n int, name func(i int) string) []string {
	for i := range min(n, maxConfirmPaths) {
		lines = append(lines, "  "+name(i))
	}
	if n > maxConfirmPaths {
		lines = append(lines, fmt.Sprintf("  ... and %d more", n-maxConfirmPaths))
	}
	return lines
}

// describeConflict names a destination entry a copy replaces with the
// other type.
func describeConflict(c transfer.Conflict) string {
	if !c.IsDir {
		return c.RelPath + "  (" + c.Kind + ")"
	}
	return c.RelPath + "/  (folder: " + describeTally(c.Tally) + ")"
}

// describeDelete names an entry only on the destination, which a mirror
// deletes.
func describeDelete(d transfer.Delete) string {
	if d.IsPart {
		return d.RelPath + "  (part file of a copy that stopped)"
	}
	if !d.IsDir {
		return d.RelPath
	}
	return d.RelPath + "/  (folder: " + describeTally(d.Tally) + ")"
}
