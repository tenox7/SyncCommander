package ui

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

// opKind is what an operation does. Scans, the checksum pass and the writes
// each have a lane of their own: one of each runs at a time (mayStart).
type opKind int

const (
	opScan     opKind = iota // a scan, a rescan or a listing
	opChecksum               // a checksum pass
	opCopy
	opDelete
	opRename
	opTouch
)

// writes says whether the operation changes files: a copy, a delete, a
// rename or a touch.
func (k opKind) writes() bool { return k >= opCopy }

// String is the operation as the log names it.
func (k opKind) String() string {
	return [...]string{"scan", "checksum", "copy", "delete", "rename", "touch"}[k]
}

// op is one running operation (D11): a scan, a checksum pass or a write.
// Each runs under its own context, which x, quit and a reopen cancel, and
// has a generation, which tells its done message from the late one of an
// operation that was dropped. Rename and touch were no operations at all:
// nothing showed them, x did not stop them, and a second write could start
// beside them (U14).
type op struct {
	kind    opKind
	gen     uint64
	cancel  context.CancelFunc
	stopped bool // the user stopped it: x again says nothing more
}

// opDoneMsg is how an operation reports back: which one it was. A scan's
// and a checksum pass's done message is just this; a copy's and a rename's
// embed it and add what they did.
type opDoneMsg struct {
	kind opKind
	gen  uint64
}

// doneMsg is any operation's done message.
type doneMsg interface{ header() opDoneMsg }

func (d opDoneMsg) header() opDoneMsg { return d }

// rescanReq is a rescan that has to wait for the running scan to finish.
type rescanReq struct {
	node    *model.TreeNode
	changed *model.ChangedPaths
}

// slot is where the running operation of kind's lane is kept.
func (m *Model) slot(kind opKind) **op {
	switch kind {
	case opScan:
		return &m.scan
	case opChecksum:
		return &m.cksum
	}
	return &m.write
}

// mayStart is the one rule for what runs beside what (D11). Each key asked
// a flag of its own: r asked "not scanning", c "not checksumming", and
// rename and touch asked nothing (U14). Now one scan, one checksum pass and
// one write run at a time, and a write holds the keys while it runs
// (handleKey), so the user starts nothing beside it. A scan or a checksum
// pass may run beside a write: one that ran when the write started, or a
// rescan that waited behind it.
func (m *Model) mayStart(kind opKind) bool { return *m.slot(kind) == nil }

// running reports whether an operation of kind runs.
func (m *Model) running(kind opKind) bool {
	o := *m.slot(kind)
	return o != nil && o.kind == kind
}

// startOp runs a scan or a checksum pass: an operation with nothing to
// report but that it ended.
func (m *Model) startOp(kind opKind, run func(ctx context.Context)) tea.Cmd {
	return m.start(kind, func(ctx context.Context, done opDoneMsg) doneMsg {
		run(ctx)
		return done
	})
}

// start runs an operation in the background under its own context, unless
// its lane is taken (mayStart). run returns the operation's done message,
// built on done. A write also stops at a fatal error, one that every next
// step would hit too (a read-only rsync module).
func (m *Model) start(kind opKind, run func(ctx context.Context, done opDoneMsg) doneMsg) tea.Cmd {
	if !m.mayStart(kind) {
		return nil
	}
	m.opGen++
	ctx, cancel := context.WithCancel(context.Background())
	if kind.writes() {
		ctx = transport.ContextWithFatalCancel(ctx, cancel)
	}
	done := opDoneMsg{kind: kind, gen: m.opGen}
	*m.slot(kind) = &op{kind: kind, gen: done.gen, cancel: cancel}
	return tea.Batch(func() tea.Msg { return run(ctx, done) }, m.ensureTick())
}

// finishOp ends the operation a done message belongs to and acts on what it
// reports. The message of an operation that was dropped (by a reopen) acts
// on nothing. Errors the operation logged as it ended open the log here:
// the tick loop stops with it, so it never sees them. A rescan queued
// behind the scan starts now.
func (m *Model) finishOp(msg doneMsg) tea.Cmd {
	h := msg.header()
	slot := m.slot(h.kind)
	if *slot == nil || (*slot).gen != h.gen {
		return nil
	}
	(*slot).cancel()
	*slot = nil
	m.refreshTreeNow()
	m.autoOpenLog()
	switch r := msg.(type) {
	case copyDoneMsg:
		if r.rescanRoot != nil {
			return m.queueRescan(r.rescanRoot, r.changed)
		}
	case renameDoneMsg:
		if r.rescan != nil {
			return m.queueRescan(r.rescan, nil)
		}
	default:
		if h.kind == opScan && m.pendingRescan != nil {
			p := m.pendingRescan
			m.pendingRescan = nil
			return m.rescanNode(p.node, p.changed)
		}
	}
	return nil
}

// queueRescan starts a rescan now, or once the running scan finishes. Two
// pending rescans collapse into one of the whole tree.
func (m *Model) queueRescan(node *model.TreeNode, changed *model.ChangedPaths) tea.Cmd {
	if m.mayStart(opScan) {
		return m.rescanNode(node, changed)
	}
	if m.pendingRescan != nil {
		node, changed = m.scanner.Tree(), nil
	}
	m.pendingRescan = &rescanReq{node: node, changed: changed}
	return nil
}

// cancelOps stops every operation and drops what waits for one: on quit and
// before a reopen. Their late done messages then act on nothing. A reopen
// kept the rescan queued behind the scan, which then ran on the new root's
// scanner with a folder of the old tree (U14).
func (m *Model) cancelOps() {
	for _, slot := range []**op{&m.scan, &m.cksum, &m.write} {
		if *slot != nil {
			(*slot).cancel()
			*slot = nil
		}
	}
	m.pendingRescan = nil
}

// stopScanOps is x while no write runs: it stops the running scan and
// checksum pass, which no key stopped (U10). What was listed or summed
// stays; a folder the scan was listing is marked as not listed.
func (m *Model) stopScanOps() {
	for _, o := range []*op{m.scan, m.cksum} {
		if o != nil && !o.stopped {
			o.stopped = true
			o.cancel()
			transport.Log.Add("scan", transport.DirIn, "user canceled "+o.kind.String())
		}
	}
	// A rescan queued behind the scan, after a copy or a rename, goes with
	// it: it would wait on the same server. Its folder is marked as a
	// stopped scan marks the folders it was listing, so its rows do not
	// pass for what is there, and a copy or a mirror lists it again first.
	if r := m.pendingRescan; m.scan != nil && r != nil {
		m.pendingRescan = nil
		m.mutateTree(func(*model.TreeNode) {
			n := r.node
			if n != nil && !n.IsDir {
				n = n.Parent
			}
			if n != nil {
				n.ListErr = true
			}
		})
	}
}

// stopRunning is x: it stops the running write, or the scan and the
// checksums when no write runs.
func (m *Model) stopRunning() {
	if m.writing() {
		m.stopWrite()
		return
	}
	m.stopScanOps()
}

// stopWrite is x while a write runs: it stops it, and its done message
// ends it. The log calls a stopped copy a transfer, as it always has.
func (m *Model) stopWrite() {
	o := m.write
	if o == nil || o.stopped {
		return
	}
	o.stopped = true
	what := o.kind.String()
	if o.kind == opCopy {
		what = "transfer"
	}
	transport.Log.Add(o.kind.String(), transport.DirIn, "user canceled "+what)
	o.cancel()
}

func (m *Model) startScan() tea.Cmd {
	opts, scanner := *m.cmpOpts, m.scanner
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("dir scan", "/", func() { scanner.Scan(ctx, opts) })
	})
}

// checksumNode checksums one file or subtree; one at a time.
func (m *Model) checksumNode(node *model.TreeNode) tea.Cmd {
	if node == nil {
		return nil
	}
	scanner := m.scanner
	return m.startOp(opChecksum, func(ctx context.Context) { scanner.ChecksumNode(ctx, node) })
}

func (m *Model) rescanNode(node *model.TreeNode, changed *model.ChangedPaths) tea.Cmd {
	opts, scanner, target := *m.cmpOpts, m.scanner, scanTargetLabel(node)
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("rescan", target, func() { scanner.RescanNode(ctx, node, opts, changed) })
	})
}

// rescanCursor rescans the cursor node and re-lists the root, so new
// top-level entries show up wherever the cursor sits.
func (m *Model) rescanCursor() tea.Cmd {
	row, ok := m.activePanel().CursorRow()
	if ok && row.Attr != nil {
		return nil
	}
	opts, scanner, node := *m.cmpOpts, m.scanner, row.Node
	target := scanTargetLabel(node)
	rescanCursor := node != nil && node.RelPath != ""
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("rescan", target, func() {
			if rescanCursor {
				scanner.RescanNode(ctx, node, opts, nil)
			}
			scanner.RefreshTopLevel(ctx, opts)
		})
	})
}

// deepRescanAll re-lists the whole tree from scratch.
func (m *Model) deepRescanAll() tea.Cmd {
	tree := m.scanner.Tree()
	if tree == nil {
		return nil
	}
	opts, scanner := *m.cmpOpts, m.scanner
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("deep scan", "/", func() { scanner.DeepRescanNode(ctx, tree, opts) })
	})
}

func (m *Model) listNode(node *model.TreeNode) tea.Cmd {
	opts, scanner, target := *m.cmpOpts, m.scanner, scanTargetLabel(node)
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("list", target, func() { scanner.ListNode(ctx, node, opts) })
	})
}

func scanTargetLabel(node *model.TreeNode) string {
	if node == nil || node.RelPath == "" {
		return "/"
	}
	return node.RelPath
}

func timeScan(op, target string, fn func()) {
	transport.Log.Add("scan", transport.DirOut, fmt.Sprintf("%s start: %s", op, target))
	t0 := time.Now()
	fn()
	transport.Log.Add("scan", transport.DirIn, fmt.Sprintf("%s done:  %s (%s)", op, target, time.Since(t0).Round(time.Millisecond)))
}

// refreshParent lists relPath's folder again, on the sides that hold it, so
// the tree shows what an operation really left there. It runs under the
// operation's context: x stops it with the operation (U10).
func refreshParent(ctx context.Context, scanner *model.Scanner, relPath string, opts model.CompareOpts) {
	scanner.RelistDir(ctx, model.DirOf(relPath), opts)
}
