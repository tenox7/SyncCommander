package ui

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

type opKind int

const (
	opScan opKind = iota
	opChecksum
)

// scanOp is one running scanner operation. gen tells a late done message from
// a cancelled predecessor apart from the current op, so a stale one can never
// clear the flag that keeps the tick loop alive.
type scanOp struct {
	gen    uint64
	cancel context.CancelFunc
}

type opDoneMsg struct {
	kind opKind
	gen  uint64
}

// rescanReq is a rescan that has to wait for the running scan to finish.
type rescanReq struct {
	node    *model.TreeNode
	changed *model.ChangedPaths
}

// startOp runs one scanner operation in the background under its own
// cancellable context and reports back with the op's generation.
func (m *Model) startOp(kind opKind, run func(ctx context.Context)) tea.Cmd {
	m.opGen++
	ctx, cancel := context.WithCancel(context.Background())
	op := &scanOp{gen: m.opGen, cancel: cancel}
	if kind == opChecksum {
		m.cksum = op
	} else {
		m.scan = op
	}
	return tea.Batch(func() tea.Msg {
		run(ctx)
		return opDoneMsg{kind: kind, gen: op.gen}
	}, m.ensureTick())
}

// finishOp clears the op a done message belongs to and ignores messages from
// cancelled predecessors. A rescan queued behind the scan starts now.
func (m *Model) finishOp(msg opDoneMsg) tea.Cmd {
	slot := &m.scan
	if msg.kind == opChecksum {
		slot = &m.cksum
	}
	if *slot == nil || (*slot).gen != msg.gen {
		return nil
	}
	(*slot).cancel()
	*slot = nil
	m.refreshTreeNow()
	if msg.kind == opScan && m.pendingRescan != nil {
		r := m.pendingRescan
		m.pendingRescan = nil
		return m.rescanNode(r.node, r.changed)
	}
	return nil
}

// queueRescan starts a rescan now, or once the running scan finishes. Two
// pending rescans collapse into one of the whole tree.
func (m *Model) queueRescan(node *model.TreeNode, changed *model.ChangedPaths) tea.Cmd {
	if !m.scanning() {
		return m.rescanNode(node, changed)
	}
	if m.pendingRescan != nil {
		node, changed = m.scanner.Tree(), nil
	}
	m.pendingRescan = &rescanReq{node: node, changed: changed}
	return nil
}

func (m *Model) cancelOps() {
	for _, op := range []*scanOp{m.scan, m.cksum} {
		if op != nil {
			op.cancel()
		}
	}
}

func (m *Model) startScan() tea.Cmd {
	opts, scanner := *m.cmpOpts, m.scanner
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("dir scan", "/", func() { scanner.Scan(ctx, opts) })
	})
}

// checksumNode checksums one file or subtree; one at a time.
func (m *Model) checksumNode(node *model.TreeNode) tea.Cmd {
	if node == nil || m.checksumming() {
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
	if m.scanning() || (ok && row.Attr != nil) {
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
	if tree == nil || m.scanning() {
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

// refreshParent re-lists relPath's directory on both sides and merges it, so
// the tree shows what an operation really left there.
func refreshParent(scanner *model.Scanner, relPath string, opts model.CompareOpts) {
	dir := model.DirOf(relPath)
	if le, re, err := scanner.ListBothDir(context.Background(), dir); err == nil {
		scanner.RefreshDir(dir, le, re, opts)
	}
}
