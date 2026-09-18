package ui

import (
	"time"

	"sc/model"
)

// readTree and mutateTree fence the UI goroutine's tree access against the
// scanner's: readTree for walks and rendering, mutateTree for the edits the UI
// makes itself (expand/collapse, side swap). Neither is reentrant — nothing
// inside fn may call back into a Scanner method that locks, refreshTree
// included.
func (m *Model) readTree(fn func(root *model.TreeNode)) { m.scanner.ReadTree(fn) }

func (m *Model) mutateTree(fn func(root *model.TreeNode)) { m.scanner.MutateTree(fn) }

// refreshTree rebuilds the visible rows. The rollup walk behind them is
// O(whole tree) — at a million nodes it costs tens of milliseconds — so it runs
// only when the scanner has actually changed something, and never more often
// than four times its own cost. That caps it at a quarter of the frame budget
// no matter how big the tree gets; the flatten itself only touches expanded
// nodes and stays cheap.
func (m *Model) refreshTree() {
	rev := m.scanner.Rev()
	m.scanner.ReadTree(func(tree *model.TreeNode) {
		if tree == nil {
			m.cachedStats = nil
			return
		}
		if rev != m.statsRev && time.Since(m.lastPropagate) >= m.propagateEvery {
			t0 := time.Now()
			s := model.PropagateStatus(tree, m.cmpOpts)
			m.lastPropagate = time.Now()
			m.propagateEvery = min(4*m.lastPropagate.Sub(t0), 2*time.Second)
			m.statsRev = rev
			m.cachedStats = &s
		}
		flat := model.FlattenTree(tree, m.cmpOpts, m.lastFlatLen)
		m.lastFlatLen = len(flat)
		m.leftPanel.SetRows(flat)
		m.rightPanel.SetRows(flat)
	})
}

// refreshTreeNow bypasses the throttle. Used when an operation finishes or the
// user acts on the tree: no further tick may be coming, so a skipped rollup
// would leave stale numbers on screen until the next keypress.
func (m *Model) refreshTreeNow() {
	m.propagateEvery = 0
	m.refreshTree()
}

// openInfo shows the whole-tree statistics from the latest rollup.
func (m *Model) openInfo() {
	m.refreshTreeNow()
	if m.cachedStats == nil {
		return
	}
	cl, cr := m.scanner.ChecksumInfo()
	m.info.Open(*m.cachedStats, "L: "+m.left.BasePath(), "R: "+m.right.BasePath(), m.scanner.ChecksumAlgo(), cl, cr)
}

func (m *Model) swapSides() {
	m.leftPanel, m.rightPanel = m.rightPanel, m.leftPanel
	m.leftPanel.side, m.rightPanel.side = model.SideLeft, model.SideRight
	m.leftPanel.active = m.activeLeft
	m.rightPanel.active = !m.activeLeft

	m.left, m.right = m.right, m.left

	m.scanner.SwapSides()

	swapped := false
	m.mutateTree(func(tree *model.TreeNode) {
		if tree == nil {
			return
		}
		swapTreeData(tree)
		swapped = true
	})
	if swapped {
		m.refreshTreeNow()
	}
}

// swapTreeData mirrors every per-side field. The swap key is refused while
// busy, so no checksum is in flight to carry across.
func swapTreeData(node *model.TreeNode) {
	node.Sides[0], node.Sides[1] = node.Sides[1], node.Sides[0]
	node.Totals[0], node.Totals[1] = node.Totals[1], node.Totals[0]

	switch node.Compare.Presence {
	case model.PresenceLeftOnly:
		node.Compare.Presence = model.PresenceRightOnly
	case model.PresenceRightOnly:
		node.Compare.Presence = model.PresenceLeftOnly
	}

	for _, child := range node.Children {
		swapTreeData(child)
	}
}
