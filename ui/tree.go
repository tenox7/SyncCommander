package ui

import (
	"time"

	"sc/model"
)

// readTree, mutateTree and changeView fence the UI goroutine's tree access
// against the scanner's: readTree for walks and rendering, mutateTree for the
// edits the UI makes to what the scanner reads too (a side swap, a folder
// marked unlisted), changeView for folds. None is reentrant — nothing inside
// fn may call back into a Scanner method that locks, refreshTree included.
func (m *Model) readTree(fn func(root *model.TreeNode)) { m.scanner.ReadTree(fn) }

func (m *Model) mutateTree(fn func(root *model.TreeNode)) { m.scanner.MutateTree(fn) }

// changeView runs fn, which folds or unfolds nodes and says whether it did.
// Expansion belongs to the UI goroutine (TreeNode.Expanded), so fn holds
// only the shared lock and the tree's revision stays: a fold changes what
// is shown, not what is counted, and costs no rollup walk over the whole
// tree (U21). The next refresh rebuilds the rows.
func (m *Model) changeView(fn func(root *model.TreeNode) bool) {
	m.readTree(func(root *model.TreeNode) {
		if fn(root) {
			m.viewRev++
		}
	})
}

// rowsSource is what the rows were built from: the tree's revision, the
// folds and the compare options. The same source gives the same rows.
type rowsSource struct {
	treeRev, viewRev uint64
	opts             model.CompareOpts
}

// refreshTree brings the rollups and the visible rows up to date. The
// rollup walk is O(whole tree) — at a million nodes it costs tens of
// milliseconds — so it runs only when what it counts changed (the tree or
// the compare options), and never more often than four times its own cost.
// That caps it at a quarter of the frame budget no matter how big the tree
// gets. The rows are rebuilt only when the tree, the folds or the options
// changed, so a tick with nothing new costs nothing (U21); the flatten
// itself only touches expanded nodes.
func (m *Model) refreshTree() {
	m.scanner.ReadTree(func(tree *model.TreeNode) {
		if tree == nil {
			m.cachedStats = nil
			return
		}
		rev, opts := m.scanner.Rev(), *m.cmpOpts
		if (rev != m.statsRev || opts != m.statsOpts) && time.Since(m.lastPropagate) >= m.propagateEvery {
			t0 := time.Now()
			s := model.PropagateStatus(tree, m.cmpOpts)
			m.lastPropagate = time.Now()
			m.propagateEvery = min(4*m.lastPropagate.Sub(t0), 2*time.Second)
			m.statsRev, m.statsOpts = rev, opts
			m.cachedStats = &s
		}
		from := rowsSource{rev, m.viewRev, opts}
		if m.rowsBuilt && from == m.rowsFrom {
			return
		}
		flat := model.FlattenTree(tree, m.cmpOpts, m.lastFlatLen)
		m.lastFlatLen = len(flat)
		m.left.panel.SetRows(flat)
		m.right.panel.SetRows(flat)
		m.rowsFrom, m.rowsBuilt = from, true
	})
}

// rollupsCurrent reports whether the last rollup counted the tree as it is
// now, with the compare options as they are. Called under the tree lock.
func (m *Model) rollupsCurrent() bool {
	return m.cachedStats != nil && m.statsRev == m.scanner.Rev() && m.statsOpts == *m.cmpOpts
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
	m.info.Open(*m.cachedStats, "L: "+m.left.backend.BasePath(), "R: "+m.right.backend.BasePath(), m.scanner.ChecksumAlgo(), cl, cr)
}

// swapSides moves each pane, backend, open argument and panel, to the
// other side, and the tree and the scanner with them.
func (m *Model) swapSides() {
	m.left, m.right = m.right, m.left
	m.left.panel.side, m.right.panel.side = model.SideLeft, model.SideRight
	m.left.panel.active = m.activeLeft
	m.right.panel.active = !m.activeLeft

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
	node.SwapTotals()

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
