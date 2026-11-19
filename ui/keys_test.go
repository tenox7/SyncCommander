package ui

import (
	"context"
	"regexp"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
)

// scannedModel opens two fake backends, scans them synchronously and fills
// the panels, so key presses act on a real tree.
func scannedModel(t *testing.T, left, right string) *Model {
	t.Helper()
	m := newTestModel(t, openBackendOrSkip(t, left), openBackendOrSkip(t, right))
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	return m
}

func press(m *Model, k string) tea.Cmd {
	_, cmd := m.Update(key(k))
	return cmd
}

func firstNode(n *model.TreeNode, want func(*model.TreeNode) bool) *model.TreeNode {
	if want(n) {
		return n
	}
	for _, c := range n.Children {
		if f := firstNode(c, want); f != nil {
			return f
		}
	}
	return nil
}

func TestTabSwitchesActivePanel(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	press(m, "tab")
	if m.activeLeft || m.left.panel.active || !m.right.panel.active || m.activePanel() != m.right.panel {
		t.Fatal("tab did not make the right panel active")
	}
}

func TestCursorKeysMoveBothPanels(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	press(m, "down")
	press(m, "down")
	press(m, "up")
	if m.left.panel.cursor != 1 || m.right.panel.cursor != 1 {
		t.Fatalf("cursors %d/%d after down, down, up; want 1/1", m.left.panel.cursor, m.right.panel.cursor)
	}
	press(m, "pgdown")
	p := m.left.panel
	if want := min(1+p.height, len(p.rows)-1); p.cursor != want || m.right.panel.cursor != want || m.right.panel.offset != p.offset {
		t.Fatalf("pgdown: cursors %d/%d offsets %d/%d, want cursor %d", p.cursor, m.right.panel.cursor, p.offset, m.right.panel.offset, want)
	}
}

// Home and End put the cursor on the first and the last row of both
// panels, and scroll so that the row shows.
func TestHomeAndEndJumpToTheFirstAndLastRow(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	press(m, "}") // expand all: more rows than one page
	p, o := m.left.panel, m.right.panel
	last := len(p.rows) - 1
	if last < p.height {
		t.Fatalf("%d rows fit on a page of %d: the test needs more", last+1, p.height)
	}
	press(m, "down")
	press(m, "end")
	if off := last - p.height + 1; p.cursor != last || o.cursor != last || p.offset != off || o.offset != off {
		t.Fatalf("end: cursors %d/%d offsets %d/%d, want cursor %d offset %d", p.cursor, o.cursor, p.offset, o.offset, last, off)
	}
	press(m, "home")
	if p.cursor != 0 || o.cursor != 0 || p.offset != 0 || o.offset != 0 {
		t.Fatalf("home: cursors %d/%d offsets %d/%d, want 0", p.cursor, o.cursor, p.offset, o.offset)
	}
}

func TestExpandAllAndCollapseAllKeys(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	top := 1 + len(m.scanner.Tree().Children)
	press(m, "}")
	if len(m.left.panel.rows) <= top {
		t.Fatalf("} left %d rows, want more than %d", len(m.left.panel.rows), top)
	}
	press(m, "{")
	if len(m.left.panel.rows) != top {
		t.Fatalf("{ left %d rows, want %d", len(m.left.panel.rows), top)
	}
}

func TestEnterExpandsDirAndLeftCollapsesIt(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	dir := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.IsDir && n.Depth == 1 })
	p := m.left.panel
	p.jumpTo(dir)
	before := len(p.rows)
	press(m, "enter")
	if !dir.Expanded || len(p.rows) <= before || p.CursorNode() != dir {
		t.Fatalf("enter: expanded=%v rows %d -> %d cursor on %v", dir.Expanded, before, len(p.rows), p.CursorNode())
	}
	press(m, "down")
	press(m, "left")
	if dir.Expanded || p.CursorNode() != dir || len(p.rows) != before {
		t.Fatalf("left from a child: expanded=%v cursor on %v rows %d", dir.Expanded, p.CursorNode(), len(p.rows))
	}
}

func TestModalKeysDoNotReachTheTree(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	press(m, "?")
	if !m.help.IsOpen() {
		t.Fatal("? did not open help")
	}
	press(m, "j")
	if m.left.panel.cursor != 0 {
		t.Fatal("j moved the cursor under an open dialog")
	}
	press(m, "esc")
	press(m, "j")
	if m.help.IsOpen() || m.left.panel.cursor != 1 {
		t.Fatalf("after esc: help open=%v cursor=%d", m.help.IsOpen(), m.left.panel.cursor)
	}
}

func TestSearchKeyJumpsToFirstMatch(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	tree := m.scanner.Tree()
	deep := firstNode(tree, func(n *model.TreeNode) bool { return n.Depth == 2 })
	re := regexp.MustCompile("^" + regexp.QuoteMeta(deep.Name) + "$")
	want := model.FindByName(tree, nil, re)
	press(m, "/")
	if !m.input.IsOpen() {
		t.Fatal("/ did not open the search input")
	}
	m.input.ed.set(re.String())
	press(m, "enter")
	if m.input.IsOpen() || m.left.panel.CursorNode() != want || !want.Parent.Expanded {
		t.Fatalf("search: input open=%v cursor on %v (want %s), parent expanded=%v", m.input.IsOpen(), m.left.panel.CursorNode(), want.RelPath, want.Parent.Expanded)
	}
}

func TestNextDiffKeyLandsOnADifference(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny?diff=0.3&drop=0.2")
	press(m, "n")
	node := m.left.panel.CursorNode()
	if node == nil || node.Depth == 0 {
		t.Fatalf("n did not move to a difference: %v", node)
	}
	if node.Compare.Presence == model.PresenceBoth && model.NodeStatus(node, m.cmpOpts) != model.AttrDifferent {
		t.Fatalf("n stopped on %s, which does not differ", node.RelPath)
	}
	if m.right.panel.cursor != m.left.panel.cursor {
		t.Fatal("panels out of sync after n")
	}
}

func TestSwapSidesMirrorsTreeAndPanels(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny?drop=0.3")
	leftOnly := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Compare.Presence == model.PresenceLeftOnly })
	if leftOnly == nil {
		t.Fatal("fixture has no left-only node")
	}
	oldLeft, oldRight := m.left.backend, m.right.backend
	leftTitle, rightTitle := m.left.panel.title, m.right.panel.title
	press(m, "s")
	if m.left.backend != oldRight || m.right.backend != oldLeft {
		t.Fatal("backends not swapped")
	}
	if m.left.panel.title != rightTitle || m.right.panel.title != leftTitle || m.left.panel.side != model.SideLeft || m.right.panel.side != model.SideRight {
		t.Fatalf("panels after swap: %q/%q sides %v/%v", m.left.panel.title, m.right.panel.title, m.left.panel.side, m.right.panel.side)
	}
	if leftOnly.Compare.Presence != model.PresenceRightOnly || leftOnly.Sides[model.SideLeft].Entry != nil {
		t.Fatalf("tree not mirrored: presence=%v", leftOnly.Compare.Presence)
	}
	runningOp(m, opCopy, nil)
	press(m, "s")
	if m.left.backend != oldRight {
		t.Fatal("swap ran while a copy was in progress")
	}
}

func TestRenameAndDeleteKeysOpenDialogs(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	file := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool {
		return !n.IsDir && n.Depth == 1 && n.Compare.Presence == model.PresenceBoth
	})
	m.left.panel.jumpTo(file)
	press(m, "e")
	if !m.input.IsOpen() || m.input.ed.String() != file.Name {
		t.Fatalf("e: input open=%v text=%q", m.input.IsOpen(), m.input.ed.String())
	}
	press(m, "esc")
	if m.input.IsOpen() {
		t.Fatal("esc left the rename input open")
	}
	press(m, "d")
	if !m.confirm.IsOpen() || m.pendingDelete == nil || m.pendingDelete.node != file {
		t.Fatalf("d: confirm open=%v pending=%v", m.confirm.IsOpen(), m.pendingDelete)
	}
	press(m, "esc") // a both-side file asks which side to delete; esc cancels
	if m.confirm.IsOpen() || m.pendingDelete != nil {
		t.Fatal("esc did not cancel the delete")
	}
}

func TestOpenDialogKeyShowsCurrentArgs(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	press(m, "y")
	if !m.openDlg.IsOpen() {
		t.Fatal("y did not open the location dialog")
	}
	if l, r := m.openDlg.Values(); l != m.left.arg || r != m.right.arg {
		t.Fatalf("dialog shows %q/%q, want %q/%q", l, r, m.left.arg, m.right.arg)
	}
	press(m, "esc")
	if m.openDlg.IsOpen() {
		t.Fatal("esc left the location dialog open")
	}
}

func TestReopenBackendsKeepsUnchangedSide(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	left := m.left.backend
	if _, errMsg := m.reopenBackends("fake://tiny?dirs=abc", m.right.arg); !strings.HasPrefix(errMsg, "left: ") || m.left.backend != left {
		t.Fatalf("bad left path: err=%q replaced=%v", errMsg, m.left.backend != left)
	}
	cmd, errMsg := m.reopenBackends(m.left.arg, "fake://small")
	if errMsg != "" || cmd == nil || !m.scanning() {
		t.Fatalf("reopen right: err=%q cmd=%v scanning=%v", errMsg, cmd != nil, m.scanning())
	}
	if m.left.backend != left || m.right.backend.BasePath() != "fake://small" || m.right.arg != "fake://small" || m.right.panel.title != "fake://small" {
		t.Fatalf("after reopen: left replaced=%v right=%q arg=%q title=%q", m.left.backend != left, m.right.backend.BasePath(), m.right.arg, m.right.panel.title)
	}
	m.cancelOps()
}

func TestCancelKeyStopsTransfersAndBlocksTreeKeys(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	var copyCancelled, deleteCancelled bool
	runningOp(m, opCopy, func() { copyCancelled = true })
	press(m, "j")
	if m.left.panel.cursor != 0 {
		t.Fatal("j moved the cursor during a copy")
	}
	press(m, "x")
	if !copyCancelled {
		t.Fatal("x did not cancel the copy")
	}
	m.write = nil
	runningOp(m, opDelete, func() { deleteCancelled = true })
	press(m, "X")
	if !deleteCancelled {
		t.Fatal("X did not cancel the delete")
	}
}

// runFirst runs the operation half of a startOp batch, leaving the tick alone.
func runFirst(cmd tea.Cmd) tea.Msg {
	msg := cmd()
	if b, ok := msg.(tea.BatchMsg); ok {
		return b[0]()
	}
	return msg
}

func TestQuitKeysCancelRunningOps(t *testing.T) {
	for _, k := range []string{"ctrl+c", "q"} {
		m := scannedModel(t, "fake://tiny", "fake://tiny")
		var got context.Context
		if _, ok := runFirst(m.startOp(opScan, func(ctx context.Context) { got = ctx })).(opDoneMsg); !ok || got.Err() != nil {
			t.Fatalf("%s: op did not run cleanly", k)
		}
		cmd := press(m, k)
		if _, ok := cmd().(tea.QuitMsg); !ok {
			t.Fatalf("%s did not quit", k)
		}
		if got.Err() != context.Canceled {
			t.Fatalf("%s left the scan context alive", k)
		}
	}
}
