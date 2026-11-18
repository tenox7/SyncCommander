package ui

import (
	"regexp"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
)

func (m *Model) activePanel() *Panel {
	if m.activeLeft {
		return m.left.panel
	}
	return m.right.panel
}

func (m *Model) inactivePanel() *Panel {
	if m.activeLeft {
		return m.right.panel
	}
	return m.left.panel
}

// syncPanels keeps the inactive panel on the same row as the active one.
func (m *Model) syncPanels() {
	src, dst := m.activePanel(), m.inactivePanel()
	dst.cursor, dst.offset = src.cursor, src.offset
}

func (m *Model) switchPanel() {
	m.activeLeft = !m.activeLeft
	m.left.panel.active = m.activeLeft
	m.right.panel.active = !m.activeLeft
}

// move applies a cursor motion to the active panel and mirrors it.
func (m *Model) move(fn func(*Panel)) {
	fn(m.activePanel())
	m.syncPanels()
}

// toggleCursor expands or collapses the cursor directory. An unlisted one is
// listed first, unless a scan already owns the tree.
func (m *Model) toggleCursor() tea.Cmd {
	p := m.activePanel()
	node := p.CursorNode()
	lazyList := false
	m.changeView(func(*model.TreeNode) bool {
		if node != nil && node.IsDir && !node.Listed && m.mayStart(opScan) {
			node.Expanded = true
			lazyList = true
			return true
		}
		return p.Toggle()
	})
	if lazyList {
		return m.listNode(node)
	}
	m.refreshTree()
	return nil
}

// collapseCursor folds the cursor directory, or jumps to and folds the
// enclosing one.
func (m *Model) collapseCursor() {
	collapsed := false
	m.changeView(func(*model.TreeNode) bool {
		collapsed = m.activePanel().Collapse()
		return collapsed
	})
	if !collapsed {
		return
	}
	m.syncPanels()
	m.refreshTree()
}

func (m *Model) setExpandedAll(expand bool) {
	m.changeView(func(tree *model.TreeNode) bool {
		if tree == nil {
			return false
		}
		model.SetExpandedAll(tree, expand)
		tree.Expanded = true
		return true
	})
	m.refreshTree()
}

// openSearch asks for a name to find, with the last search filled in.
// Enter goes to the next match after the cursor, and after the last match
// to the first again: each search went to the first match, so there was
// no next (U24). A pattern that is not a regular expression, or that no
// name matches, keeps the dialog open and says so: the dialog closed, and
// nothing told why the cursor did not move.
func (m *Model) openSearch() {
	m.input.Open("Search (regex):", m.lastSearch, func(query string) tea.Cmd {
		if query == "" {
			return nil
		}
		re, err := regexp.Compile(query)
		if err != nil {
			m.input.SetError(err.Error())
			return nil
		}
		m.lastSearch = query
		after := m.activePanel().CursorFile()
		found := m.jumpToMatch(func(tree *model.TreeNode) *model.TreeNode {
			if n := model.FindByName(tree, after, re); n != nil {
				return n
			}
			return model.FindByName(tree, nil, re)
		})
		if !found {
			m.input.SetError("no name matches")
		}
		return nil
	})
}

func (m *Model) jumpToNextDiff() {
	after := m.activePanel().CursorFile()
	m.jumpToMatch(func(tree *model.TreeNode) *model.TreeNode {
		target := model.FindNextDiff(tree, after, m.cmpOpts, m.rollupsCurrent())
		if target != nil {
			target.Expanded = true
		}
		return target
	})
}

// jumpToMatch runs find under the tree lock, then expands the way to the
// match and puts the cursor on it. It reports whether find found one.
func (m *Model) jumpToMatch(find func(tree *model.TreeNode) *model.TreeNode) bool {
	var target *model.TreeNode
	m.changeView(func(tree *model.TreeNode) bool {
		if tree == nil {
			return false
		}
		if target = find(tree); target == nil {
			return false
		}
		for n := target.Parent; n != nil; n = n.Parent {
			n.Expanded = true
		}
		return true
	})
	if target == nil {
		return false
	}
	m.refreshTree()
	m.activePanel().jumpTo(target)
	m.syncPanels()
	return true
}
