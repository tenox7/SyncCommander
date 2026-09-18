package ui

import (
	"regexp"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
)

func (m *Model) activePanel() *Panel {
	if m.activeLeft {
		return m.leftPanel
	}
	return m.rightPanel
}

func (m *Model) inactivePanel() *Panel {
	if m.activeLeft {
		return m.rightPanel
	}
	return m.leftPanel
}

// syncPanels keeps the inactive panel on the same row as the active one.
func (m *Model) syncPanels() {
	src, dst := m.activePanel(), m.inactivePanel()
	dst.cursor, dst.offset = src.cursor, src.offset
}

func (m *Model) switchPanel() {
	m.activeLeft = !m.activeLeft
	m.leftPanel.active = m.activeLeft
	m.rightPanel.active = !m.activeLeft
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
	m.mutateTree(func(*model.TreeNode) {
		if node != nil && node.IsDir && !node.Listed && !m.scanning() {
			node.Expanded = true
			lazyList = true
			return
		}
		p.Toggle()
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
	m.mutateTree(func(*model.TreeNode) { collapsed = m.activePanel().Collapse() })
	if !collapsed {
		return
	}
	m.syncPanels()
	m.refreshTree()
}

func (m *Model) setExpandedAll(expand bool) {
	m.mutateTree(func(tree *model.TreeNode) {
		if tree == nil {
			return
		}
		model.SetExpandedAll(tree, expand)
		tree.Expanded = true
	})
	m.refreshTree()
}

func (m *Model) openSearch() {
	m.input.Open("Search (regex):", "", func(query string) tea.Cmd {
		re, err := regexp.Compile(query)
		if query == "" || err != nil {
			return nil
		}
		m.jumpToMatch(func(tree *model.TreeNode) *model.TreeNode { return model.FindByName(tree, nil, re) })
		return nil
	})
}

func (m *Model) jumpToNextDiff() {
	after := m.activePanel().CursorFile()
	m.jumpToMatch(func(tree *model.TreeNode) *model.TreeNode {
		target := model.FindNextDiff(tree, after, m.cmpOpts)
		if target != nil {
			target.Expanded = true
		}
		return target
	})
}

// jumpToMatch runs find under the tree lock, then expands the way to the
// match and puts the cursor on it.
func (m *Model) jumpToMatch(find func(tree *model.TreeNode) *model.TreeNode) {
	var target *model.TreeNode
	m.mutateTree(func(tree *model.TreeNode) {
		if tree == nil {
			return
		}
		if target = find(tree); target == nil {
			return
		}
		for n := target.Parent; n != nil; n = n.Parent {
			n.Expanded = true
		}
	})
	if target == nil {
		return
	}
	m.refreshTree()
	m.activePanel().jumpTo(target)
	m.syncPanels()
}
