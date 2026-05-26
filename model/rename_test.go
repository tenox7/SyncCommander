package model

import "testing"

func countByPresence(children []*TreeNode) (both, leftOnly, rightOnly int) {
	for _, c := range children {
		switch c.Compare.Presence {
		case PresenceBoth:
			both++
		case PresenceLeftOnly:
			leftOnly++
		case PresenceRightOnly:
			rightOnly++
		}
	}
	return
}

func findChild(children []*TreeNode, name string) *TreeNode {
	for _, c := range children {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func TestRenameRightMergesWithLeft(t *testing.T) {
	root := NewRootNode()
	left := []FileEntry{{RelPath: "foo", Name: "foo", Size: 1}}
	right := []FileEntry{{RelPath: "foo-bar", Name: "foo-bar", Size: 2}}
	root.Children = MergeChildren(root, left, right, 1, false, false, false)

	bar := findChild(root.Children, "foo-bar")
	if bar == nil {
		t.Fatal("setup: foo-bar node not found")
	}

	s := &Scanner{tree: root}
	s.RenameNode(bar, "foo", "foo", "foo-bar", false, false, false)

	both, leftOnly, rightOnly := countByPresence(root.Children)
	if len(root.Children) != 1 || both != 1 {
		t.Fatalf("want 1 merged PresenceBoth child, got %d children both=%d leftOnly=%d rightOnly=%d", len(root.Children), both, leftOnly, rightOnly)
	}
	n := root.Children[0]
	if n.Left == nil || n.Right == nil {
		t.Errorf("merged node missing a side: left=%v right=%v", n.Left != nil, n.Right != nil)
	}
	if n.Compare.Size != AttrDifferent {
		t.Errorf("want size AttrDifferent (1 vs 2), got %v", n.Compare.Size)
	}
}

func TestRenameLeftMergesWithRight(t *testing.T) {
	root := NewRootNode()
	left := []FileEntry{{RelPath: "baz", Name: "baz"}}
	right := []FileEntry{{RelPath: "foo", Name: "foo"}}
	root.Children = MergeChildren(root, left, right, 1, false, false, false)

	baz := findChild(root.Children, "baz")
	s := &Scanner{tree: root}
	s.RenameNode(baz, "foo", "foo", "baz", false, false, false)

	both, _, _ := countByPresence(root.Children)
	if len(root.Children) != 1 || both != 1 {
		t.Fatalf("want 1 merged PresenceBoth child, got %d (both=%d)", len(root.Children), both)
	}
}

func TestRenameDirMergesAndRelists(t *testing.T) {
	root := NewRootNode()
	left := []FileEntry{{RelPath: "d1", Name: "d1", IsDir: true}}
	right := []FileEntry{{RelPath: "d2", Name: "d2", IsDir: true}}
	root.Children = MergeChildren(root, left, right, 1, false, false, false)

	d2 := findChild(root.Children, "d2")
	d2.Listed = true
	d2.Expanded = true
	d2.Children = []*TreeNode{{Name: "inner", RelPath: "d2/inner", Parent: d2}}

	s := &Scanner{tree: root}
	s.RenameNode(d2, "d1", "d1", "d2", false, false, false)

	if len(root.Children) != 1 {
		t.Fatalf("want 1 merged dir, got %d", len(root.Children))
	}
	n := root.Children[0]
	if n.Compare.Presence != PresenceBoth {
		t.Errorf("want PresenceBoth dir, got %v", n.Compare.Presence)
	}
	if n.Listed || n.Children != nil {
		t.Errorf("merged dir must be unlisted for relist: listed=%v children=%d", n.Listed, len(n.Children))
	}
}

func TestRenameNoCollision(t *testing.T) {
	root := NewRootNode()
	left := []FileEntry{{RelPath: "a", Name: "a"}}
	right := []FileEntry{{RelPath: "a", Name: "a"}}
	root.Children = MergeChildren(root, left, right, 1, false, false, false)

	a := findChild(root.Children, "a")
	s := &Scanner{tree: root}
	s.RenameNode(a, "b", "b", "a", false, false, false)

	if len(root.Children) != 1 {
		t.Fatalf("want 1 child, got %d", len(root.Children))
	}
	if root.Children[0].Name != "b" || root.Children[0].Compare.Presence != PresenceBoth {
		t.Errorf("want renamed both node b, got name=%q presence=%v", root.Children[0].Name, root.Children[0].Compare.Presence)
	}
}

func TestRenameSameSideNoMerge(t *testing.T) {
	root := NewRootNode()
	var left []FileEntry
	right := []FileEntry{{RelPath: "foo", Name: "foo"}, {RelPath: "bar", Name: "bar"}}
	root.Children = MergeChildren(root, left, right, 1, false, false, false)

	bar := findChild(root.Children, "bar")
	s := &Scanner{tree: root}
	s.RenameNode(bar, "foo", "foo", "bar", false, false, false)

	if len(root.Children) != 2 {
		t.Fatalf("same-side clash must not merge: want 2 children, got %d", len(root.Children))
	}
}
