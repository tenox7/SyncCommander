package model

import "testing"

// After a recursive delete of a directory on one side, RefreshDir of the parent
// must leave the surviving node single-sided AND prune its whole subtree to that
// side — otherwise the deleted node blanks out but its children keep rendering
// as matched on both sides (stale right entries).
func TestRefreshDirAfterRightDeletePrunesSubtree(t *testing.T) {
	root := NewRootNode()

	dir := &TreeNode{
		RelPath: "cbc1 pcb", Name: "cbc1 pcb", IsDir: true, Depth: 1, Parent: root,
		Left:  &FileEntry{RelPath: "cbc1 pcb", Name: "cbc1 pcb", IsDir: true},
		Right: &FileEntry{RelPath: "cbc1 pcb", Name: "cbc1 pcb", IsDir: true},
		Listed: true, Expanded: true,
	}
	sub := &TreeNode{
		RelPath: "cbc1 pcb/gerbers", Name: "gerbers", IsDir: true, Depth: 2, Parent: dir,
		Left:  &FileEntry{RelPath: "cbc1 pcb/gerbers", Name: "gerbers", IsDir: true},
		Right: &FileEntry{RelPath: "cbc1 pcb/gerbers", Name: "gerbers", IsDir: true},
		Listed: true,
	}
	sub.Children = []*TreeNode{{
		RelPath: "cbc1 pcb/gerbers/g.gbr", Name: "g.gbr", Depth: 3, Parent: sub,
		Left:  &FileEntry{RelPath: "cbc1 pcb/gerbers/g.gbr", Name: "g.gbr", Size: 10},
		Right: &FileEntry{RelPath: "cbc1 pcb/gerbers/g.gbr", Name: "g.gbr", Size: 10},
	}}
	file := &TreeNode{
		RelPath: "cbc1 pcb/readme.md", Name: "readme.md", Depth: 2, Parent: dir,
		Left:  &FileEntry{RelPath: "cbc1 pcb/readme.md", Name: "readme.md", Size: 5},
		Right: &FileEntry{RelPath: "cbc1 pcb/readme.md", Name: "readme.md", Size: 5},
	}
	dir.Children = []*TreeNode{sub, file}
	root.Children = []*TreeNode{dir}

	s := &Scanner{tree: root}
	// Right side deleted: parent re-list returns the dir on the left only.
	left := []FileEntry{{RelPath: "cbc1 pcb", Name: "cbc1 pcb", IsDir: true}}
	s.RefreshDir("", left, nil, false, false, false)

	got := findChild(root.Children, "cbc1 pcb")
	if got == nil {
		t.Fatal("cbc1 pcb missing after refresh")
	}
	if got.Compare.Presence != PresenceLeftOnly {
		t.Fatalf("dir presence = %v, want PresenceLeftOnly", got.Compare.Presence)
	}
	var check func(n *TreeNode)
	check = func(n *TreeNode) {
		for _, c := range n.Children {
			if c.Right != nil {
				t.Errorf("%s still has a Right entry after right-side delete", c.RelPath)
			}
			if c.Compare.Presence != PresenceLeftOnly {
				t.Errorf("%s presence = %v, want PresenceLeftOnly", c.RelPath, c.Compare.Presence)
			}
			check(c)
		}
	}
	check(got)
	if len(got.Children) != 2 {
		t.Errorf("surviving left children dropped: got %d, want 2", len(got.Children))
	}
}

// A right-only descendant under a directory deleted on the right must vanish,
// not linger as a dangling right-only row.
func TestRefreshDirPruneDropsGoneSideOnlyDescendant(t *testing.T) {
	root := NewRootNode()
	dir := &TreeNode{
		RelPath: "d", Name: "d", IsDir: true, Depth: 1, Parent: root,
		Left:  &FileEntry{RelPath: "d", Name: "d", IsDir: true},
		Right: &FileEntry{RelPath: "d", Name: "d", IsDir: true},
		Listed: true,
	}
	dir.Children = []*TreeNode{
		{RelPath: "d/both.txt", Name: "both.txt", Depth: 2, Parent: dir,
			Left:  &FileEntry{RelPath: "d/both.txt", Name: "both.txt"},
			Right: &FileEntry{RelPath: "d/both.txt", Name: "both.txt"}},
		{RelPath: "d/rightonly.txt", Name: "rightonly.txt", Depth: 2, Parent: dir,
			Right: &FileEntry{RelPath: "d/rightonly.txt", Name: "rightonly.txt"}},
	}
	root.Children = []*TreeNode{dir}

	s := &Scanner{tree: root}
	s.RefreshDir("", []FileEntry{{RelPath: "d", Name: "d", IsDir: true}}, nil, false, false, false)

	got := findChild(root.Children, "d")
	if len(got.Children) != 1 || got.Children[0].Name != "both.txt" {
		names := []string{}
		for _, c := range got.Children {
			names = append(names, c.Name)
		}
		t.Fatalf("want only [both.txt] left, got %v", names)
	}
}
