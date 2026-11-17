package model

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

var diffTestTime = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func diffTestDir(name string) FileEntry {
	return FileEntry{RelPath: name, Name: name, IsDir: true, ModTime: diffTestTime}
}

func diffTestFile(rel string, size int64) FileEntry {
	return FileEntry{RelPath: rel, Name: rel[len(DirOf(rel))+1:], Size: size, ModTime: diffTestTime}
}

// noWalk reports a walk into a folder the search must not open: such a
// folder holds a nil child, which panics when walked.
func noWalk(t *testing.T, what string, find func() *TreeNode) (got *TreeNode) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s walked a folder it must pass over: %v", what, r)
		}
	}()
	return find()
}

// U21: n walked the tree from the top to the cursor on every press, so near
// the end of a million files it went over nearly all of them before it
// looked at what follows the cursor. Now the walk starts at the cursor.
func TestNextDiffStartsAtTheCursor(t *testing.T) {
	opts := CompareOpts{Size: true, ModTime: true}
	root := NewRootNode()
	both := []FileEntry{diffTestDir("a"), diffTestDir("b")}
	root.Children = MergeChildren(root, both, both, 1, opts, nil)
	a, b := root.Children[0], root.Children[1]
	if a.Name != "a" || b.Name != "b" {
		t.Fatalf("children in the order %s, %s", a.Name, b.Name)
	}
	a.Children = []*TreeNode{nil}
	b.Children = MergeChildren(b,
		[]FileEntry{diffTestFile("b/same", 1), diffTestFile("b/x", 1)},
		[]FileEntry{diffTestFile("b/same", 1), diffTestFile("b/x", 2)}, 2, opts, nil)
	same, x := b.Children[0], b.Children[1]

	for _, tc := range []struct{ after, want *TreeNode }{{b, x}, {same, x}, {x, nil}} {
		what := "the next difference after " + nodePath(tc.after)
		if got := noWalk(t, what, func() *TreeNode { return FindNextDiff(root, tc.after, &opts, false) }); got != tc.want {
			t.Errorf("%s is %s, want %s", what, nodePath(got), nodePath(tc.want))
		}
	}
}

// U21: with nothing different after the cursor, n still walked every row
// after it to find nothing: in two trees in step, the whole tree, 35 ms a
// press at a million files. A folder the current rollup counts as all
// equal is now passed over whole, also the rows after the cursor in it.
func TestNextDiffPassesOverEqualFolders(t *testing.T) {
	opts := CompareOpts{Size: true, ModTime: true}
	root := NewRootNode()
	dirs := []FileEntry{diffTestDir("a"), diffTestDir("b"), diffTestDir("c")}
	root.Children = MergeChildren(root, dirs, dirs, 1, opts, nil)
	root.Listed = true
	a, b, c := root.Children[0], root.Children[1], root.Children[2]
	for _, d := range []*TreeNode{a, c} {
		files := []FileEntry{diffTestFile(d.Name+"/1", 1), diffTestFile(d.Name+"/2", 2)}
		d.Children = MergeChildren(d, files, files, 2, opts, nil)
		d.Listed = true
	}
	b.Children = MergeChildren(b,
		[]FileEntry{diffTestFile("b/same", 1), diffTestFile("b/x", 1)},
		[]FileEntry{diffTestFile("b/same", 1), diffTestFile("b/x", 2)}, 2, opts, nil)
	b.Listed = true
	x := b.Children[1]
	PropagateStatus(root, &opts)
	if a.ChildStatus != AttrEqual || c.ChildStatus != AttrEqual || b.ChildStatus != AttrDifferent {
		t.Fatalf("the rollup counts a, b, c as %v, %v, %v", a.ChildStatus, b.ChildStatus, c.ChildStatus)
	}
	a1 := a.Children[0]
	a.Children, c.Children = []*TreeNode{a1, nil}, []*TreeNode{nil}

	for _, tc := range []struct{ after, want *TreeNode }{{nil, x}, {a, x}, {a1, x}, {x, nil}, {c, nil}} {
		what := "the next difference after " + nodePath(tc.after)
		if got := noWalk(t, what, func() *TreeNode { return FindNextDiff(root, tc.after, &opts, true) }); got != tc.want {
			t.Errorf("%s is %s, want %s", what, nodePath(got), nodePath(tc.want))
		}
	}
}

// U21: from every row of random trees, the walk from the cursor finds what
// the walk from the top found, with the rollup passing over equal folders
// or not; and from a row a rescan dropped, nothing. The trees hold folders
// not listed, on one side or both, and files that differ in size or time
// or are not compared yet.
func TestNextDiffFindsWhatTheWalkFromTheTopFound(t *testing.T) {
	rng := rand.New(rand.NewPCG(21, 21))
	optSets := []CompareOpts{{Size: true, ModTime: true}, {Size: true, Checksum: true}}
	passedOver := 0
	for round := range 1000 {
		opts := optSets[round%len(optSets)]
		root := randomDiffTree(rng, opts)
		PropagateStatus(root, &opts)
		all := allNodes(root)
		for _, n := range all {
			if n.IsDir && n.Compare.Presence == PresenceBoth && n.ChildStatus == AttrEqual && len(n.Children) > 0 {
				passedOver++
			}
		}
		for _, after := range append([]*TreeNode{nil}, all...) {
			want := nextDiffFromTheTop(root, after, &opts)
			for _, counted := range []bool{false, true} {
				if got := FindNextDiff(root, after, &opts, counted); got != want {
					t.Fatalf("round %d (rollup used: %v): the next difference after %s is %s, want %s", round, counted, nodePath(after), nodePath(got), nodePath(want))
				}
			}
		}
		var dirs []*TreeNode
		for _, n := range all {
			if n.IsDir && n != root && len(n.Children) > 0 {
				dirs = append(dirs, n)
			}
		}
		if len(dirs) == 0 {
			continue
		}
		gone := dirs[rng.IntN(len(dirs))]
		siblings := gone.Parent.Children
		i := slices.Index(siblings, gone)
		gone.Parent.Children = slices.Delete(slices.Clone(siblings), i, i+1)
		for _, after := range allNodes(gone) {
			if got := FindNextDiff(root, after, &opts, false); got != nil {
				t.Fatalf("round %d: from %s, which a rescan dropped, the next difference is %s, want none", round, nodePath(after), nodePath(got))
			}
		}
	}
	if passedOver < 100 {
		t.Errorf("only %d folders could be passed over: the trees do not test it", passedOver)
	}
}

// nextDiffFromTheTop is FindNextDiff as it was: a walk from the top that
// starts matching once it passes after.
func nextDiffFromTheTop(root, after *TreeNode, opts *CompareOpts) *TreeNode {
	started := after == nil
	var visit func(n *TreeNode) *TreeNode
	visit = func(n *TreeNode) *TreeNode {
		if started && isDiffLeaf(n, opts) {
			return n
		}
		if n == after {
			started = true
			if isDiffLeaf(n, opts) {
				return nil
			}
		}
		if n.IsDir {
			for _, child := range n.Children {
				if r := visit(child); r != nil {
					return r
				}
			}
		}
		return nil
	}
	return visit(root)
}

// randomDiffTree is up to four levels of folders and files. A tenth of them
// are on the left only, a tenth on the right only; of the files on both
// sides a tenth differ in size and a tenth in time. One folder in five is
// not listed.
func randomDiffTree(rng *rand.Rand, opts CompareOpts) *TreeNode {
	root := NewRootNode()
	var fill func(dir *TreeNode, depth int)
	fill = func(dir *TreeNode, depth int) {
		var left, right []FileEntry
		for i := range rng.IntN(5) {
			name := fmt.Sprintf("n%d", i)
			rel := name
			if dir.RelPath != "" {
				rel = dir.RelPath + "/" + name
			}
			e := FileEntry{RelPath: rel, Name: name, IsDir: depth < 4 && rng.IntN(2) == 0, Size: 1, ModTime: diffTestTime}
			onLeft, onRight := true, true
			switch dir.Compare.Presence {
			case PresenceLeftOnly:
				onRight = false
			case PresenceRightOnly:
				onLeft = false
			default:
				switch rng.IntN(10) {
				case 0:
					onRight = false
				case 1:
					onLeft = false
				}
			}
			if onLeft {
				left = append(left, e)
			}
			if onRight {
				if !e.IsDir {
					switch rng.IntN(10) {
					case 0:
						e.Size = 2
					case 1:
						e.ModTime = e.ModTime.Add(time.Hour)
					}
				}
				right = append(right, e)
			}
		}
		dir.Children = MergeChildren(dir, left, right, depth, opts, nil)
		dir.Listed = true
		for _, c := range dir.Children {
			if c.IsDir && rng.IntN(5) != 0 {
				fill(c, depth+1)
			}
		}
	}
	fill(root, 1)
	return root
}

func allNodes(n *TreeNode) []*TreeNode {
	out := []*TreeNode{n}
	for _, c := range n.Children {
		out = append(out, allNodes(c)...)
	}
	return out
}

func nodePath(n *TreeNode) string {
	switch {
	case n == nil:
		return "none"
	case n.RelPath == "":
		return "the top"
	}
	return n.RelPath
}
