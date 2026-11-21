package transfer

import (
	"time"

	"sc/model"
)

// Plan is what a copy of a node would destroy or write over on the
// destination, read from the tree in one pass by PlanCopy (D6). The dialog
// shows it, the user's answers trim it, and Copy is held to it: it does
// nothing of these kinds that is not in the plan. The dialog and the copy
// read them at different times from a tree that kept changing: a folder the
// copy listed only as it ran, or a rescan while the dialog was open, added
// deletes and overwrites nobody was shown.
type Plan struct {
	// Conflicts are destination entries of the other type in the way of a
	// copied entry (F2): the copy deletes them to write its own. Any other
	// such entry fails its path.
	Conflicts []Conflict
	// Newer are destination files newer or larger than their source (F16):
	// the copy writes over them. Any other such file fails, unless
	// KeepNewer leaves it as it is.
	Newer     []Newer
	KeepNewer bool
	// Unknown are files on both sides not compared yet (F10): the copy
	// rewrites them. Any other such file is left out.
	Unknown []string
	// Deletes are the entries only on the destination (F15). With Mirror,
	// the copy deletes them once every file landed, each only while it is
	// still as the plan has it.
	Deletes []Delete
	Mirror  bool
	// Unlisted says a folder under the node was not listed yet: the copy
	// lists it, and finds there what the plan does not have.
	Unlisted bool
	// Src and Dst are what a folder node holds on each side: the dialog
	// warns when the source holds less, or no files at all.
	Src, Dst Holds
}

// Holds is what a folder holds on one side: its files, its folders and the
// files' bytes. Complete is false while a folder in it is not listed, when
// it holds more than this.
type Holds struct {
	Files, Dirs int
	Size        int64
	Complete    bool
}

// Conflict is a destination entry of the other type in a copy's way.
type Conflict struct {
	RelPath string
	IsDir   bool
	Kind    string // what a file is: file, link, ...
	Tally          // what a folder holds
}

// Newer is a destination file a copy would write over though it is newer
// or larger; Why says which.
type Newer struct{ RelPath, Why string }

// Delete is an entry only on the destination, as the plan read it.
type Delete struct {
	RelPath string
	IsDir   bool
	IsPart  bool // what a cut-off copy left, of a file the copy does not write
	Size    int64
	ModTime time.Time
	Tally   // what a folder holds
}

// Tally counts what a folder holds; Complete is false while a folder in it
// is not listed.
type Tally struct {
	Files, Dirs int
	Complete    bool
}

// PlanCopy reads what a copy of node would destroy or write over, in one
// pass over the tree, as if the user agreed to all of it. The caller trims
// what the user does not want; Mirror is off until the user turns it on.
func PlanCopy(scanner *model.Scanner, node *model.TreeNode, leftToRight bool, opts model.CompareOpts) Plan {
	var p Plan
	src, dst := model.CopySides(leftToRight)
	scanner.ReadTree(func(*model.TreeNode) {
		// The plan is read on the UI's goroutine, which does not ask the
		// destination whether it makes links: that may connect. It plans
		// for a link written, and a copy to where links cannot be made
		// removes nothing for a link it skips.
		for _, c := range model.CollectTypeCollisions(node, leftToRight, true) {
			if e := c.Sides[dst].Entry; e != nil {
				p.Conflicts = append(p.Conflicts, Conflict{RelPath: c.RelPath, IsDir: e.IsDir, Kind: e.Kind(), Tally: tally(c)})
			}
		}
		// The part files the copy writes into, of the files it writes.
		parts := map[string]bool{}
		visit := func(n *model.TreeNode, notCompared bool) {
			if e := n.Sides[src].Entry; e != nil && !e.IsDir && !e.IsSpecial() {
				parts[model.PartPath(n.RelPath)] = true
			}
			if notCompared {
				p.Unknown = append(p.Unknown, n.RelPath)
			}
			if why := model.OverwritesNewer(n, leftToRight, opts); why != "" {
				p.Newer = append(p.Newer, Newer{RelPath: n.RelPath, Why: why})
			}
		}
		// A file the user copies is copied, compared or not.
		if !node.IsDir {
			visit(node, false)
			return
		}
		model.WalkCopyFiles(node, &opts, leftToRight, visit)
		p.Src, p.Dst = holdsOn(node, src), holdsOn(node, dst)
		for _, d := range model.CollectMirrorDeletes(node, leftToRight) {
			switch e := d.Sides[dst].Entry; {
			case e == nil:
			case e.IsPart() && parts[d.RelPath]:
				// What a cut-off copy of a file it writes left: the copy
				// writes into it and renames it over the file. The dialog
				// asked to keep it or to delete it, and the copy did
				// neither. A destination that takes no parts leaves it,
				// and the next copy asks.
			default:
				p.Deletes = append(p.Deletes, Delete{RelPath: d.RelPath, IsDir: e.IsDir, IsPart: e.IsPart(), Size: e.Size, ModTime: e.ModTime, Tally: tally(d)})
			}
		}
		p.Unlisted = model.UnlistedDir(node) != nil
	})
	return p
}

// partOfSourceFile reports whether d, only on the destination, is the part
// file of a file the source holds beside it: what a cut-off copy of that
// file left, which a copy of it writes into.
func partOfSourceFile(d *model.TreeNode, src, dst model.Side) bool {
	e := d.Sides[dst].Entry
	if e == nil || !e.IsPart() || d.Parent == nil {
		return false
	}
	for _, f := range d.Parent.Children {
		if s := f.Sides[src].Entry; s != nil && !s.IsDir && !s.IsSpecial() && model.PartName(f.Name) == d.Name {
			return true
		}
	}
	return false
}

// holdsOn counts the files, folders and bytes a folder node holds on side.
func holdsOn(node *model.TreeNode, side model.Side) Holds {
	h := Holds{Complete: true}
	var walk func(*model.TreeNode)
	walk = func(n *model.TreeNode) {
		if !n.Listed || n.ListErr {
			h.Complete = false
		}
		for _, c := range n.Children {
			switch e := c.Sides[side].Entry; {
			case e == nil, e.IsPart(): // what a cut-off copy left is no data
			case e.IsDir:
				h.Dirs++
				walk(c)
			default:
				h.Files++
				h.Size += e.Size
			}
		}
	}
	walk(node)
	return h
}

// tally counts what a folder node holds; nothing for a file.
func tally(n *model.TreeNode) Tally {
	if !n.IsDir {
		return Tally{Complete: true}
	}
	var t Tally
	t.Files, t.Dirs, t.Complete = model.CountDescendants(n)
	return t
}

// DeleteCounts is how many files and folders the deletes remove, counting
// what the folders hold.
func DeleteCounts(deletes []Delete) (files, dirs int) {
	for _, d := range deletes {
		if !d.IsDir {
			files++
			continue
		}
		files += d.Files
		dirs += d.Dirs + 1
	}
	return files, dirs
}

// pathSet indexes paths.
func pathSet[T any](items []T, path func(T) string) map[string]bool {
	set := make(map[string]bool, len(items))
	for _, it := range items {
		set[path(it)] = true
	}
	return set
}
