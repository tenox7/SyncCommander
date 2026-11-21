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
	_, dst := model.CopySides(leftToRight)
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
		visit := func(n *model.TreeNode, notCompared bool) {
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
		for _, d := range model.CollectMirrorDeletes(node, leftToRight) {
			if e := d.Sides[dst].Entry; e != nil {
				p.Deletes = append(p.Deletes, Delete{RelPath: d.RelPath, IsDir: e.IsDir, Size: e.Size, ModTime: e.ModTime, Tally: tally(d)})
			}
		}
		p.Unlisted = model.UnlistedDir(node) != nil
	})
	return p
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
