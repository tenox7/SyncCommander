package model

import (
	"os"
	"testing"
	"time"
)

func linkEntry(name, target string, size int64, at time.Time) FileEntry {
	return FileEntry{RelPath: name, Name: name, Size: size, ModTime: at, Mode: os.ModeSymlink | 0o777, Link: LinkTo(target)}
}

// 2026-11-20: two links compare by their targets, as rsync does, whatever
// the compare options: their times cannot be set over sftp, and their size
// is only the target's length. A target the listing did not give is not
// known.
func TestLinksCompareByTarget(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	opts := CompareOpts{Size: true, ModTime: true, Mode: true}
	for _, tc := range []struct {
		what string
		l, r FileEntry
		want AttrStatus
	}{
		{"same target, other times", linkEntry("x", "a.txt", 5, at), linkEntry("x", "a.txt", 5, at.Add(time.Hour*30)), AttrEqual},
		{"a target of the same length", linkEntry("x", "a.txt", 5, at), linkEntry("x", "b.txt", 5, at), AttrDifferent},
		{"the right's target not given", linkEntry("x", "a.txt", 5, at), linkEntry("x", "", 5, at), AttrUnknown},
	} {
		d := &TreeNode{RelPath: "", IsDir: true, Listed: true}
		d.Children = MergeChildren(d, []FileEntry{tc.l}, []FileEntry{tc.r}, 1, opts, nil)
		if len(d.Children) != 1 || !d.Children[0].IsLink {
			t.Fatalf("%s: %d rows; want one link row", tc.what, len(d.Children))
		}
		n := d.Children[0]
		if got := NodeStatus(n, &opts); got != tc.want {
			t.Errorf("%s: NodeStatus %v, want %v", tc.what, got, tc.want)
		}
		var flat []Row
		n.Expanded = true
		flattenNode(n, 0, &opts, &flat)
		var row *AttrRow
		for _, r := range flat {
			if r.Attr != nil && r.Attr.Label == "link" {
				row = r.Attr
			}
		}
		if row == nil || row.Val[SideLeft] != "a.txt" || row.Status != tc.want {
			t.Errorf("%s: the link row is %+v; want the targets, status %v", tc.what, row, tc.want)
		}
	}
}

// A link is a kind of its own: a file and a pipe of its name are twins,
// sorted file, link, pipe. A copy plans for the link written (links set):
// a twin in its way is a collision; when the destination makes no links,
// the link is not written and its twin stays.
func TestLinkIsAKindOfItsOwn(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	opts := CompareOpts{Size: true, ModTime: true}
	root := NewRootNode()
	root.Children = MergeChildren(root,
		[]FileEntry{linkEntry("x", "t", 1, at)},
		[]FileEntry{{RelPath: "x", Name: "x", Size: 4, ModTime: at}}, 1, opts, nil)
	if len(root.Children) != 2 || root.Children[0].IsLink || !root.Children[1].IsLink {
		t.Fatalf("rows %v; want the file, then the link", root.Children)
	}
	link := root.Children[1]
	if got := CollectTypeCollisions(link, true, true); len(got) != 1 || got[0] != root.Children[0] {
		t.Errorf("collisions of a link written: %v; want the file", got)
	}
	if got := CollectTypeCollisions(link, true, false); len(got) != 0 {
		t.Errorf("collisions of a link not written: %v; want none", got)
	}
	if got := CollectTypeCollisions(root, true, true); len(got) != 1 {
		t.Errorf("collisions of the top row, links written: %v; want the file", got)
	}
	if got := CollectTypeCollisions(root, true, false); len(got) != 0 {
		t.Errorf("collisions of the top row, no links: %v; want none", got)
	}
}
