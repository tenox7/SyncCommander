package model

import (
	"slices"
	"testing"
	"time"
)

// F10: with checksum compare on and the checksums not computed yet, every
// file on both sides compares "unknown", and a folder copy rewrote them all:
// 3 identical files, 3 overwritten. Now a copy leaves them out unless told
// to copy them, and CountUnknownCopies says how many there are, so the user
// can be asked.
func TestCopyLeavesOutFilesNotComparedYet(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	entries := func(xSize int64) []FileEntry {
		var es []FileEntry
		for _, name := range []string{"a", "b", "c"} {
			es = append(es, FileEntry{RelPath: "d/" + name, Name: name, Size: 10, ModTime: at})
		}
		return append(es, FileEntry{RelPath: "d/x", Name: "x", Size: xSize, ModTime: at})
	}
	root := NewRootNode()
	dir := &FileEntry{RelPath: "d", Name: "d", IsDir: true}
	d := &TreeNode{RelPath: "d", Name: "d", IsDir: true, Depth: 1, Parent: root, Listed: true, Sides: [2]SideState{{Entry: dir}, {Entry: dir}}}
	root.Children = []*TreeNode{d}
	off := CompareOpts{Size: true, ModTime: true, TimeGrace: true}
	on := off
	on.Checksum = true
	d.Children = MergeChildren(d, entries(10), entries(20), 2, on, nil)
	names := func(ns []*TreeNode) []string {
		var out []string
		for _, n := range ns {
			out = append(out, n.Name)
		}
		slices.Sort(out)
		return out
	}
	for _, tc := range []struct {
		what      string
		opts      CompareOpts
		unknown   bool
		want      []string
		notCompar int
	}{
		{"checksum compare off", off, false, []string{"x"}, 0},
		{"checksum compare on, checksums not computed", on, false, []string{"x"}, 3},
		{"checksum compare on, told to copy them", on, true, []string{"a", "b", "c", "x"}, 3},
	} {
		if got := names(CollectCopyFiles(d, &tc.opts, true, tc.unknown)); !slices.Equal(got, tc.want) {
			t.Errorf("%s: a folder copy writes %q, want %q", tc.what, got, tc.want)
		}
		if got := CountUnknownCopies(d, &tc.opts, true); got != tc.notCompar {
			t.Errorf("%s: CountUnknownCopies = %d, want %d", tc.what, got, tc.notCompar)
		}
	}
}
