package model

import (
	"slices"
	"testing"
	"time"
)

// F22: three rules said "equal". The counters took size and mtime only:
// a file with another checksum counted as equal, a file with another mtime
// counted as different with mtime compare off, a file not compared yet
// counted as equal. Next difference skipped a file whose checksum is known
// to differ, which the panel shows in red and a copy copies. Now NodeStatus
// decides, and each of them asks it.
func TestOneRuleDecidesEqual(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	type file struct {
		name     string
		later    time.Duration // the right side's mtime is this much later
		checksum AttrStatus
		want     AttrStatus
	}
	for _, tc := range []struct {
		what  string
		opts  CompareOpts
		files []file
	}{
		{"mtime compare off, checksum compare on", CompareOpts{Size: true, Checksum: true}, []file{
			{"mtime.txt", 65 * time.Minute, AttrEqual, AttrEqual}, // the counters said different
			{"pending.txt", 0, AttrUnknown, AttrUnknown},          // the counters said equal
		}},
		{"checksum compare off", CompareOpts{Size: true, ModTime: true}, []file{
			{"sum.txt", 0, AttrDifferent, AttrDifferent}, // NodeStatus and the counters said equal
			{"same.txt", 0, AttrUnknown, AttrEqual},
		}},
	} {
		var left, right []FileEntry
		for _, f := range tc.files {
			left = append(left, FileEntry{RelPath: "d/" + f.name, Name: f.name, Size: 10, ModTime: at})
			right = append(right, FileEntry{RelPath: "d/" + f.name, Name: f.name, Size: 10, ModTime: at.Add(f.later)})
		}
		root := NewRootNode()
		dir := &FileEntry{RelPath: "d", Name: "d", IsDir: true}
		d := &TreeNode{RelPath: "d", Name: "d", IsDir: true, Depth: 1, Parent: root, Listed: true, Sides: [2]SideState{{Entry: dir}, {Entry: dir}}}
		root.Children = []*TreeNode{d}
		d.Children = MergeChildren(d, left, right, 2, tc.opts, nil)
		var wantCounts [3]int // equal, different, not compared
		var wantDiff []string
		for _, f := range tc.files {
			n := firstChild(d, f.name)
			n.Compare.Checksum = f.checksum // as a checksum pass left it
			if got := NodeStatus(n, &tc.opts); got != f.want {
				t.Errorf("%s: NodeStatus(%s) = %v, want %v", tc.what, f.name, got, f.want)
			}
			switch f.want {
			case AttrEqual:
				wantCounts[0]++
			case AttrDifferent:
				wantCounts[1]++
				wantDiff = append(wantDiff, f.name)
			default:
				wantCounts[2]++
			}
		}
		st := PropagateStatus(root, &tc.opts)
		if got := [3]int{st.FilesEqual, st.FilesDiff, st.FilesUnknown}; got != wantCounts {
			t.Errorf("%s: equal, different, not compared = %v, want %v", tc.what, got, wantCounts)
		}
		var copied, found []string
		for _, n := range CollectCopyFiles(d, &tc.opts, true, false) {
			copied = append(copied, n.Name)
		}
		for n := FindNextDiff(root, nil, &tc.opts); n != nil; n = FindNextDiff(root, n, &tc.opts) {
			found = append(found, n.Name)
		}
		slices.Sort(copied)
		slices.Sort(found)
		slices.Sort(wantDiff)
		if !slices.Equal(copied, wantDiff) {
			t.Errorf("%s: a folder copy writes %q, want the different ones %q", tc.what, copied, wantDiff)
		}
		if !slices.Equal(found, wantDiff) {
			t.Errorf("%s: next difference finds %q, want %q", tc.what, found, wantDiff)
		}
	}
}

func firstChild(d *TreeNode, name string) *TreeNode {
	for _, c := range d.Children {
		if c.Name == name {
			return c
		}
	}
	panic("no " + name)
}
