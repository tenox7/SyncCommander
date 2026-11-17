package model

import (
	"math"
	"testing"
	"time"
	"unsafe"
)

// F13: 973,673 nodes took 830 MB, about 850 bytes for each file on both
// sides: a TreeNode was 384 bytes and a FileEntry 144, two of them per
// node. Eight compare results of eight bytes each, three time.Time of 24
// bytes each that are rarely read, a 48-byte rollup that only a folder
// needs, and padding between scattered flags. Now a node is 224 bytes and
// an entry 96. A field that makes them bigger again has to be weighed: at
// ten million files every byte is ten megabytes.
func TestTreeStructsStaySmall(t *testing.T) {
	for _, tc := range []struct {
		what      string
		size, max uintptr
	}{
		{"TreeNode", unsafe.Sizeof(TreeNode{}), 224},
		{"FileEntry", unsafe.Sizeof(FileEntry{}), 96},
		{"SideState", unsafe.Sizeof(SideState{}), 48},
		{"CompareResult", unsafe.Sizeof(CompareResult{}), 8},
	} {
		if tc.size > tc.max {
			t.Errorf("a %s is %d bytes, want at most %d", tc.what, tc.size, tc.max)
		}
	}
}

// F13: the access, change and birth times are kept in 8 bytes. They keep
// the instant to the nanosecond, the zero time stays no time, and the
// backends' local zone comes back.
func TestUnixNanoKeepsTheTime(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 34, 56, 789012345, time.UTC)
	for _, tc := range []struct {
		what string
		in   time.Time
		want time.Time
	}{
		{"a time", at, at},
		{"no time", time.Time{}, time.Time{}},
		{"1970 itself, a nanosecond later", time.Unix(0, 0), time.Unix(0, 1)},
		{"before 1970", time.Unix(-86400, 5), time.Unix(-86400, 5)},
		{"before 1678, at the start of the range", time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), time.Unix(0, math.MinInt64)},
		{"after 2262, at the end of the range", time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC), time.Unix(0, math.MaxInt64)},
	} {
		got := NanoOf(tc.in).Time()
		if !got.Equal(tc.want) || got.IsZero() != tc.want.IsZero() {
			t.Errorf("%s: %v came back as %v, want %v", tc.what, tc.in, got, tc.want)
		}
		if !got.IsZero() && got.Location() != time.Local {
			t.Errorf("%s: came back in %v, want the local zone", tc.what, got.Location())
		}
	}
	if NanoOf(at).IsZero() || !NanoOf(time.Time{}).IsZero() {
		t.Error("IsZero does not tell a time from no time")
	}
	if NanoOf(at) <= NanoOf(at.Add(-time.Nanosecond)) {
		t.Error("a later time is not a larger UnixNano")
	}
}

// F13: a folder's rollup lives in a block that only a folder holding
// something has; a file carries none, and its totals read as nothing.
func TestOnlyFoldersCarryTotals(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	root := NewRootNode()
	d := &TreeNode{RelPath: "d", Name: "d", IsDir: true, Depth: 1, Parent: root, Listed: true,
		Sides: [2]SideState{{Entry: &FileEntry{RelPath: "d", Name: "d", IsDir: true}}, {Entry: &FileEntry{RelPath: "d", Name: "d", IsDir: true}}}}
	empty := &TreeNode{RelPath: "e", Name: "e", IsDir: true, Depth: 1, Parent: root, Listed: true,
		Sides: [2]SideState{{Entry: &FileEntry{RelPath: "e", Name: "e", IsDir: true}}}}
	root.Children = []*TreeNode{d, empty}
	opts := CompareOpts{Size: true, ModTime: true}
	d.Children = MergeChildren(d,
		[]FileEntry{{RelPath: "d/a", Name: "a", Size: 10, ModTime: at}, {RelPath: "d/b", Name: "b", Size: 5, ModTime: at}},
		[]FileEntry{{RelPath: "d/a", Name: "a", Size: 10, ModTime: at}}, 2, opts, nil)
	PropagateStatus(root, &opts)
	if got := d.Totals(SideLeft); got != (SideTotals{Size: 15, Files: 2}) {
		t.Errorf("d holds %+v on the left, want 15 bytes in 2 files", got)
	}
	if got := d.Totals(SideRight); got != (SideTotals{Size: 10, Files: 1}) {
		t.Errorf("d holds %+v on the right, want 10 bytes in 1 file", got)
	}
	for _, n := range append([]*TreeNode{empty}, d.Children...) {
		if n.totals != nil {
			t.Errorf("%s carries a rollup block: %+v", n.RelPath, *n.totals)
		}
		if got := n.Totals(SideLeft); got != (SideTotals{}) {
			t.Errorf("%s holds %+v", n.RelPath, got)
		}
	}
	d.SwapTotals()
	if d.Totals(SideLeft).Files != 1 || d.Totals(SideRight).Files != 2 {
		t.Errorf("after the swap d holds %+v and %+v", d.Totals(SideLeft), d.Totals(SideRight))
	}
}

// F13: a cached checksum holds while its file has the size and the time it
// had when summed, kept in 8 bytes now; a file whose time moved, at the
// same size, is summed again.
func TestCachedChecksumNeedsTheSameTime(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 123456789, time.UTC)
	d := &TreeNode{RelPath: "d", Name: "d", IsDir: true, Depth: 1, Listed: true}
	entries := func(mtime time.Time) []FileEntry {
		return []FileEntry{{RelPath: "d/a", Name: "a", Size: 10, ModTime: mtime}}
	}
	opts := CompareOpts{Size: true, ModTime: true, Checksum: true}
	d.Children = MergeChildren(d, entries(at), entries(at), 2, opts, nil)
	n := d.Children[0]
	for s := range n.Sides {
		st := &n.Sides[s]
		st.Checksum, st.ChecksumSize, st.ChecksumModTime = "sum", 10, NanoOf(at)
	}
	d.Children = MergeChildren(d, entries(at), entries(at), 2, opts, nil)
	if n.Sides[SideLeft].Checksum != "sum" || n.Sides[SideRight].Checksum != "sum" {
		t.Fatal("a listing of the same file dropped its checksum")
	}
	d.Children = MergeChildren(d, entries(at.Add(time.Nanosecond)), entries(at), 2, opts, nil)
	if got := n.Sides[SideLeft].Checksum; got != "" {
		t.Errorf("the left file's time moved a nanosecond, and its checksum %q was kept", got)
	}
	if got := n.Sides[SideRight].Checksum; got != "sum" {
		t.Errorf("the right file did not change, and its checksum is %q", got)
	}
}
