package model

import "testing"

// A batch holds at most 1024 files and 1 GiB, or one bigger file: a
// request the connection cut off is made again whole (S19).
func TestChecksumBatchKeepsToItsLimits(t *testing.T) {
	const gib = int64(1) << 30
	files := func(sizes ...int64) []*TreeNode {
		var ns []*TreeNode
		for _, size := range sizes {
			n := &TreeNode{}
			n.Sides[SideRight].Entry = &FileEntry{Size: size}
			ns = append(ns, n)
		}
		return ns
	}
	many := make([]int64, 1500)
	s := &Scanner{}
	for _, tc := range []struct {
		name  string
		sizes []int64
		want  int
	}{
		{"many small files", many, 1024},
		{"a few", many[:3], 3},
		{"files up to 1 GiB", []int64{gib / 2, gib / 2, 1}, 2},
		{"one file over", []int64{2 * gib, 1}, 1},
		{"a file that makes it over", []int64{gib / 2, gib/2 + 1, 1}, 1},
	} {
		if got := s.batchLen(files(tc.sizes...), SideRight); got != tc.want {
			t.Errorf("%s: a batch of %d, want %d", tc.name, got, tc.want)
		}
	}
}
