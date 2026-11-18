package model

import "testing"

// U23: two rescans that become one name every path either named.
func TestMergeChanged(t *testing.T) {
	a := &ChangedPaths{Paths: [2]map[string]bool{{"d/a": true}, {"d/a2": true}}, Dirs: [2][]string{{"batch"}, nil}}
	b := &ChangedPaths{Paths: [2]map[string]bool{{"e/b": true}, nil}, Dirs: [2][]string{nil, {"other"}}}
	if MergeChanged(nil, nil) != nil || MergeChanged(a, nil) != a || MergeChanged(nil, b) != b {
		t.Fatal("a merge with nothing is not the other one")
	}
	m := MergeChanged(a, b)
	for _, c := range []struct {
		side Side
		path string
		want bool
	}{
		{SideLeft, "d/a", true}, {SideRight, "d/a2", true}, {SideLeft, "e/b", true},
		{SideLeft, "batch/f", true}, {SideRight, "other/f", true},
		{SideRight, "d/a", false}, {SideLeft, "other/f", false},
	} {
		if got := m.has(c.side, c.path); got != c.want {
			t.Errorf("merged has(%d, %s) = %v, want %v", c.side, c.path, got, c.want)
		}
	}
	if !m.touchesSubtree("e") || !m.touchesSubtree("batch") || m.touchesSubtree("x") {
		t.Error("the merged paths' folders are not indexed")
	}
	if a.has(SideLeft, "e/b") || b.has(SideLeft, "d/a") || len(a.Paths[SideLeft]) != 1 {
		t.Error("the merge changed an input")
	}
}
