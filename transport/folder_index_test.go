package transport

import (
	"slices"
	"strings"
	"testing"
)

// W-M8: a cut hands on the path and each one added under it, as
// strings.HasPrefix with a slash tells, and keeps nothing of them. a/q and
// a/q/r were never added: they lead to a/q/r/s all the same.
func TestFolderIndexCutsATreeAndNoMore(t *testing.T) {
	x := folderIndex{}
	added := []string{"a", "a/b", "a/b/c", "a//x", "a/", "a/q/r/s", "ab", "ab/c", "x", "x/a", "x/a/b"}
	for _, p := range added {
		x.add(p)
	}
	x.add("a/b") // again: no second link
	var got []string
	x.cut("a", func(p string) { got = append(got, p) })
	slices.Sort(got)
	var under []string
	for _, p := range got {
		if p == "a" || slices.Contains(added, p) {
			under = append(under, p)
		}
	}
	want := []string{"a", "a/", "a//x", "a/b", "a/b/c", "a/q/r/s"}
	if !slices.Equal(under, want) {
		t.Errorf("a cut of a handed on %q of the paths added; want %q", under, want)
	}
	for _, p := range got {
		if p != "a" && !strings.HasPrefix(p, "a/") {
			t.Errorf("a cut of a handed on %q", p)
		}
	}
	for p, kids := range x {
		if p == "a" || strings.HasPrefix(p, "a/") {
			t.Errorf("after the cut the index still holds %q, with %d in it", p, len(kids))
		}
	}
	if _, ok := x[""]["a"]; ok {
		t.Error("after the cut the base still leads to a")
	}

	got = nil
	x.cut("x/a", func(p string) { got = append(got, p) })
	slices.Sort(got)
	if want := []string{"x/a", "x/a/b"}; !slices.Equal(got, want) {
		t.Errorf("a cut of x/a handed on %q; want %q", got, want)
	}
	x.add("a/b/c") // a path added again after its cut is found again
	got = nil
	x.cut("a/b", func(p string) { got = append(got, p) })
	if !slices.Contains(got, "a/b/c") {
		t.Errorf("a cut of a/b after a/b/c came again handed on %q", got)
	}
	got = nil
	x.cut("", func(p string) { got = append(got, p) })
	slices.Sort(got)
	// a leads to a/b/c's place still: handed on for nothing.
	if want := []string{"", "a", "ab", "ab/c", "x"}; !slices.Equal(got, want) {
		t.Errorf("a cut of the base handed on %q; want %q", got, want)
	}
	if len(x) != 0 {
		t.Errorf("after a cut of the base the index holds %v", x)
	}
}

// The memo knows each folder it was told of until it forgets that one, or
// one above it. Its zero value is ready.
func TestDirMemoForgetsATreeAndNoMore(t *testing.T) {
	var m dirMemo
	if m.knows("a") {
		t.Fatal("an empty memo knows a")
	}
	m.forgetTree("a") // nothing to forget
	m.forget("a")
	for _, d := range []string{"a", "a/b/c", "a//x", "a/q/r/s", "ab", "ab/c", "x/a"} {
		m.know(d)
	}
	m.forget("ab") // alone: ab/c stays
	m.forgetTree("a")
	for d, want := range map[string]bool{"a": false, "a/b/c": false, "a//x": false, "a/q/r/s": false, "ab": false, "ab/c": true, "x/a": true} {
		if got := m.knows(d); got != want {
			t.Errorf("knows(%q) = %v, want %v", d, got, want)
		}
	}
	m.know("a/b/c")
	m.forgetTree("a/b")
	if m.knows("a/b/c") {
		t.Error("a/b/c, known again, outlived a forget of a/b")
	}
	m.forgetTree("")
	for _, d := range []string{"ab/c", "x/a"} {
		if m.knows(d) {
			t.Errorf("%s outlived a forget of the base", d)
		}
	}
}
