package model

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// stubBackend serves a fixed directory map and fails List for any path in fail.
type stubBackend struct {
	dirs map[string][]FileEntry
	fail map[string]bool
}

func (b *stubBackend) BasePath() string { return "/stub" }

func (b *stubBackend) List(_ context.Context, relDir string) ([]FileEntry, error) {
	if b.fail[relDir] {
		return nil, errors.New("connection reset")
	}
	return b.dirs[relDir], nil
}

func (b *stubBackend) Checksum(context.Context, string) (string, error) { return "", nil }
func (b *stubBackend) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}
func (b *stubBackend) CopyFrom(context.Context, string, io.Reader, os.FileMode) error { return nil }
func (b *stubBackend) Mkdir(context.Context, string, os.FileMode) error               { return nil }
func (b *stubBackend) Rename(context.Context, string, string) error                   { return nil }
func (b *stubBackend) Remove(context.Context, string) error                           { return nil }
func (b *stubBackend) RemoveAll(context.Context, string) error                        { return nil }
func (b *stubBackend) Open(context.Context, string) (io.ReadCloser, error)            { return nil, nil }

func dirEntry(rel, name string) FileEntry {
	return FileEntry{RelPath: rel, Name: name, IsDir: true}
}
func fileEntry(rel, name string, size int64) FileEntry {
	return FileEntry{RelPath: rel, Name: name, Size: size}
}

// A List failure on one side must not merge the other side's entries: doing so
// turns a transient network error into a tree full of false LeftOnly/RightOnly
// nodes, which drives wrong copy and mirror-delete sets.
func TestScanListFailureDoesNotMergePartialResult(t *testing.T) {
	left := &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("d", "d")},
		"d": {fileEntry("d/a.txt", "a.txt", 10)},
	}}
	right := &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("d", "d")},
		"d": {fileEntry("d/a.txt", "a.txt", 10)},
	}, fail: map[string]bool{"d": true}}

	s := NewScanner(left, right, 1, 1, true)
	s.Scan(context.Background(), CompareOpts{})

	d := findChild(s.Tree().Children, "d")
	if d == nil {
		t.Fatal("dir d missing")
	}
	if !d.ListErr {
		t.Error("ListErr not set on the directory whose listing failed")
	}
	if d.Listed {
		t.Error("Listed set despite the failed listing")
	}
	if len(d.Children) != 0 {
		t.Fatalf("children merged from the surviving side: %d", len(d.Children))
	}
}

// Copy and mirror-delete walk the in-memory tree, so an unlisted dir would read
// as empty. UnlistedDir is what the copy path checks before enumerating.
func TestUnlistedDirFindsShallowAndFailedDirs(t *testing.T) {
	root := NewRootNode()
	deep := &TreeNode{Name: "deep", RelPath: "d/deep", IsDir: true, Listed: false}
	d := &TreeNode{Name: "d", RelPath: "d", IsDir: true, Listed: true, Children: []*TreeNode{deep}}
	root.Children = []*TreeNode{d}

	if got := UnlistedDir(root); got != deep {
		t.Fatalf("UnlistedDir = %v, want the unlisted child", got)
	}
	deep.Listed = true
	if got := UnlistedDir(root); got != nil {
		t.Fatalf("UnlistedDir = %v, want nil once everything is listed", got)
	}
	deep.ListErr = true
	if got := UnlistedDir(root); got != deep {
		t.Fatalf("UnlistedDir = %v, want the dir whose listing failed", got)
	}
}

// A shallow scan leaves subtrees unlisted; EnsureSubtreeListed fills them in so
// the copy enumerates real files instead of an empty subtree.
func TestEnsureSubtreeListedFillsShallowTree(t *testing.T) {
	dirs := map[string][]FileEntry{
		"":      {dirEntry("d", "d")},
		"d":     {dirEntry("d/sub", "sub")},
		"d/sub": {fileEntry("d/sub/a.txt", "a.txt", 10)},
	}
	left := &stubBackend{dirs: dirs}
	right := &stubBackend{dirs: dirs}

	s := NewScanner(left, right, 1, 1, false) // shallow
	s.Scan(context.Background(), CompareOpts{})

	d := findChild(s.Tree().Children, "d")
	if UnlistedDir(d) == nil {
		t.Fatal("shallow scan left nothing unlisted; test needs a deeper tree")
	}
	opts := &CompareOpts{Size: true, ModTime: true}
	if files := CollectCopyFiles(d, opts, true); len(files) != 0 {
		t.Fatalf("unlisted subtree already yields %d files", len(files))
	}

	if !s.EnsureSubtreeListed(context.Background(), d, CompareOpts{}) {
		t.Fatal("EnsureSubtreeListed reported failure")
	}
	if n := UnlistedDir(d); n != nil {
		t.Fatalf("%s still unlisted", n.RelPath)
	}
	sub := findChild(d.Children, "sub")
	if sub == nil || len(sub.Children) != 1 {
		t.Fatalf("subtree not populated: %+v", sub)
	}
}

// A listing that keeps failing must be reported, so the copy path can abort
// rather than mirror-delete against a tree it could not enumerate.
func TestEnsureSubtreeListedReportsFailure(t *testing.T) {
	dirs := map[string][]FileEntry{
		"":      {dirEntry("d", "d")},
		"d":     {dirEntry("d/sub", "sub")},
		"d/sub": {fileEntry("d/sub/a.txt", "a.txt", 10)},
	}
	left := &stubBackend{dirs: dirs}
	right := &stubBackend{dirs: dirs}

	s := NewScanner(left, right, 1, 1, false)
	s.Scan(context.Background(), CompareOpts{})
	d := findChild(s.Tree().Children, "d")

	right.fail = map[string]bool{"d/sub": true}
	if s.EnsureSubtreeListed(context.Background(), d, CompareOpts{}) {
		t.Fatal("EnsureSubtreeListed reported success despite a failing listing")
	}
}

// A batch transfer rewrites every file in the subtree, not just the diff set,
// so cached checksums under that subtree must be dropped wholesale.
func TestChangedPathsDirsCoverSubtree(t *testing.T) {
	c := &ChangedPaths{Dirs: [2][]string{SideRight: {"d/sub"}}}
	if !c.has(SideRight, "d/sub/a.txt") {
		t.Error("file under a changed dir not reported as changed")
	}
	if !c.has(SideRight, "d/sub") {
		t.Error("the changed dir itself not reported as changed")
	}
	if c.has(SideRight, "d/other.txt") {
		t.Error("file outside the changed dir reported as changed")
	}
	if c.has(SideLeft, "d/sub/a.txt") {
		t.Error("right-side change leaked to the left side")
	}
	if !c.touchesSubtree("d") {
		t.Error("ancestor of a changed dir not reported as touched")
	}
	if !(&ChangedPaths{Dirs: [2][]string{SideLeft: {""}}}).has(SideLeft, "anything") {
		t.Error("root-scoped batch must cover the whole tree")
	}
}
