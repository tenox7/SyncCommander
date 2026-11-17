package ui

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"sc/model"
)

// foldTree is a/1.txt, a/2.txt, b/x.txt and c/deep/y.txt on both sides,
// all at one time; b/x.txt is longer on the right, the only difference.
func foldTree(t *testing.T) (left, right string) {
	t.Helper()
	root := t.TempDir()
	left, right = filepath.Join(root, "L"), filepath.Join(root, "R")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for rel, bodies := range map[string][2]string{
		"a/1.txt":      {"one", "one"},
		"a/2.txt":      {"two", "two"},
		"b/x.txt":      {"short", "a longer body"},
		"c/deep/y.txt": {"same", "same"},
	} {
		for i, side := range []string{left, right} {
			p := filepath.Join(side, filepath.FromSlash(rel))
			writeTestFile(t, p, bodies[i])
			if err := os.Chtimes(p, at, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	return left, right
}

// hasRow reports whether a row of relPath, not an attribute row, is in the
// panel.
func hasRow(p *Panel, relPath string) bool {
	for _, r := range p.rows {
		if r.Attr == nil && r.Node.RelPath == relPath {
			return true
		}
	}
	return false
}

// U21: a fold took the tree's write lock, which counts as a change to the
// tree, so the next refresh counted the whole tree again: 42 ms for Enter
// on one folder at a million files, and for n. A fold, an unfold, } and {,
// a jump to the next difference and a step out change what is shown, not
// what is counted: each shows its rows at once and counts nothing again.
func TestFoldsAreNotRecounted(t *testing.T) {
	left, right := foldTree(t)
	m := scannedModel(t, left, right)
	stats := m.cachedStats
	if stats == nil || stats.FilesDiff != 1 {
		t.Fatalf("after the scan the rollup holds %+v, want one different file", stats)
	}
	p := m.activePanel()
	cursorTo(t, m, "a", true)
	for _, step := range []struct {
		key        string
		show, hide []string
		cursor     string
	}{
		{key: "enter", show: []string{"a/1.txt", "a/2.txt"}},
		{key: "enter", hide: []string{"a/1.txt"}},
		{key: "}", show: []string{"a/1.txt", "b/x.txt", "c/deep/y.txt"}},
		{key: "{", show: []string{"a", "b", "c"}, hide: []string{"a/1.txt", "b/x.txt", "c/deep"}},
		{key: "n", show: []string{"b/x.txt"}, hide: []string{"a/1.txt"}, cursor: "b/x.txt"},
		{key: "left", show: []string{"b/x.txt"}, cursor: "b/x.txt"},
		{key: "left", hide: []string{"b/x.txt"}, cursor: "b"},
	} {
		// Nothing may hold a recount back but its own reason.
		m.propagateEvery = 0
		press(m, step.key)
		if m.cachedStats != stats {
			t.Errorf("%s counted the whole tree again", step.key)
		}
		for _, rel := range step.show {
			if !hasRow(p, rel) {
				t.Errorf("after %s no row shows %s", step.key, rel)
			}
		}
		for _, rel := range step.hide {
			if hasRow(p, rel) {
				t.Errorf("after %s a row still shows %s", step.key, rel)
			}
		}
		if n := p.CursorNode(); step.cursor != "" && (n == nil || n.RelPath != step.cursor) {
			t.Errorf("after %s the cursor is on %v, want %s", step.key, n, step.cursor)
		}
	}
}

// U21: every 100 ms tick rebuilt the rows, changed or not. Now a refresh
// keeps the rows while nothing they show has changed, and rebuilds them for
// a change to the tree or to the compare options. An option changed in the
// settings is counted at once; before, only the next fold counted it.
func TestRowsAreRebuiltOnlyWhenSomethingChanged(t *testing.T) {
	left, right := foldTree(t)
	m := scannedModel(t, left, right)
	p := m.activePanel()
	press(m, "}")
	rows := unsafe.SliceData(p.rows)
	m.refreshTree()
	m.refreshTreeNow()
	if unsafe.SliceData(p.rows) != rows {
		t.Error("a refresh with nothing changed built the rows again")
	}

	writeTestFile(t, filepath.Join(left, "a", "3.txt"), "three")
	var a *model.TreeNode
	m.readTree(func(root *model.TreeNode) {
		a = firstNode(root, func(n *model.TreeNode) bool { return n.RelPath == "a" })
	})
	m.scanner.RescanNode(context.Background(), a, *m.cmpOpts, nil)
	m.refreshTree()
	if !hasRow(p, "a/3.txt") {
		t.Error("a file a rescan found has no row")
	}

	cursorTo(t, m, "x.txt", false)
	press(m, "enter")
	mtimeRow := func() *model.AttrRow {
		for _, r := range p.rows {
			if r.Attr != nil && r.Node.RelPath == "b/x.txt" && r.Attr.Label == "mtime" {
				return r.Attr
			}
		}
		return nil
	}
	if r := mtimeRow(); r == nil || r.Inactive {
		t.Fatalf("b/x.txt shows its modify time as %+v, want a compared time row", r)
	}
	press(m, "=")
	if !m.settings.IsOpen() || m.settings.options[m.settings.cursor].Value != &m.cmpOpts.Size {
		t.Fatal("the settings do not open on the size option")
	}
	press(m, " ")
	if m.cmpOpts.Size {
		t.Fatal("space did not turn the size compare off")
	}
	if got := m.cachedStats; got.FilesDiff != 0 || got.FilesEqual != 4 {
		t.Errorf("with sizes not compared the rollup holds %d different and %d equal files, want 0 and 4", got.FilesDiff, got.FilesEqual)
	}
	press(m, "down")
	press(m, " ")
	press(m, "esc")
	if m.cmpOpts.ModTime {
		t.Fatal("space did not turn the modify time compare off")
	}
	if r := mtimeRow(); r == nil || !r.Inactive {
		t.Errorf("with times not compared b/x.txt shows its modify time as %+v, want a row marked not compared", r)
	}
}

// U21: a fold holds only the shared lock now, so the scanner must not read
// what the UI folds. A rescan that read the folds while the UI changed them
// would be a data race (go test -race).
func TestFoldWhileARescanRuns(t *testing.T) {
	left, right := foldTree(t)
	m := scannedModel(t, left, right)
	root, opts := m.scanner.Tree(), *m.cmpOpts
	for range 20 {
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.scanner.RescanNode(context.Background(), root, opts, nil)
		}()
		press(m, "}")
		press(m, "{")
		<-done
	}
}

// U21: n passes over the folders the rollup counts as all equal, but only
// while the rollup counts the tree as it is, with the options in use: a
// difference the rollup has not counted yet is still found.
func TestNextDiffUsesTheRollupOnlyWhileItIsCurrent(t *testing.T) {
	left, right := foldTree(t)
	m := scannedModel(t, left, right)
	p := m.activePanel()
	pressN := func(want string) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("n walked into a folder the rollup counts as all equal: %v", r)
			}
		}()
		press(m, "n")
		if n := p.CursorNode(); n == nil || n.RelPath != want {
			t.Errorf("n went to %v, want %s", n, want)
		}
	}
	var c *model.TreeNode
	m.readTree(func(root *model.TreeNode) {
		c = firstNode(root, func(n *model.TreeNode) bool { return n.RelPath == "c" })
	})

	// The rollup is current, and c holds only equal files: n after b/x.txt
	// passes over c without a look inside. Its rows are swapped for a nil
	// one, which panics when walked.
	cursorTo(t, m, "a", true)
	pressN("b/x.txt")
	kept := c.Children
	c.Children = []*model.TreeNode{nil}
	pressN("b/x.txt")
	c.Children = kept

	// Counted with sizes off, b/x.txt is equal and so is b. With sizes on
	// again and not counted yet, it differs.
	press(m, "=")
	press(m, " ")
	press(m, "esc")
	if m.cmpOpts.Size || m.cachedStats.FilesDiff != 0 {
		t.Fatalf("with sizes off the rollup holds %d different files, want 0", m.cachedStats.FilesDiff)
	}
	m.cmpOpts.Size = true
	cursorTo(t, m, "a", true)
	pressN("b/x.txt")

	// c/deep/y.txt changes, and a rescan finds it before a refresh counts it.
	writeTestFile(t, filepath.Join(right, "c", "deep", "y.txt"), "changed on the right")
	var deep *model.TreeNode
	m.readTree(func(root *model.TreeNode) {
		deep = firstNode(root, func(n *model.TreeNode) bool { return n.RelPath == "c/deep" })
	})
	m.scanner.RescanNode(context.Background(), deep, *m.cmpOpts, nil)
	pressN("c/deep/y.txt")
}
