package ui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// unlist makes the folder at rel look as after a shallow scan: not listed,
// nothing in it. A copy lists it only when it runs.
func unlist(t *testing.T, m *Model, rel string) {
	t.Helper()
	found := false
	m.mutateTree(func(root *model.TreeNode) {
		if n := firstNode(root, func(n *model.TreeNode) bool { return n.RelPath == rel && n.IsDir }); n != nil {
			n.Children, n.Listed, found = nil, false, true
		}
	})
	if !found {
		t.Fatalf("no folder %s in the tree", rel)
	}
}

// writeAt writes body to p and sets its times to at.
func writeAt(t *testing.T, p, body string, at time.Time) {
	t.Helper()
	writeTestFile(t, p, body)
	if err := os.Chtimes(p, at, at); err != nil {
		t.Fatal(err)
	}
}

// runCopyAnswer presses k, the answer that starts the copy, and runs it.
func runCopyAnswer(t *testing.T, m *Model, k string) {
	t.Helper()
	cmd := press(m, k)
	if cmd == nil {
		t.Fatalf("%s started no copy", k)
	}
	m.Update(runFirst(cmd))
}

// U5, D6: what a mirror deleted was read from the tree after the copy, not
// from the dialog. The copy lists a folder the scan had not listed, finds
// more there that is only on the destination, and deleted that too: a name
// no dialog had shown. Now the dialog names what the copy deletes, the copy
// deletes only that, and the next copy asks about the rest.
func TestMirrorDeletesOnlyWhatTheDialogShowed(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	writeAt(t, filepath.Join(left, "x", "sub", "a.txt"), "on both", at)
	writeAt(t, filepath.Join(right, "x", "sub", "a.txt"), "on both", at)
	writeTestFile(t, filepath.Join(right, "x", "gone.txt"), "only on the right")
	writeTestFile(t, filepath.Join(right, "x", "sub", "extra.txt"), "only on the right, in a folder not listed")
	m := scannedModel(t, left, right)
	unlist(t, m, "x/sub")
	cursorTo(t, m, "x", true)
	press(m, ">")
	body := strings.Join(m.confirm.lines, "\n")
	if !askingMirror(m) {
		t.Fatalf("> did not ask whether to delete:\n%s", body)
	}
	if !strings.Contains(body, "x/gone.txt") {
		t.Errorf("the dialog does not name what the copy deletes:\n%s", body)
	}
	mark := len(transport.Log.Lines())
	runCopyAnswer(t, m, "d")
	if _, err := os.Stat(filepath.Join(right, "x", "gone.txt")); !os.IsNotExist(err) {
		t.Errorf("x/gone.txt, which the dialog showed, is still there: %v", err)
	}
	if _, err := os.Stat(filepath.Join(right, "x", "sub", "extra.txt")); err != nil {
		t.Fatalf("x/sub/extra.txt is gone, and no dialog showed it: %v", err)
	}
	if !loggedSince(mark, "mirror delete x/sub/extra.txt skipped") {
		t.Error("the log does not say what the copy left for the next one")
	}
	settled(m)
	press(m, ">")
	if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "x/sub/extra.txt") {
		t.Errorf("the next copy does not ask about x/sub/extra.txt:\n%s", body)
	}
}

// F16, D6: y wrote over every newer or larger file the copy found, also one
// in a folder it listed only as it ran, which the dialog had not listed.
// Now y writes over the files the dialog listed; one found later fails, and
// the next copy asks.
func TestCopyOverwritesOnlyTheNewerFilesItListed(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	later := at.Add(10 * time.Minute)
	writeAt(t, filepath.Join(left, "d", "n1.txt"), "source", at)
	writeAt(t, filepath.Join(right, "d", "n1.txt"), "dest!!", later)
	writeAt(t, filepath.Join(left, "d", "sub", "n2.txt"), "source", at)
	writeAt(t, filepath.Join(right, "d", "sub", "n2.txt"), "dest!!", later)
	m := scannedModel(t, left, right)
	unlist(t, m, "d/sub")
	cursorTo(t, m, "d", true)
	press(m, ">")
	if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "1 files are newer or larger on right:") || !strings.Contains(body, "d/n1.txt  (newer)") {
		t.Fatalf("> did not ask about d/n1.txt alone:\n%s", body)
	}
	mark := len(transport.Log.Lines())
	runCopyAnswer(t, m, "y")
	if got := readTestFile(t, filepath.Join(right, "d", "n1.txt")); got != "source" {
		t.Errorf("d/n1.txt, which the dialog listed, holds %q", got)
	}
	if got := readTestFile(t, filepath.Join(right, "d", "sub", "n2.txt")); got != "dest!!" {
		t.Errorf("d/sub/n2.txt was written over, and no dialog listed it: it holds %q", got)
	}
	if !loggedSince(mark, "COPY d/sub/n2.txt: refused, it is newer on right") {
		t.Error("the log does not say d/sub/n2.txt was refused")
	}
}

// F10, D6: y to "copy them too" rewrote every file not compared yet that the
// copy found, also one in a folder it listed only as it ran. Now it rewrites
// the files the dialog counted; one found later is left out, as s leaves
// them.
func TestCopyRewritesOnlyTheUnknownFilesItCounted(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, dir := range []string{"d", filepath.Join("d", "sub")} {
		writeAt(t, filepath.Join(left, dir, "u.txt"), "left bytes", at)
		writeAt(t, filepath.Join(right, dir, "u.txt"), "right byte", at)
	}
	m := scannedModel(t, left, right)
	m.cmpOpts.Checksum = true // switched on, nothing summed yet
	unlist(t, m, "d/sub")
	cursorTo(t, m, "d", true)
	press(m, ">")
	if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "1 files on both sides are not compared yet") {
		t.Fatalf("> did not ask about d/u.txt alone:\n%s", body)
	}
	runCopyAnswer(t, m, "y")
	if got := readTestFile(t, filepath.Join(right, "d", "u.txt")); got != "left bytes" {
		t.Errorf("d/u.txt, which the dialog counted, holds %q", got)
	}
	if got := readTestFile(t, filepath.Join(right, "d", "sub", "u.txt")); got != "right byte" {
		t.Errorf("d/sub/u.txt was rewritten, and no dialog counted it: it holds %q", got)
	}
}

// D6: a mirror delete was checked against a fresh listing by its name
// alone. A file written to after the dialog showed it was deleted with its
// new content. Now the delete needs the entry the dialog showed: the same
// type, and for a file the same size and time.
func TestMirrorKeepsAFileThatChangedSinceTheDialog(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	writeAt(t, filepath.Join(left, "d", "a.txt"), "on both", at)
	writeAt(t, filepath.Join(right, "d", "a.txt"), "on both", at)
	writeAt(t, filepath.Join(right, "d", "gone.txt"), "old", at)
	m := scannedModel(t, left, right)
	cursorTo(t, m, "d", true)
	press(m, ">")
	if !askingMirror(m) {
		t.Fatalf("> did not ask whether to delete: keys %q", m.confirm.keys)
	}
	writeTestFile(t, filepath.Join(right, "d", "gone.txt"), "new work, written after the dialog")
	mark := len(transport.Log.Lines())
	runCopyAnswer(t, m, "d")
	if got := readTestFile(t, filepath.Join(right, "d", "gone.txt")); got != "new work, written after the dialog" {
		t.Fatalf("d/gone.txt holds %q", got)
	}
	if !loggedSince(mark, "mirror delete d/gone.txt skipped") {
		t.Error("the log does not say why d/gone.txt was kept")
	}
}
