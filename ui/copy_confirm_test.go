package ui

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

func writeTestFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// F2: > on a file asks only about that file's own type conflict, and the
// dialog names the folder it will delete.
func TestCopyDialogNamesOnlyTheCopiedFilesConflict(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "a.txt"), "the file the user copies")
	writeTestFile(t, filepath.Join(left, "notes"), "a file on the left")
	writeTestFile(t, filepath.Join(right, "notes", "2024.txt"), "a folder on the right")

	m := scannedModel(t, left, right)
	m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "a.txt" }))
	press(m, ">")
	if m.confirm.IsOpen() || m.pendingCopy != nil {
		t.Errorf("> on a.txt opened a dialog: %q", m.confirm.lines)
	}

	m = scannedModel(t, left, right)
	m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "notes" && !n.IsDir }))
	press(m, ">")
	if !m.confirm.IsOpen() || m.pendingCopy == nil {
		t.Fatal("> on the file notes did not ask before deleting the folder notes/")
	}
	if !slices.Equal(m.pendingCopy.replace, []string{"notes"}) {
		t.Errorf("pending copy replaces %q, want [notes]", m.pendingCopy.replace)
	}
	if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "notes/  (folder: 1 files, 0 folders)") {
		t.Errorf("dialog does not name the folder it deletes:\n%s", body)
	}
}

// F10: > on a folder with files on both sides not compared yet (checksum
// compare on, checksums not computed) rewrote them all without a word. Now
// the dialog says how many there are and asks: y copies them too, s skips
// them and copies the rest.
func TestCopyDialogAsksAboutFilesNotComparedYet(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "d", "same.txt"), "left bytes")
	writeTestFile(t, filepath.Join(right, "d", "same.txt"), "right byte")
	writeTestFile(t, filepath.Join(left, "d", "new.txt"), "only on the left")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, p := range []string{filepath.Join(left, "d", "same.txt"), filepath.Join(right, "d", "same.txt")} {
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		key, want string
	}{{"s", "right byte"}, {"y", "left bytes"}} {
		os.Remove(filepath.Join(right, "d", "new.txt"))
		writeTestFile(t, filepath.Join(right, "d", "same.txt"), "right byte")
		if err := os.Chtimes(filepath.Join(right, "d", "same.txt"), at, at); err != nil {
			t.Fatal(err)
		}
		m := scannedModel(t, left, right)
		m.cmpOpts.Checksum = true // switched on, nothing summed yet
		m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "d" }))
		press(m, ">")
		if !m.confirm.IsOpen() || m.pendingCopy == nil {
			t.Fatal("> on a folder with a file not compared yet did not ask")
		}
		if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "1 files on both sides are not compared yet") {
			t.Errorf("the dialog does not give the count:\n%s", body)
		}
		if !strings.Contains(m.confirm.keys, "y=copy them too") || !strings.Contains(m.confirm.keys, "s=skip them") {
			t.Errorf("the dialog offers %q", m.confirm.keys)
		}
		cmd := press(m, tc.key)
		if cmd == nil {
			t.Fatalf("%s started no copy", tc.key)
		}
		if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
			t.Fatalf("%s: the copy did not run", tc.key)
		}
		if got, _ := os.ReadFile(filepath.Join(right, "d", "same.txt")); string(got) != tc.want {
			t.Errorf("after %s, d/same.txt holds %q, want %q", tc.key, got, tc.want)
		}
		if got, _ := os.ReadFile(filepath.Join(right, "d", "new.txt")); string(got) != "only on the left" {
			t.Errorf("after %s, d/new.txt holds %q: the rest was not copied", tc.key, got)
		}
	}
}
