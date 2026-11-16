package ui

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

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

// F15: > on a folder whose copy on the destination holds more was always a
// mirror. The dialog said "Will also delete", y copied and deleted, and Esc
// did nothing: there was no way to copy and keep. Now the dialog asks: c
// copies and keeps what is only on the destination, d copies and deletes
// it, Esc cancels. y alone does nothing.
func TestFolderCopyAsksWhetherToDelete(t *testing.T) {
	for _, tc := range []struct {
		name         string
		key          string
		copied, kept bool
	}{
		{"cancel", "esc", false, true},
		{"keep", "c", true, true},
		{"delete", "d", true, false},
	} {
		root := filepath.Join(t.TempDir(), tc.name)
		left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
		writeTestFile(t, filepath.Join(left, "d", "new.txt"), "only on the left")
		writeTestFile(t, filepath.Join(right, "d", "extra.txt"), "only on the right")
		m := scannedModel(t, left, right)
		m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "d" && n.IsDir }))
		press(m, ">")
		if !m.confirm.IsOpen() || m.pendingCopy == nil {
			t.Fatalf("%s: > on a folder with a file only on the right did not ask", tc.name)
		}
		if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "Only on right: 1 files, 0 folders") {
			t.Errorf("%s: the dialog does not say what is only on the right:\n%s", tc.name, body)
		}
		if m.confirm.keys != copyMirrorKeys {
			t.Fatalf("%s: the dialog offers %q", tc.name, m.confirm.keys)
		}
		if v := ansi.Strip(m.confirm.View(80, 24)); maxLineWidth(v) > 80 || !strings.Contains(v, copyMirrorKeys) {
			t.Errorf("%s: on an 80-column screen the dialog is %d wide and shows its keys as:\n%s", tc.name, maxLineWidth(v), v)
		}
		if press(m, "y") != nil || !m.confirm.IsOpen() {
			t.Fatalf("%s: y answered a question it is not an answer to", tc.name)
		}
		cmd := press(m, tc.key)
		if m.confirm.IsOpen() {
			t.Fatalf("%s: %s left the dialog open", tc.name, tc.key)
		}
		if tc.copied {
			if cmd == nil {
				t.Fatalf("%s: %s started no copy", tc.name, tc.key)
			}
			if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
				t.Fatalf("%s: the copy did not run", tc.name)
			}
		} else if cmd != nil || m.copying() || m.pendingCopy != nil {
			t.Fatalf("%s: %s did not cancel", tc.name, tc.key)
		}
		if _, err := os.Stat(filepath.Join(right, "d", "new.txt")); (err == nil) != tc.copied {
			t.Errorf("%s: d/new.txt copied: %v, want %v", tc.name, err == nil, tc.copied)
		}
		if _, err := os.Stat(filepath.Join(right, "d", "extra.txt")); (err == nil) != tc.kept {
			t.Errorf("%s: d/extra.txt kept: %v, want %v", tc.name, err == nil, tc.kept)
		}
	}
}

// F15 with F10: when the folder also holds files on both sides not compared
// yet, the dialog asks the two things one after the other.
func TestFolderCopyAsksDeletesThenFilesNotComparedYet(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "d", "same.txt"), "left bytes")
	writeTestFile(t, filepath.Join(right, "d", "same.txt"), "right byte")
	writeTestFile(t, filepath.Join(right, "d", "extra.txt"), "only on the right")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, p := range []string{filepath.Join(left, "d", "same.txt"), filepath.Join(right, "d", "same.txt")} {
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	m := scannedModel(t, left, right)
	m.cmpOpts.Checksum = true // switched on, nothing summed yet
	m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "d" && n.IsDir }))
	press(m, ">")
	if !m.confirm.IsOpen() || m.confirm.keys != copyMirrorKeys {
		t.Fatalf("the first question is not keep or delete: %q", m.confirm.keys)
	}
	if press(m, "c") != nil {
		t.Fatal("c started the copy before the second question")
	}
	if body := strings.Join(m.confirm.lines, "\n"); !m.confirm.IsOpen() || !strings.Contains(body, "1 files on both sides are not compared yet") {
		t.Fatalf("the second question is not the files not compared yet:\n%s", body)
	}
	cmd := press(m, "s")
	if cmd == nil {
		t.Fatal("s started no copy")
	}
	if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
		t.Fatal("the copy did not run")
	}
	if got, _ := os.ReadFile(filepath.Join(right, "d", "same.txt")); string(got) != "right byte" {
		t.Errorf("d/same.txt holds %q: s did not skip it", got)
	}
	if _, err := os.Stat(filepath.Join(right, "d", "extra.txt")); err != nil {
		t.Errorf("d/extra.txt is gone: c did not keep it: %v", err)
	}
}

// F15: a copy whose dialog showed a type conflict and no deletes ran as a
// mirror all the same. A folder it listed only then (after a shallow scan)
// lost what was only on the destination, which no dialog had shown.
func TestCopyThatShowedNoDeletesDeletesNothing(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeTestFile(t, filepath.Join(left, "x", "notes"), "a file on the left")
	writeTestFile(t, filepath.Join(right, "x", "notes", "2024.txt"), "a folder on the right")
	writeTestFile(t, filepath.Join(left, "x", "sub", "a.txt"), "on both")
	writeTestFile(t, filepath.Join(right, "x", "sub", "a.txt"), "on both")
	writeTestFile(t, filepath.Join(right, "x", "sub", "extra.txt"), "only on the right")
	m := scannedModel(t, left, right)
	m.mutateTree(func(root *model.TreeNode) { // as after a shallow scan
		sub := firstNode(root, func(n *model.TreeNode) bool { return n.RelPath == "x/sub" && n.IsDir })
		sub.Children, sub.Listed = nil, false
	})
	m.left.panel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "x" && n.IsDir }))
	press(m, ">")
	if !m.confirm.IsOpen() || m.confirm.keys != "" {
		t.Fatalf("> did not ask about the type conflict alone: keys %q", m.confirm.keys)
	}
	if body := strings.Join(m.confirm.lines, "\n"); strings.Contains(body, "Only on right") {
		t.Fatalf("the dialog shows deletes:\n%s", body)
	}
	cmd := press(m, "y")
	if cmd == nil {
		t.Fatal("y started no copy")
	}
	if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
		t.Fatal("the copy did not run")
	}
	if _, err := os.Stat(filepath.Join(right, "x", "notes")); err != nil {
		t.Errorf("x/notes was not copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(right, "x", "sub", "extra.txt")); err != nil {
		t.Errorf("x/sub/extra.txt is gone, and no dialog showed it: %v", err)
	}
}
