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
	"sc/transfer"
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
	var replaced []string
	for _, c := range m.pendingCopy.plan.Conflicts {
		replaced = append(replaced, c.RelPath)
	}
	if !slices.Equal(replaced, []string{"notes"}) {
		t.Errorf("pending copy replaces %q, want [notes]", replaced)
	}
	if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "This item has a different type on the left.\nThe copy DELETES it on the right:\n  notes/  (folder: 1 file)") {
		t.Errorf("dialog does not name the folder it deletes:\n%s", body)
	}
}

// askingMirror reports whether the open dialog is a copy's question to
// keep or to delete what is only on the destination.
func askingMirror(m *Model) bool {
	return m.confirm.IsOpen() && m.pendingCopy != nil && m.pendingCopy.ask == askMirror && strings.Contains(m.confirm.keys, "\nd   = copy. DELETE ")
}

// The keep-or-delete question names what is only on the destination by
// its count, and its keys name it the same way: "c=copy, keep them
// d=copy and delete them" did not say which entries "them" were.
func TestDeletesWhatNamesTheCount(t *testing.T) {
	file := func(p string) transfer.Delete { return transfer.Delete{RelPath: p} }
	dir := func(p string, files, dirs int) transfer.Delete {
		return transfer.Delete{RelPath: p, IsDir: true, Tally: transfer.Tally{Files: files, Dirs: dirs, Complete: true}}
	}
	for _, c := range []struct {
		deletes []transfer.Delete
		want    string
		one     bool
	}{
		{[]transfer.Delete{file("a")}, "this file", true},
		{[]transfer.Delete{file("a"), file("b"), file("c")}, "these 3 files", false},
		{[]transfer.Delete{dir("d", 0, 0)}, "this folder", true},
		{[]transfer.Delete{dir("d", 0, 0), dir("e", 0, 0)}, "these 2 folders", false},
		{[]transfer.Delete{dir("d", 2, 0)}, "these 2 files and 1 folder", false},
		{[]transfer.Delete{file("a"), dir("d", 0, 1)}, "these 1 file and 2 folders", false},
	} {
		if what, one := deletesWhat(c.deletes); what != c.want || one != c.one {
			t.Errorf("deletesWhat(%+v) = %q, %v; want %q, %v", c.deletes, what, one, c.want, c.one)
		}
	}
	want := "c   = copy. KEEP these 3 files on the left.\nd   = copy. DELETE these 3 files on the left.\nEsc = cancel. Nothing changes."
	if got := mirrorKeys("these 3 files", "left"); got != want {
		t.Errorf("mirrorKeys = %q, want %q", got, want)
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
		if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "This file is only on the right:\n  d/extra.txt") {
			t.Errorf("%s: the dialog does not say what is only on the right:\n%s", tc.name, body)
		}
		keys := []string{"c   = copy. KEEP this file on the right.", "d   = copy. DELETE this file on the right.", "Esc = cancel. Nothing changes."}
		if m.confirm.keys != strings.Join(keys, "\n") {
			t.Fatalf("%s: the dialog offers %q", tc.name, m.confirm.keys)
		}
		v := ansi.Strip(m.confirm.View(80, 24))
		for _, k := range keys {
			if maxLineWidth(v) > 80 || !strings.Contains(v, k) {
				t.Errorf("%s: on an 80-column screen the dialog is %d wide and shows no line %q:\n%s", tc.name, maxLineWidth(v), k, v)
			}
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
	if !askingMirror(m) {
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
	if body := strings.Join(m.confirm.lines, "\n"); strings.Contains(body, "only on the right") {
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

// A folder copy from an empty folder to one that holds files is almost
// always a mistake: the wrong folder, the wrong way, or a disk that is not
// mounted. Its mirror deleted everything on the destination after a dialog
// like any other. Now the dialog that asks to delete shows a red warning
// too, in the same dialog: d deletes, Esc cancels.
func TestCopyFromAnEmptyFolderWarnsInTheDialog(t *testing.T) {
	for _, tc := range []struct {
		key, answer string
		leftToRight bool
	}{
		{">", "d", true},
		{"<", "d", false},
		{">", "esc", true},
	} {
		root := t.TempDir()
		left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
		empty, full, src, dst := left, right, "LEFT", "right"
		if !tc.leftToRight {
			empty, full, src, dst = right, left, "RIGHT", "left"
		}
		if err := os.MkdirAll(filepath.Join(empty, "d"), 0o755); err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, filepath.Join(full, "d", "a.txt"), "12345")
		writeTestFile(t, filepath.Join(full, "d", "sub", "b.txt"), "678")
		m := scannedModel(t, left, right)
		cursorTo(t, m, "d", true)
		press(m, tc.key)
		want := []string{
			"⚠ WARNING: THE " + src + " HAS NO FILES.",
			"The " + dst + " has 2 files, 1 folder, 8B.",
			"This is almost always a mistake.",
			"Make sure that the folder and the direction are correct.",
		}
		if !askingMirror(m) || !slices.Equal(m.confirm.warn, want) {
			t.Fatalf("%s from an empty folder: keys %q, warn %q, want %q", tc.key, m.confirm.keys, m.confirm.warn, want)
		}
		if body := strings.Join(m.confirm.lines, "\n"); !strings.Contains(body, "These 2 files and 1 folder are only on the "+dst+":") {
			t.Errorf("%s: the dialog does not name what it deletes:\n%s", tc.key, body)
		}
		v := ansi.Strip(m.confirm.View(80, 24))
		for _, w := range want {
			if maxLineWidth(v) > 80 || !strings.Contains(v, w) {
				t.Errorf("%s: on an 80-column screen the dialog is %d wide and shows no line %q:\n%s", tc.key, maxLineWidth(v), w, v)
			}
		}
		if tc.answer == "esc" {
			if press(m, "esc") != nil || m.confirm.IsOpen() || m.pendingCopy != nil {
				t.Fatal("Esc did not cancel")
			}
			if _, err := os.Stat(filepath.Join(full, "d", "a.txt")); err != nil {
				t.Errorf("Esc deleted d/a.txt: %v", err)
			}
			continue
		}
		runCopyAnswer(t, m, tc.answer)
		for _, p := range []string{filepath.Join(full, "d", "a.txt"), filepath.Join(full, "d", "sub")} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Errorf("%s, d: %s is still there: %v", tc.key, p, err)
			}
		}
	}
}

// A folder copy from a side with one file, one folder or one byte less
// than the other warns too, in its first question, with what each side
// holds. One from a side that holds as much does not, nor one from a side
// not fully listed, as what it holds is not known.
func TestCopyWithLessWarnsInTheFirstQuestion(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		left, right map[string]string // path: content; a path that ends in / is a folder
		ask         copyAsk
		want        []string
	}{
		{"one byte less", map[string]string{"a.txt": "abc", "sub/b.txt": "b"}, map[string]string{"a.txt": "abcdefgh", "sub/b.txt": "b"}, askNewer, []string{
			"⚠ WARNING: THE LEFT HAS LESS DATA THAN THE RIGHT.",
			"Left:  2 files, 1 folder, 4B",
			"Right: 2 files, 1 folder, 9B",
		}},
		{"one file less", map[string]string{"a.txt": "0123456789"}, map[string]string{"a.txt": "0123456789", "x.txt": "x"}, askMirror, []string{
			"⚠ WARNING: THE LEFT HAS FEWER FILES AND LESS DATA THAN THE RIGHT.",
			"Left:  1 file, 0 folders, 10B",
			"Right: 2 files, 0 folders, 11B",
		}},
		{"one folder less", map[string]string{"a.txt": "0123456789"}, map[string]string{"a.txt": "0123456789", "e/": ""}, askMirror, []string{
			"⚠ WARNING: THE LEFT HAS FEWER FOLDERS THAN THE RIGHT.",
			"Left:  1 file, 0 folders, 10B",
			"Right: 1 file, 1 folder, 10B",
		}},
		{"as much", map[string]string{"a.txt": "0123456789", "b.txt": "b"}, map[string]string{"a.txt": "0123", "c.txt": "c"}, askMirror, nil},
	} {
		root := t.TempDir()
		left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
		for dir, files := range map[string]map[string]string{left: tc.left, right: tc.right} {
			for p, body := range files {
				if strings.HasSuffix(p, "/") {
					if err := os.MkdirAll(filepath.Join(dir, "d", p), 0o755); err != nil {
						t.Fatal(err)
					}
					continue
				}
				writeAt(t, filepath.Join(dir, "d", p), body, at)
			}
		}
		m := scannedModel(t, left, right)
		cursorTo(t, m, "d", true)
		press(m, ">")
		if tc.want != nil {
			tc.want = append(tc.want, "Make sure that the folder and the direction are correct.")
		}
		if !m.confirm.IsOpen() || m.pendingCopy == nil || m.pendingCopy.ask != tc.ask || !slices.Equal(m.confirm.warn, tc.want) {
			t.Errorf("%s: ask %v, warn %q, want ask %v, warn %q", tc.name, m.pendingCopy, m.confirm.warn, tc.ask, tc.want)
		}
		press(m, "esc")
		if tc.name != "one byte less" {
			continue
		}
		unlist(t, m, "d/sub")
		cursorTo(t, m, "d", true)
		press(m, ">")
		if !m.confirm.IsOpen() || m.confirm.warn != nil {
			t.Errorf("a copy from a folder not fully listed warned: %q", m.confirm.warn)
		}
		press(m, "esc")
	}
}

// The user's case: > on a folder where a cut-off copy left part files of
// the files it copies asked "c=copy, keep them  d=copy and delete them"
// about those parts, and c kept nothing: the copy wrote into them. Now no
// dialog opens and the copy runs. A part with no file to copy is still
// asked about, and the dialog says what it is.
func TestCopyDoesNotAskAboutItsOwnParts(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	for _, n := range []string{"1617", "1618", "1619"} {
		writeTestFile(t, filepath.Join(left, "ipad", "IMG_"+n+".JPG"), "jpeg "+n)
		writeTestFile(t, filepath.Join(right, "ipad", ".IMG_"+n+".JPG.sc-part"), "jp")
	}
	m := scannedModel(t, left, right)
	cursorTo(t, m, "ipad", true)
	cmd := press(m, ">")
	if m.confirm.IsOpen() || cmd == nil {
		t.Fatalf("> asked about the copy's own parts:\n%s", strings.Join(m.confirm.lines, "\n"))
	}
	if _, ok := runFirst(cmd).(copyDoneMsg); !ok {
		t.Fatal("the copy did not run")
	}
	for _, n := range []string{"1617", "1618", "1619"} {
		if got := readTestFile(t, filepath.Join(right, "ipad", "IMG_"+n+".JPG")); got != "jpeg "+n {
			t.Errorf("IMG_%s.JPG holds %q", n, got)
		}
		if _, err := os.Stat(filepath.Join(right, "ipad", ".IMG_"+n+".JPG.sc-part")); !os.IsNotExist(err) {
			t.Errorf("the part of IMG_%s.JPG is still there: %v", n, err)
		}
	}

	writeTestFile(t, filepath.Join(right, "ipad", ".IMG_1620.JPG.sc-part"), "jp")
	m = scannedModel(t, left, right)
	cursorTo(t, m, "ipad", true)
	press(m, ">")
	if body := strings.Join(m.confirm.lines, "\n"); !askingMirror(m) || !strings.Contains(body, "This file is only on the right:\n  ipad/.IMG_1620.JPG.sc-part  (part file of a copy that stopped)") {
		t.Errorf("a part with no file to copy is not asked about as one:\n%s", body)
	}
}
