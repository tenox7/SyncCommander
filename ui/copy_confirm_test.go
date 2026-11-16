package ui

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
	m.leftPanel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "a.txt" }))
	press(m, ">")
	if m.confirm.IsOpen() || m.pendingCopy != nil {
		t.Errorf("> on a.txt opened a dialog: %q", m.confirm.lines)
	}

	m = scannedModel(t, left, right)
	m.leftPanel.jumpTo(firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "notes" && !n.IsDir }))
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
