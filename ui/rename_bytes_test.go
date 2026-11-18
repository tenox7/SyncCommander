package ui

import (
	"context"
	"path"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
)

// latin1 is the name an old server gives in Latin-1: not UTF-8.
const latin1 = "r\xe9sum\xe9.txt"

// latin1Names lists resume.txt as latin1, as an old Unix server whose names
// are Latin-1 does, and keeps each rename it is asked for instead of
// making it.
type latin1Names struct {
	model.Backend
	renames *[]string
}

func (b latin1Names) List(ctx context.Context, rel string) ([]model.FileEntry, error) {
	entries, err := b.Backend.List(ctx, rel)
	for i := range entries {
		if entries[i].Name == "resume.txt" {
			entries[i].Name = latin1
			entries[i].RelPath = strings.TrimPrefix(path.Join(rel, latin1), "/")
		}
	}
	return entries, err
}

func (b latin1Names) Rename(_ context.Context, oldRel, newRel string) error {
	*b.renames = append(*b.renames, oldRel+" -> "+newRel)
	return nil
}

// U17: the editor turned each byte that is not UTF-8 into U+FFFD, so the
// text never equalled the old name: e, then Enter with nothing typed,
// renamed r\xe9sum\xe9.txt to r�sum�.txt on both sides. An
// untouched name, or one edited back, is now no rename, and a new name
// that would hold U+FFFD for those bytes is refused.
func TestRenameKeepsANameThatIsNotUTF8(t *testing.T) {
	root := t.TempDir()
	var renames []string
	sides := [2]model.Backend{}
	for i, side := range []string{"left", "right"} {
		writeTestFile(t, filepath.Join(root, side, "resume.txt"), "x")
		writeTestFile(t, filepath.Join(root, side, "odd\ufffd.txt"), "x")
		sides[i] = latin1Names{Backend: openBackendOrSkip(t, filepath.Join(root, side)), renames: &renames}
	}
	m := newTestModel(t, sides[0], sides[1])
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	settled(m) // errors other tests logged do not open the log over the tree
	node := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == latin1 })
	if node == nil {
		t.Fatal("the Latin-1 name is not in the tree")
	}
	enter := func(what string, keys ...tea.KeyMsg) {
		t.Helper()
		m.openRename(node)
		for _, k := range keys {
			m.Update(k)
		}
		if cmd := press(m, "enter"); cmd != nil {
			m.Update(runFirst(cmd))
		}
		if len(renames) > 0 {
			t.Errorf("%s: renamed %q", what, renames)
			renames = nil
		}
	}
	enter("Enter with nothing typed")
	enter("a letter typed and taken back", key("x"), key("backspace"))
	enter("the cursor moved", key("left"), key("left"))
	if m.input.IsOpen() {
		t.Fatalf("the dialog is open after an unchanged name: %q", m.input.err)
	}
	enter("a letter added to the name", key("x"))
	if !m.input.IsOpen() || !strings.Contains(m.input.err, "not text") {
		t.Errorf("dialog open=%v err=%q, want it open with the reason", m.input.IsOpen(), m.input.err)
	}
	m.input.Close()
	// A name typed in full is renamed to.
	m.openRename(node)
	m.Update(tea.KeyMsg{Type: tea.KeyCtrlU})
	m.Update(key("resume2.txt"))
	if cmd := press(m, "enter"); cmd != nil {
		m.Update(runFirst(cmd))
	}
	want := latin1 + " -> resume2.txt"
	if len(renames) != 2 || renames[0] != want || renames[1] != want {
		t.Errorf("renames %q, want %q on both sides", renames, want)
	}
	// A name that is UTF-8 may hold U+FFFD itself, and keep it.
	renames = nil
	odd := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == "odd\ufffd.txt" })
	if odd == nil {
		t.Fatal("odd\ufffd.txt is not in the tree")
	}
	m.openRename(odd)
	m.input.ed.set("odd\ufffd2.txt")
	if cmd := press(m, "enter"); cmd != nil {
		m.Update(runFirst(cmd))
	}
	if want := "odd\ufffd.txt -> odd\ufffd2.txt"; len(renames) != 2 || renames[0] != want {
		t.Errorf("renames %q, want %q on both sides (dialog err %q)", renames, want, m.input.err)
	}
}

// U17: the editor gives back the bytes it was given while its runes are
// the ones it made of them.
func TestLineEditorKeepsBytesThatAreNotUTF8(t *testing.T) {
	var e lineEditor
	e.set(latin1)
	if got := e.String(); got != latin1 {
		t.Errorf("unedited: %q, want %q", got, latin1)
	}
	e.handleKey(key("x"))
	if got := e.String(); got != "r�sum�.txtx" {
		t.Errorf("edited: %q", got)
	}
	e.handleKey(key("backspace"))
	if got := e.String(); got != latin1 {
		t.Errorf("edited back: %q, want %q", got, latin1)
	}
	e.set("abc")
	e.handleKey(key("backspace"))
	e.handleKey(key("d"))
	if got := e.String(); got != "abd" {
		t.Errorf("a plain name edited: %q, want abd", got)
	}
}
