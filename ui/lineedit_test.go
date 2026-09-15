package ui

import (
	"testing"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
)

var namedKeys = map[string]tea.KeyType{
	"left": tea.KeyLeft, "backspace": tea.KeyBackspace, "up": tea.KeyUp, "down": tea.KeyDown, "enter": tea.KeyEnter,
	"esc": tea.KeyEsc, "tab": tea.KeyTab, "pgup": tea.KeyPgUp, "pgdown": tea.KeyPgDown, "ctrl+c": tea.KeyCtrlC,
}

func key(s string) tea.KeyMsg {
	if t, ok := namedKeys[s]; ok {
		return tea.KeyMsg{Type: t}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

// Editing happened by byte, so a backspace inside "café" split the é.
func TestLineEditorEditsByRune(t *testing.T) {
	var e lineEditor
	e.set("café.txt")
	for i := 0; i < 4; i++ {
		e.handleKey(key("left"))
	}
	e.handleKey(key("backspace"))
	if got := e.String(); got != "caf.txt" || !utf8.ValidString(got) {
		t.Fatalf("after backspace: %q", got)
	}
	e.handleKey(key("ü"))
	if got := e.String(); got != "cafü.txt" {
		t.Fatalf("after insert: %q", got)
	}
	if e.cursor != 4 {
		t.Errorf("cursor = %d, want 4 runes in", e.cursor)
	}
}
