package ui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// lineEditor is a single-line text field edited by rune, so the cursor and
// backspace never split a multibyte character.
type lineEditor struct {
	runes  []rune
	cursor int
}

func (e *lineEditor) set(s string) {
	e.runes = []rune(s)
	e.cursor = len(e.runes)
}

func (e *lineEditor) String() string { return string(e.runes) }

// handleKey applies one key; it reports false for keys it does not handle so
// the dialog can act on them.
func (e *lineEditor) handleKey(msg tea.KeyMsg) bool {
	switch msg.String() {
	case "left":
		e.cursor = max(e.cursor-1, 0)
	case "right":
		e.cursor = min(e.cursor+1, len(e.runes))
	case "home", "ctrl+a":
		e.cursor = 0
	case "end", "ctrl+e":
		e.cursor = len(e.runes)
	case "backspace":
		if e.cursor > 0 {
			e.runes = append(e.runes[:e.cursor-1], e.runes[e.cursor:]...)
			e.cursor--
		}
	case "delete":
		if e.cursor < len(e.runes) {
			e.runes = append(e.runes[:e.cursor], e.runes[e.cursor+1:]...)
		}
	case "ctrl+u":
		e.runes = e.runes[e.cursor:]
		e.cursor = 0
	case "ctrl+k":
		e.runes = e.runes[:e.cursor]
	default:
		if len(msg.Runes) == 0 {
			return false
		}
		s := string(msg.Runes)
		if msg.Paste {
			s = strings.NewReplacer("\r", "", "\n", "", "\t", " ").Replace(s)
		}
		ins := []rune(s)
		e.runes = append(e.runes[:e.cursor], append(ins, e.runes[e.cursor:]...)...)
		e.cursor += len(ins)
	}
	return true
}

var styleCursorCell = lipgloss.NewStyle().Reverse(true)

// render draws the text with the cursor cell reversed.
func (e *lineEditor) render() string {
	before := string(e.runes[:e.cursor])
	cell, after := " ", ""
	if e.cursor < len(e.runes) {
		cell, after = string(e.runes[e.cursor]), string(e.runes[e.cursor+1:])
	}
	return before + styleCursorCell.Render(cell) + after
}
