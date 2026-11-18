package ui

import (
	"slices"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// lineEditor is a single-line text field edited by rune, so the cursor and
// backspace never split a multibyte character. Each byte of a text that is
// not UTF-8 is a U+FFFD rune: while the runes are the ones set made, String
// gives the text set was given, byte for byte. A name of Latin-1 bytes
// came back with U+FFFD in place of them, and Enter with nothing typed
// renamed the file to that (U17).
type lineEditor struct {
	runes  []rune
	cursor int
	given  string // the text set was given
	made   []rune // the runes set made of it
}

func (e *lineEditor) set(s string) {
	e.given = s
	e.runes = []rune(s)
	e.made = slices.Clone(e.runes)
	e.cursor = len(e.runes)
}

func (e *lineEditor) String() string {
	if slices.Equal(e.runes, e.made) {
		return e.given
	}
	return string(e.runes)
}

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

// render draws the text with the cursor cell reversed. A name being
// renamed can hold controls: they are drawn escaped, the text keeps them.
func (e *lineEditor) render() string {
	before := string(e.runes[:e.cursor])
	cell, after := " ", ""
	if e.cursor < len(e.runes) {
		cell, after = string(e.runes[e.cursor]), string(e.runes[e.cursor+1:])
	}
	return shown(before) + styleCursorCell.Render(shown(cell)) + shown(after)
}
