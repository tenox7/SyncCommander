package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type InputDialog struct {
	visible bool
	title   string
	value   string
	cursor  int
	onDone  func(string)
}

func NewInputDialog() *InputDialog {
	return &InputDialog{}
}

func (d *InputDialog) Open(title, initial string, onDone func(string)) {
	d.visible = true
	d.title = title
	d.value = initial
	d.cursor = len(initial)
	d.onDone = onDone
}

func (d *InputDialog) Close() {
	d.visible = false
	d.onDone = nil
}

func (d *InputDialog) IsOpen() bool {
	return d.visible
}

func (d *InputDialog) Confirm() {
	if d.onDone != nil {
		d.onDone(d.value)
	}
	d.Close()
}

func (d *InputDialog) HandleKey(key string) {
	switch key {
	case "left":
		if d.cursor > 0 {
			d.cursor--
		}
	case "right":
		if d.cursor < len(d.value) {
			d.cursor++
		}
	case "home", "ctrl+a":
		d.cursor = 0
	case "end", "ctrl+e":
		d.cursor = len(d.value)
	case "backspace":
		if d.cursor > 0 {
			d.value = d.value[:d.cursor-1] + d.value[d.cursor:]
			d.cursor--
		}
	case "delete":
		if d.cursor < len(d.value) {
			d.value = d.value[:d.cursor] + d.value[d.cursor+1:]
		}
	case "ctrl+u":
		d.value = d.value[d.cursor:]
		d.cursor = 0
	case "ctrl+k":
		d.value = d.value[:d.cursor]
	default:
		if len(key) == 1 && key[0] >= 32 {
			d.value = d.value[:d.cursor] + key + d.value[d.cursor:]
			d.cursor++
		}
	}
}

var styleInputBorder = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("4")).
	Padding(1, 2)

func (d *InputDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Render(d.title))
	sb.WriteString("\n\n")

	before := d.value[:d.cursor]
	after := d.value[d.cursor:]
	cursorChar := " "
	if d.cursor < len(d.value) {
		cursorChar = string(d.value[d.cursor])
		after = d.value[d.cursor+1:]
	}
	cursor := lipgloss.NewStyle().Reverse(true).Render(cursorChar)
	sb.WriteString(fmt.Sprintf("%s%s%s", before, cursor, after))

	sb.WriteString("\n\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Enter=confirm  Esc=cancel"))

	dialog := styleInputBorder.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
