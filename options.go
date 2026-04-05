package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type Option struct {
	Label string
	Value *bool
}

type OptionsDialog struct {
	options []Option
	cursor  int
	visible bool
}

func NewOptionsDialog() *OptionsDialog {
	return &OptionsDialog{}
}

func (d *OptionsDialog) SetOptions(opts []Option) {
	d.options = opts
}

func (d *OptionsDialog) Open() {
	d.visible = true
	d.cursor = 0
}

func (d *OptionsDialog) Close() {
	d.visible = false
}

func (d *OptionsDialog) IsOpen() bool {
	return d.visible
}

func (d *OptionsDialog) MoveUp() {
	if d.cursor > 0 {
		d.cursor--
	}
}

func (d *OptionsDialog) MoveDown() {
	if d.cursor < len(d.options)-1 {
		d.cursor++
	}
}

func (d *OptionsDialog) UpdateChecksumLabel(algo string) {
	label := fmt.Sprintf("Checksum (%s)", algo)
	for i := range d.options {
		if strings.HasPrefix(d.options[i].Label, "Checksum") {
			d.options[i].Label = label
			break
		}
	}
}

func (d *OptionsDialog) Toggle() {
	if d.cursor >= 0 && d.cursor < len(d.options) {
		opt := &d.options[d.cursor]
		*opt.Value = !*opt.Value
	}
}

var (
	styleDialogBorder = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("4")).
				Padding(1, 2)
	styleDialogTitle = lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	styleOptOn       = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleOptOff      = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

func (d *OptionsDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(styleDialogTitle.Render("Options"))
	sb.WriteString("\n\n")

	for i, opt := range d.options {
		marker := "  "
		if i == d.cursor {
			marker = "▶ "
		}
		state := styleOptOff.Render("[off]")
		if *opt.Value {
			state = styleOptOn.Render("[ on]")
		}
		sb.WriteString(fmt.Sprintf("%s%s  %s\n", marker, state, opt.Label))
	}

	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Space=toggle  Esc=close"))

	dialog := styleDialogBorder.Render(sb.String())

	dw := lipgloss.Width(dialog)
	dh := lipgloss.Height(dialog)
	x := (width - dw) / 2
	y := (height - dh) / 2
	if x < 0 {
		x = 0
	}
	if y < 0 {
		y = 0
	}

	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
