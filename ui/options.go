package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type Option struct {
	Label string
	Value *bool
}

type SettingsDialog struct {
	options []Option
	cursor  int
	visible bool
}

func NewSettingsDialog() *SettingsDialog {
	return &SettingsDialog{}
}

func (d *SettingsDialog) SetOptions(opts []Option) {
	d.options = opts
}

func (d *SettingsDialog) Open() {
	d.visible = true
	d.cursor = 0
}

func (d *SettingsDialog) Close() {
	d.visible = false
}

func (d *SettingsDialog) IsOpen() bool {
	return d.visible
}

func (d *SettingsDialog) MoveUp() {
	if d.cursor > 0 {
		d.cursor--
	}
}

func (d *SettingsDialog) MoveDown() {
	if d.cursor < len(d.options)-1 {
		d.cursor++
	}
}

func (d *SettingsDialog) UpdateChecksumLabel(algo string) {
	label := fmt.Sprintf("Checksum (%s)", algo)
	for i := range d.options {
		if strings.HasPrefix(d.options[i].Label, "Checksum") {
			d.options[i].Label = label
			break
		}
	}
}

func (d *SettingsDialog) Toggle() {
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

func (d *SettingsDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(styleDialogTitle.Render("Settings"))
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
