package main

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type HelpDialog struct {
	visible bool
}

func NewHelpDialog() *HelpDialog {
	return &HelpDialog{}
}

func (d *HelpDialog) Open()        { d.visible = true }
func (d *HelpDialog) Close()       { d.visible = false }
func (d *HelpDialog) IsOpen() bool { return d.visible }

var helpKeys = []struct{ key, desc string }{
	{"R", "Rescan file or directory"},
	{"C", "Compute checksum (SHA256)"},
	{"T", "Touch — sync timestamps"},
	{"N", "Rename file or directory"},
	{"D", "Delete file or directory"},
	{">", "Copy left → right (mirror)"},
	{"<", "Copy right → left (mirror)"},
	{"→/Enter", "Expand directory"},
	{"←", "Collapse / go to parent"},
	{"Tab", "Switch active panel"},
	{"PgUp/Dn", "Page up / down"},
	{"I", "Info / statistics"},
	{"~", "Remote protocol log"},
	{"S", "Settings"},
	{"W", "Toggle line wrap"},
	{"Ctrl+L", "Redraw screen"},
	{"Q / Esc", "Quit"},
}

func (d *HelpDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Render("Keyboard Shortcuts"))
	sb.WriteString("\n\n")

	keyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Width(10)
	descStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("7"))

	for _, h := range helpKeys {
		sb.WriteString(keyStyle.Render(h.key))
		sb.WriteString(descStyle.Render(h.desc))
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("?=help  Esc=close"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(1, 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
