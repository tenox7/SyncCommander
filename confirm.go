package main

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type ConfirmDialog struct {
	visible bool
	title   string
	lines   []string
	danger  bool
}

func NewConfirmDialog() *ConfirmDialog {
	return &ConfirmDialog{}
}

func (d *ConfirmDialog) Open(title string, lines []string, danger bool) {
	d.visible = true
	d.title = title
	d.lines = lines
	d.danger = danger
}

func (d *ConfirmDialog) Close() {
	d.visible = false
}

func (d *ConfirmDialog) IsOpen() bool {
	return d.visible
}

func (d *ConfirmDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	borderColor := lipgloss.Color("4")
	titleStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	if d.danger {
		borderColor = lipgloss.Color("1")
		titleStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	}

	var sb strings.Builder
	sb.WriteString(titleStyle.Render(d.title))
	for _, line := range d.lines {
		sb.WriteString("\n")
		sb.WriteString(line)
	}
	sb.WriteString("\n\n")
	if d.danger {
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render("Y=confirm"))
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("  Esc=cancel"))
	} else {
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Y=confirm  Esc=cancel"))
	}

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(1, 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
