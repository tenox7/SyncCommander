package ui

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

type helpEntry struct{ key, desc string }

var helpNavigation = []helpEntry{
	{"→/Enter", "Expand directory"},
	{"←", "Collapse / parent"},
	{"}", "Expand all"},
	{"{", "Collapse all"},
	{"Tab", "Switch panel"},
	{"PgUp/Dn", "Page up / down"},
	{"n", "Next difference"},
	{"/", "Search (regex)"},
	{"i", "Info / statistics"},
	{"~", "Remote protocol log"},
	{"=", "Settings"},
	{"w", "Toggle line wrap"},
	{"Ctrl+L", "Redraw screen"},
	{"q", "Quit"},
}

var helpOperations = []helpEntry{
	{"b", "Set base to dir"},
	{"u", "Set base to parent dir"},
	{"y", "Change directory / URL"},
	{"o", "Open file diff / hex view"},
	{"r", "Rescan"},
	{"R", "Deep scan recursively"},
	{"c", "Compute checksum (CRC)"},
	{"t", "Touch: update timestamps"},
	{"e", "Rename file or directory"},
	{"d", "Delete file or directory"},
	{"s", "Swap left and right"},
	{">", "Copy left → right"},
	{"<", "Copy right → left"},
	{"x", "Cancel in-progress copy"},
}

func renderHelpSection(title string, entries []helpEntry) string {
	var sb strings.Builder
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	keyStyle := lipgloss.NewStyle().Bold(true).Width(10)

	sb.WriteString(titleStyle.Render(title))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", len(title)))
	sb.WriteString("\n")
	for _, h := range entries {
		sb.WriteString(keyStyle.Render(h.key))
		sb.WriteString(h.desc)
		sb.WriteString("\n")
	}
	return sb.String()
}

func (d *HelpDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	nav := renderHelpSection("Navigation", helpNavigation)
	ops := renderHelpSection("Operations", helpOperations)

	colStyle := lipgloss.NewStyle().PaddingRight(4)
	columns := lipgloss.JoinHorizontal(lipgloss.Top, colStyle.Render(nav), ops)

	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Bold(true).Render("Keyboard Shortcuts"))
	sb.WriteString("\n\n")
	sb.WriteString(columns)
	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("12")).Render("?=help  Esc=close"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(1, 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
