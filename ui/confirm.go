package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

type ConfirmDialog struct {
	visible    bool
	title      string
	lines      []string
	danger     bool
	choiceMode bool   // L=left / R=right / B=both, used for PresenceBoth deletes
	keys       string // the keys line, when the dialog takes other keys than Y
}

func NewConfirmDialog() *ConfirmDialog {
	return &ConfirmDialog{}
}

func (d *ConfirmDialog) Open(title string, lines []string, danger bool) {
	d.visible = true
	d.title = title
	d.lines = lines
	d.danger = danger
	d.choiceMode = false
	d.keys = ""
}

// OpenKeys is Open with its own keys line; the caller handles those keys.
func (d *ConfirmDialog) OpenKeys(title string, lines []string, danger bool, keys string) {
	d.Open(title, lines, danger)
	d.keys = keys
}

func (d *ConfirmDialog) OpenChoice(title string, lines []string, danger bool) {
	d.Open(title, lines, danger)
	d.choiceMode = true
}

func (d *ConfirmDialog) Close() {
	d.visible = false
	d.choiceMode = false
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

	// Lines wrap inside the box, after a / or a - where they can, and
	// keep their indent: a long path went past the edge of the screen
	// and was cut there (U20). The box takes 2 cells of border, 4 of
	// padding and a margin of 2.
	fit := func(s string) string {
		s = shown(s)
		body := strings.TrimLeft(s, " ")
		indent := s[:len(s)-len(body)]
		return indent + strings.ReplaceAll(ansi.Wrap(body, max(width-8-len(indent), 16), "/-"), "\n", "\n"+indent)
	}
	var sb strings.Builder
	sb.WriteString(titleStyle.Render(fit(d.title)))
	for _, line := range d.lines {
		sb.WriteString("\n")
		sb.WriteString(fit(line))
	}
	sb.WriteString("\n\n")
	if d.keys != "" {
		style := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
		if d.danger {
			style = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
		}
		sb.WriteString(style.Render(d.keys))
	} else if d.choiceMode {
		accent := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
		if d.danger {
			accent = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
		}
		sb.WriteString(accent.Render("a=left only  l=right only  b=both sides"))
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("  Esc=cancel"))
	} else if d.danger {
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

// whereLines names node for a dialog by its whole path, and the folder it
// is in on each side named, as the panel titles show them. The dialogs
// named the node alone, over a screen that hides the tree: which of two
// folders of one name, or of two long names alike up to the end, was not
// shown, and the top folder showed as "//" (U20). Call it under the read
// lock.
func (m *Model) whereLines(node *model.TreeNode, left, right bool) []string {
	what := "the top folder"
	if !node.IsRoot() {
		what = node.RelPath
		if node.IsDir {
			what += "/"
		}
	}
	lines := []string{what}
	if left {
		lines = append(lines, "  in L: "+m.left.backend.BasePath())
	}
	if right {
		lines = append(lines, "  in R: "+m.right.backend.BasePath())
	}
	return lines
}
