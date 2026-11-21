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
	choiceMode bool     // L=left / R=right / B=both, used for PresenceBoth deletes
	keys       string   // the keys line, when the dialog takes other keys than Y
	warn       []string // a red band under the title (Warn)
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
	d.warn = nil
}

// Warn puts lines in a red band under the open dialog's title, for what is
// almost always a mistake. The next Open takes it away.
func (d *ConfirmDialog) Warn(lines []string) { d.warn = lines }

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
	wrap := func(s string, room int) string {
		s = shown(s)
		body := strings.TrimLeft(s, " ")
		indent := s[:len(s)-len(body)]
		return indent + strings.ReplaceAll(ansi.Wrap(body, max(room-len(indent), 16), "/-"), "\n", "\n"+indent)
	}
	fit := func(s string) string { return wrap(s, width-8) }
	var sb strings.Builder
	sb.WriteString(titleStyle.Render(fit(d.title)))
	if len(d.warn) > 0 {
		// Bold white on red, with 2 cells of padding on each side, as wide
		// as the widest line of the box.
		wide := 0
		for _, s := range append(append([]string{d.title}, d.lines...), strings.Split(d.keys, "\n")...) {
			for _, l := range strings.Split(fit(s), "\n") {
				wide = max(wide, ansi.StringWidth(l))
			}
		}
		lines := make([]string, len(d.warn))
		for i, l := range d.warn {
			lines[i] = wrap(l, width-12)
			for _, w := range strings.Split(lines[i], "\n") {
				wide = max(wide, ansi.StringWidth(w)+4)
			}
		}
		band := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("1")).Padding(1, 2).Width(wide)
		sb.WriteString("\n\n")
		sb.WriteString(band.Render(strings.Join(lines, "\n")))
	}
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
		// The keys can take a line each, and wrap as the lines above do.
		for i, k := range strings.Split(d.keys, "\n") {
			if i > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(style.Render(fit(k)))
		}
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
	lines := []string{nodeName(node)}
	if left {
		lines = append(lines, "  in L: "+m.left.backend.BasePath())
	}
	if right {
		lines = append(lines, "  in R: "+m.right.backend.BasePath())
	}
	return lines
}

// nodeName names node for a dialog by its whole path. Call it under the
// read lock.
func nodeName(node *model.TreeNode) string {
	if node.IsRoot() {
		return "the top folder"
	}
	if node.IsDir {
		return node.RelPath + "/"
	}
	return node.RelPath
}
