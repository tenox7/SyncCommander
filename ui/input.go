package ui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type InputDialog struct {
	visible bool
	title   string
	ed      lineEditor
	onDone  func(string) tea.Cmd
}

func NewInputDialog() *InputDialog {
	return &InputDialog{}
}

func (d *InputDialog) Open(title, initial string, onDone func(string) tea.Cmd) {
	d.visible = true
	d.title = title
	d.ed.set(initial)
	d.onDone = onDone
}

func (d *InputDialog) Close() {
	d.visible = false
	d.onDone = nil
}

func (d *InputDialog) IsOpen() bool { return d.visible }

func (d *InputDialog) Confirm() tea.Cmd {
	var cmd tea.Cmd
	if d.onDone != nil {
		cmd = d.onDone(d.ed.String())
	}
	d.Close()
	return cmd
}

func (d *InputDialog) HandleKey(msg tea.KeyMsg) { d.ed.handleKey(msg) }

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
	sb.WriteString(d.ed.render())
	sb.WriteString("\n\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Enter=confirm  Esc=cancel"))
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, styleInputBorder.Render(sb.String()))
}
