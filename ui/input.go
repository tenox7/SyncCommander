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
	err     string // why the last Enter was refused; the dialog stays open
}

func NewInputDialog() *InputDialog {
	return &InputDialog{}
}

func (d *InputDialog) Open(title, initial string, onDone func(string) tea.Cmd) {
	d.visible = true
	d.title = title
	d.ed.set(initial)
	d.onDone = onDone
	d.err = ""
}

// SetError refuses the value just entered: called from onDone, it keeps the
// dialog open with msg under the text.
func (d *InputDialog) SetError(msg string) { d.err = msg }

func (d *InputDialog) Close() {
	d.visible = false
	d.onDone = nil
}

func (d *InputDialog) IsOpen() bool { return d.visible }

func (d *InputDialog) Confirm() tea.Cmd {
	d.err = ""
	var cmd tea.Cmd
	if d.onDone != nil {
		cmd = d.onDone(d.ed.String())
	}
	if d.err == "" {
		d.Close()
	}
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
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Render(shown(d.title)))
	sb.WriteString("\n\n")
	sb.WriteString(d.ed.render())
	if d.err != "" {
		sb.WriteString("\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(shown(d.err)))
	}
	sb.WriteString("\n\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Enter=confirm  Esc=cancel"))
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, styleInputBorder.Render(sb.String()))
}
