package ui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type OpenDialog struct {
	visible     bool
	left        lineEditor
	right       lineEditor
	activeRight bool
	errMsg      string
}

func NewOpenDialog() *OpenDialog {
	return &OpenDialog{}
}

func (d *OpenDialog) Open(leftPath, rightPath string) {
	d.visible = true
	d.left.set(leftPath)
	d.right.set(rightPath)
	d.activeRight = false
	d.errMsg = ""
}

func (d *OpenDialog) Close() {
	d.visible = false
	d.errMsg = ""
}

func (d *OpenDialog) IsOpen() bool { return d.visible }

func (d *OpenDialog) SetError(msg string) { d.errMsg = msg }

func (d *OpenDialog) Values() (left, right string) { return d.left.String(), d.right.String() }

func (d *OpenDialog) HandleKey(msg tea.KeyMsg) {
	switch msg.String() {
	case "tab", "shift+tab", "up", "down":
		d.activeRight = !d.activeRight
		d.errMsg = ""
		return
	}
	if d.activeRight {
		d.right.handleKey(msg)
	} else {
		d.left.handleKey(msg)
	}
}

func (d *OpenDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("7"))
	activeLabelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Bold(true)
	hintStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	field := func(label string, ed *lineEditor, active bool) string {
		if !active {
			text := ed.String()
			if text == "" {
				text = hintStyle.Render("(empty)")
			}
			return labelStyle.Render(label) + "\n" + text
		}
		return activeLabelStyle.Render(label) + "\n" + ed.render()
	}

	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Render("Base URL"))
	sb.WriteString("\n\n")
	sb.WriteString(field("Left:", &d.left, !d.activeRight))
	sb.WriteString("\n\n")
	sb.WriteString(field("Right:", &d.right, d.activeRight))
	if d.errMsg != "" {
		sb.WriteString("\n\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(d.errMsg))
	}
	sb.WriteString("\n\n")
	sb.WriteString(hintStyle.Render("/local/path  sftp://  ssh://  ftp[s|es]://  rsync[+ssh]://  webdav[s]://  restic[s]://  rclone://"))
	sb.WriteString("\n\n")
	sb.WriteString(hintStyle.Render("Enter=open  Esc=cancel"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(1, 2)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, style.Render(sb.String()))
}
