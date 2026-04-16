package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type OpenDialog struct {
	visible     bool
	leftValue   string
	rightValue  string
	leftCursor  int
	rightCursor int
	activeRight bool
	errMsg      string
}

func NewOpenDialog() *OpenDialog {
	return &OpenDialog{}
}

func (d *OpenDialog) Open(leftPath, rightPath string) {
	d.visible = true
	d.leftValue = leftPath
	d.rightValue = rightPath
	d.leftCursor = len(leftPath)
	d.rightCursor = len(rightPath)
	d.activeRight = false
	d.errMsg = ""
}

func (d *OpenDialog) Close() {
	d.visible = false
	d.errMsg = ""
}

func (d *OpenDialog) IsOpen() bool {
	return d.visible
}

func (d *OpenDialog) SetError(msg string) {
	d.errMsg = msg
}

func (d *OpenDialog) HandleKey(key string) {
	var value *string
	var cursor *int
	if d.activeRight {
		value = &d.rightValue
		cursor = &d.rightCursor
	} else {
		value = &d.leftValue
		cursor = &d.leftCursor
	}

	switch key {
	case "tab", "shift+tab", "up", "down":
		d.activeRight = !d.activeRight
		d.errMsg = ""
		return
	case "left":
		if *cursor > 0 {
			(*cursor)--
		}
	case "right":
		if *cursor < len(*value) {
			(*cursor)++
		}
	case "home", "ctrl+a":
		*cursor = 0
	case "end", "ctrl+e":
		*cursor = len(*value)
	case "backspace":
		if *cursor > 0 {
			*value = (*value)[:*cursor-1] + (*value)[*cursor:]
			(*cursor)--
		}
	case "delete":
		if *cursor < len(*value) {
			*value = (*value)[:*cursor] + (*value)[*cursor+1:]
		}
	case "ctrl+u":
		*value = (*value)[*cursor:]
		*cursor = 0
	case "ctrl+k":
		*value = (*value)[:*cursor]
	default:
		if len(key) == 1 && key[0] >= 32 {
			*value = (*value)[:*cursor] + key + (*value)[*cursor:]
			(*cursor)++
		}
	}
}

func (d *OpenDialog) renderField(value string, cursor int, active bool) string {
	if !active {
		if value == "" {
			return lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("(empty)")
		}
		return value
	}
	before := value[:cursor]
	after := value[cursor:]
	cursorChar := " "
	if cursor < len(value) {
		cursorChar = string(value[cursor])
		after = value[cursor+1:]
	}
	cur := lipgloss.NewStyle().Reverse(true).Render(cursorChar)
	return fmt.Sprintf("%s%s%s", before, cur, after)
}

func (d *OpenDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Render("Base URL"))
	sb.WriteString("\n\n")

	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("7"))
	activeLabelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Bold(true)

	if !d.activeRight {
		sb.WriteString(activeLabelStyle.Render("Left:"))
	} else {
		sb.WriteString(labelStyle.Render("Left:"))
	}
	sb.WriteString("\n")
	sb.WriteString(d.renderField(d.leftValue, d.leftCursor, !d.activeRight))
	sb.WriteString("\n\n")

	if d.activeRight {
		sb.WriteString(activeLabelStyle.Render("Right:"))
	} else {
		sb.WriteString(labelStyle.Render("Right:"))
	}
	sb.WriteString("\n")
	sb.WriteString(d.renderField(d.rightValue, d.rightCursor, d.activeRight))

	if d.errMsg != "" {
		sb.WriteString("\n\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(d.errMsg))
	}

	sb.WriteString("\n\n")
	hintStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	sb.WriteString(hintStyle.Render("/local/path  sftp://  ssh://  ftp[s|es]://  rsync[+ssh]://"))
	sb.WriteString("\n\n")
	sb.WriteString(hintStyle.Render("Enter=open  Esc=cancel"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(1, 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
