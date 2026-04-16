package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"sc/transport"
)

type LogDialog struct {
	visible      bool
	offset       int
	follow       bool
	width        int
	height       int
	lastSeenErrs int
	closedAt     time.Time
}

func NewLogDialog() *LogDialog {
	return &LogDialog{follow: true}
}

func (d *LogDialog) Open()  { d.visible = true; d.follow = true }
func (d *LogDialog) Close() {
	d.visible = false
	d.closedAt = time.Now()
	d.lastSeenErrs = transport.Log.ErrCount()
}

func (d *LogDialog) AutoOpen(errCount int) {
	if d.visible || errCount <= d.lastSeenErrs {
		return
	}
	if !d.closedAt.IsZero() && time.Since(d.closedAt) < 5*time.Second {
		return
	}
	d.lastSeenErrs = errCount
	d.visible = true
	d.follow = true
}
func (d *LogDialog) IsOpen() bool { return d.visible }

func (d *LogDialog) viewHeight() int {
	h := d.height - 6
	if h < 1 {
		h = 1
	}
	return h
}

func (d *LogDialog) ScrollUp() {
	d.follow = false
	d.offset -= d.viewHeight() / 2
	if d.offset < 0 {
		d.offset = 0
	}
}

func (d *LogDialog) ScrollDown() {
	lines := transport.Log.Len()
	vh := d.viewHeight()
	d.offset += vh / 2
	max := lines - vh
	if max < 0 {
		max = 0
	}
	if d.offset >= max {
		d.offset = max
		d.follow = true
	}
}

func (d *LogDialog) PageUp() {
	d.follow = false
	d.offset -= d.viewHeight()
	if d.offset < 0 {
		d.offset = 0
	}
}

func (d *LogDialog) PageDown() {
	lines := transport.Log.Len()
	vh := d.viewHeight()
	d.offset += vh
	max := lines - vh
	if max < 0 {
		max = 0
	}
	if d.offset >= max {
		d.offset = max
		d.follow = true
	}
}

func (d *LogDialog) Home() {
	d.follow = false
	d.offset = 0
}

func (d *LogDialog) End() {
	d.follow = true
}

func (d *LogDialog) View(width, height int, spinner string) string {
	if !d.visible {
		return ""
	}
	d.width = width
	d.height = height

	lines := transport.Log.Lines()
	vh := d.viewHeight()
	contentWidth := width - 6
	if contentWidth < 20 {
		contentWidth = 20
	}

	if d.follow {
		d.offset = len(lines) - vh
		if d.offset < 0 {
			d.offset = 0
		}
	}

	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	titleStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("4"))

	var sb strings.Builder
	followMark := " "
	if d.follow {
		followMark = "▼"
	}
	sb.WriteString(titleStyle.Render(fmt.Sprintf("%s Remote Log  %s  (%d lines)", spinner, followMark, len(lines))))
	sb.WriteString("\n")
	sb.WriteString(dimStyle.Render(strings.Repeat("─", contentWidth)))
	sb.WriteString("\n")

	end := d.offset + vh
	if end > len(lines) {
		end = len(lines)
	}
	for i := d.offset; i < end; i++ {
		line := lines[i]
		if len(line) > contentWidth {
			line = line[:contentWidth]
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	for i := end - d.offset; i < vh; i++ {
		sb.WriteString("\n")
	}

	sb.WriteString(dimStyle.Render(strings.Repeat("─", contentWidth)))
	sb.WriteString("\n")
	sb.WriteString(dimStyle.Render("↑↓=scroll  PgUp/Dn=page  Home/End  Esc=close"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(0, 1).
		Width(contentWidth + 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
