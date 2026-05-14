package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"sc/transport"
)

type LogDialog struct {
	visible       bool
	offset        int
	follow        bool
	errOnly       bool
	width         int
	height        int
	lastSeenErrs  int
	lastSeenFatal int
	closedAt      time.Time
}

func NewLogDialog() *LogDialog {
	return &LogDialog{follow: true}
}

func (d *LogDialog) Open() { d.visible = true; d.follow = true }

func (d *LogDialog) ToggleErrFilter() {
	d.errOnly = !d.errOnly
	d.offset = 0
	d.follow = true
}

func isErrLogLine(line string) bool {
	parts := strings.SplitN(line, " ", 4)
	if len(parts) < 3 {
		return false
	}
	return parts[2] == "ERR" || parts[2] == "FAIL" || parts[2] == "FATAL"
}

func (d *LogDialog) filteredLines() []string {
	lines := transport.Log.Lines()
	if !d.errOnly {
		return lines
	}
	out := lines[:0:0]
	for _, l := range lines {
		if isErrLogLine(l) {
			out = append(out, l)
		}
	}
	return out
}

func (d *LogDialog) filteredLen() int {
	if !d.errOnly {
		return transport.Log.Len()
	}
	return len(d.filteredLines())
}
func (d *LogDialog) Close() {
	d.visible = false
	d.closedAt = time.Now()
	d.lastSeenErrs = transport.Log.ErrCount()
	d.lastSeenFatal = transport.Log.FatalCount()
}

func (d *LogDialog) AutoOpen(errCount, fatalCount int) {
	if fatalCount > d.lastSeenFatal {
		d.lastSeenErrs = errCount
		d.lastSeenFatal = fatalCount
		d.visible = true
		d.follow = true
		d.errOnly = true
		return
	}
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
	lines := d.filteredLen()
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
	lines := d.filteredLen()
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

	lines := d.filteredLines()
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
	filterTag := ""
	if d.errOnly {
		filterTag = "  [errors only]"
	}
	sb.WriteString(titleStyle.Render(fmt.Sprintf("%s Remote Log  %s  (%d lines)%s", spinner, followMark, len(lines), filterTag)))
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
	sb.WriteString(dimStyle.Render("↑↓=scroll  PgUp/Dn=page  Home/End  e=errors  Esc=close"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(0, 1).
		Width(contentWidth + 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}
