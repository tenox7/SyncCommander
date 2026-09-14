package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

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

// count and window read only the lines on screen; the log itself may hold
// a hundred thousand.
func (d *LogDialog) count() int {
	if d.errOnly {
		return transport.Log.ErrLen()
	}
	return transport.Log.Len()
}

func (d *LogDialog) window(from, to int) []string {
	if d.errOnly {
		return transport.Log.ErrSlice(from, to)
	}
	return transport.Log.Slice(from, to)
}

func (d *LogDialog) Close() {
	d.visible = false
	d.closedAt = time.Now()
	d.lastSeenErrs = transport.Log.ErrCount()
	d.lastSeenFatal = transport.Log.FatalCount()
}

// AutoOpen pops the log on new errors, but not within five seconds of the
// user closing it; a fatal error always opens it in errors-only mode.
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

func (d *LogDialog) viewHeight() int { return max(d.height-6, 1) }

func (d *LogDialog) scrollBy(delta int) {
	d.offset = max(d.offset+delta, 0)
	if delta < 0 {
		d.follow = false
		return
	}
	if end := max(d.count()-d.viewHeight(), 0); d.offset >= end {
		d.offset = end
		d.follow = true
	}
}

func (d *LogDialog) ScrollUp()   { d.scrollBy(-d.viewHeight() / 2) }
func (d *LogDialog) ScrollDown() { d.scrollBy(d.viewHeight() / 2) }
func (d *LogDialog) PageUp()     { d.scrollBy(-d.viewHeight()) }
func (d *LogDialog) PageDown()   { d.scrollBy(d.viewHeight()) }

func (d *LogDialog) Home() {
	d.follow = false
	d.offset = 0
}

func (d *LogDialog) End() { d.follow = true }

func (d *LogDialog) View(width, height int, spinner string) string {
	if !d.visible {
		return ""
	}
	d.width, d.height = width, height
	total := d.count()
	vh := d.viewHeight()
	contentWidth := max(width-6, 20)
	if d.follow {
		d.offset = max(total-vh, 0)
	}
	end := min(d.offset+vh, total)
	lines := d.window(d.offset, end)

	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	titleStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	followMark := " "
	if d.follow {
		followMark = "▼"
	}
	filterTag := ""
	if d.errOnly {
		filterTag = "  [errors only]"
	}

	var sb strings.Builder
	sb.WriteString(titleStyle.Render(fmt.Sprintf("%s Remote Log  %s  (%d lines)%s", spinner, followMark, total, filterTag)))
	sb.WriteString("\n")
	sb.WriteString(dimStyle.Render(strings.Repeat("─", contentWidth)))
	sb.WriteString("\n")
	for _, line := range lines {
		sb.WriteString(ansi.Truncate(line, contentWidth, ""))
		sb.WriteString("\n")
	}
	for i := len(lines); i < vh; i++ {
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
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, style.Render(sb.String()))
}
