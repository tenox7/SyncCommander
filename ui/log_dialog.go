package ui

import (
	"fmt"
	"os"
	"path/filepath"
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
	spinner       string
	status        string
	statusAt      time.Time
}

// NewLogDialog starts on errors only: an error line in all lines went off
// the screen at once while a long copy or scan wrote more.
func NewLogDialog() *LogDialog {
	return &LogDialog{follow: true, errOnly: true}
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
	d.lastSeenErrs = problems()
	d.lastSeenFatal = transport.Log.FatalCount()
}

// AutoOpen pops the log on new errors or failures (errCount counts both),
// but not within five seconds of the user closing it; a fatal error always
// opens it. It opens on errors only, also when e had set all lines: the
// error that opened it went off the screen at once while a long copy or
// scan wrote more lines. A fatal error also sets an open log to errors
// only.
func (d *LogDialog) AutoOpen(errCount, fatalCount int) {
	if fatalCount > d.lastSeenFatal {
		d.lastSeenErrs = errCount
		d.lastSeenFatal = fatalCount
		d.openOnErrors()
		return
	}
	if d.visible || errCount <= d.lastSeenErrs {
		return
	}
	if !d.closedAt.IsZero() && time.Since(d.closedAt) < 5*time.Second {
		return
	}
	d.lastSeenErrs = errCount
	d.openOnErrors()
}

func (d *LogDialog) openOnErrors() {
	d.visible = true
	d.follow = true
	d.errOnly = true
}

// Export writes the whole log (every retained line, ignoring the errors-only
// filter) to a timestamped file in the working directory and reports the
// result in the dialog's status line.
func (d *LogDialog) Export() {
	name := fmt.Sprintf("sc-log-%s.log", time.Now().Format("20060102-150405"))
	lines := transport.Log.Lines()
	data := strings.Join(lines, "\n")
	if len(lines) > 0 {
		data += "\n"
	}
	if err := os.WriteFile(name, []byte(data), 0o644); err != nil {
		d.setStatus("export failed: " + err.Error())
		return
	}
	where := name
	if abs, err := filepath.Abs(name); err == nil {
		where = abs
	}
	d.setStatus(fmt.Sprintf("saved %d lines to %s", len(lines), where))
}

func (d *LogDialog) setStatus(s string) {
	d.status = s
	d.statusAt = time.Now()
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

var (
	errOnlyBadge  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("1"))
	allLinesBadge = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("4"))
)

func (d *LogDialog) View(width, height int) string {
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
	// The filter shows in both states, and e says what it changes to:
	// [errors only] showed only while on, so all lines had no sign.
	badge := allLinesBadge.Render(" ALL LINES ")
	eKey := "e=show errors only"
	if d.errOnly {
		badge = errOnlyBadge.Render(" ERRORS ONLY ")
		eKey = "e=show all lines"
	}

	var sb strings.Builder
	title := titleStyle.Render(fmt.Sprintf("%s Remote Log  %s  ", d.spinner, followMark)) + badge +
		titleStyle.Render("  "+plural(total, "line"))
	sb.WriteString(ansi.Truncate(title, contentWidth, ""))
	sb.WriteString("\n")
	sb.WriteString(dimStyle.Render(strings.Repeat("─", contentWidth)))
	sb.WriteString("\n")
	for _, line := range lines {
		sb.WriteString(ansi.Truncate(shown(line), contentWidth, ""))
		sb.WriteString("\n")
	}
	for i := len(lines); i < vh; i++ {
		sb.WriteString("\n")
	}
	sb.WriteString(dimStyle.Render(strings.Repeat("─", contentWidth)))
	sb.WriteString("\n")
	if d.status != "" && time.Since(d.statusAt) < 10*time.Second {
		sb.WriteString(titleStyle.Render(ansi.Truncate(shown(d.status), contentWidth, "")))
	} else {
		keys := "↑↓=scroll  PgUp/Dn=page  Home/End  " + eKey + "  s=save  Esc=close"
		sb.WriteString(dimStyle.Render(ansi.Truncate(keys, contentWidth, "")))
	}

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(0, 1).
		Width(contentWidth + 2)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, style.Render(sb.String()))
}
