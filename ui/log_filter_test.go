package ui

import (
	"fmt"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"sc/transport"
)

// An error opened the log on all lines, and the lines of a long copy or
// scan pushed the error off the screen at once. Now an error opens the log
// on errors only, also after e had set all lines, and the error stays.
func TestAnErrorOpensTheLogOnErrorsOnly(t *testing.T) {
	m := dialogModel(t)
	m.logView.errOnly = false // all lines, as e sets them
	errorOnTick(m, transport.DirErr, "an error while a scan runs")
	for i := range 500 {
		transport.Log.Add("scan", transport.DirIn, fmt.Sprintf("a line the scan logged after the error, %d", i))
	}
	if !logOnTop(m) || !m.logView.errOnly {
		t.Errorf("the log on top %v, errors only %v; want both", logOnTop(m), m.logView.errOnly)
	}
	if v := ansi.Strip(m.View()); !strings.Contains(v, "an error while a scan runs") {
		t.Errorf("the error went off the screen:\n%s", v)
	}
}

// The log starts on errors only, and says which lines it shows in both
// states: [errors only] showed only while on, so all lines had no sign,
// and e=errors did not say what e changes to.
func TestTheLogSaysWhichLinesItShows(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	press(m, "~")
	for _, want := range []struct {
		errOnly    bool
		badge, key string
	}{
		{true, "ERRORS ONLY", "e=show all lines"},
		{false, "ALL LINES", "e=show errors only"},
		{true, "ERRORS ONLY", "e=show all lines"},
	} {
		v := ansi.Strip(m.logView.View(80, 24))
		if m.logView.errOnly != want.errOnly || !strings.Contains(v, want.badge) || !strings.Contains(v, want.key) {
			t.Errorf("errors only %v, want %v with %q and %q:\n%s", m.logView.errOnly, want.errOnly, want.badge, want.key, v)
		}
		press(m, "e")
	}
}

// The title and the key line are cut at the dialog's width: wrapped, they
// made the log one row taller than the screen.
func TestTheLogFitsANarrowScreen(t *testing.T) {
	d := NewLogDialog()
	d.Open()
	for _, errOnly := range []bool{true, false} {
		d.errOnly = errOnly
		v := d.View(50, 20)
		if rows := strings.Count(v, "\n") + 1; rows != 20 {
			t.Errorf("errors only %v: %d rows on a 20-row screen:\n%s", errOnly, rows, ansi.Strip(v))
		}
		if w := maxLineWidth(v); w > 50 {
			t.Errorf("errors only %v: %d wide on a 50-column screen", errOnly, w)
		}
	}
}
