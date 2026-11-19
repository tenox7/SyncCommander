package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

func maxLineWidth(s string) int {
	w := 0
	for _, l := range strings.Split(s, "\n") {
		w = max(w, lipgloss.Width(l))
	}
	return w
}

func wantAll(t *testing.T, what, s string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(s, w) {
			t.Fatalf("%s lacks %q:\n%s", what, w, s)
		}
	}
}

func TestStatusBarShowsScanProgress(t *testing.T) {
	info := StatusInfo{State: "DIR SCAN", DirsListed: 3, DirsTotal: 10, FilesScanned: 42, TotalSize: 1 << 20}
	s := ansi.Strip(RenderStatusBar(info, 120))
	wantAll(t, "status bar", s, "DIR SCAN", "3/10 dirs", "42 files")
	if w := maxLineWidth(s); w > 120 {
		t.Fatalf("status bar is %d wide on a 120-column screen", w)
	}
}

// U10: a scan has no popup to show its stop key, so the status bar shows
// it where it shows ?=help.
func TestStatusBarShowsTheStopKey(t *testing.T) {
	for state, want := range map[string]string{"DIR SCAN": "x=stop", "CHECKSUM": "x=stop", "RENAME": "x=stop", "TOUCH": "x=stop", "": "?=help", "COPY": "?=help"} {
		if s := ansi.Strip(RenderStatusBar(StatusInfo{State: state}, 120)); !strings.Contains(s, want) {
			t.Errorf("state %q: the status bar lacks %q:\n%s", state, want, s)
		}
	}
}

func TestCopyPopupShowsFilesAndPercent(t *testing.T) {
	d := CopyPopupData{LeftToRight: true, DoneFiles: 1, TotalFiles: 4, BytesCopied: 50, TotalBytes: 200, Parallel: 2,
		Slots: []CopySlotView{{File: "a.bin", Size: 100, Bytes: 25, Elapsed: time.Second, Spinner: "⠹"}},
		Speed: []float64{10, 20, 40}, SpeedPeak: 40}
	s := ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	wantAll(t, "copy popup", s, "COPY  1/4 files", "a.bin", "⠹", "25%", "25 B/s", "⣠⡇", "max 40 B/s")
	if strings.Contains(s, "…") {
		t.Fatalf("copy popup truncates a row that should fit:\n%s", s)
	}
	if w := maxLineWidth(s); w > 80 {
		t.Fatalf("copy popup is %d wide on an 80-column screen", w)
	}
}

func TestProgressBarFillsInEighths(t *testing.T) {
	for _, tc := range []struct {
		done int64
		want string
	}{{0, "░░"}, {1, "▁░"}, {4, "▄░"}, {8, "█░"}, {9, "█▁"}, {16, "██"}, {99, "██"}} {
		if got := progressBar(tc.done, 16, 2); got != tc.want {
			t.Errorf("progressBar(%d, 16, 2) = %q, want %q", tc.done, got, tc.want)
		}
	}
}

func TestDeletePopupClampsProgress(t *testing.T) {
	s := ansi.Strip(RenderDeletePopup("gone.txt", "right", 7, 0, 5, 90*time.Second, popupWidth(60)))
	wantAll(t, "delete popup", s, "DELETE right  5/5 items", "gone.txt", "100%", "Elapsed: 1:30")
	if w := maxLineWidth(s); w > 60 {
		t.Fatalf("delete popup is %d wide on a 60-column screen", w)
	}
}

func TestPanelTopBarDeltasFollowTheSide(t *testing.T) {
	stats := &TreeStats{LeftDirs: 3, RightDirs: 1, LeftFiles: 10, RightFiles: 12, LeftSize: 3 << 10, RightSize: 1 << 10}
	wantAll(t, "left top bar", ansi.Strip(RenderPanelTopBar(stats, true, true, "", 60)), "+2d -2f +2.0K")
	wantAll(t, "right top bar", ansi.Strip(RenderPanelTopBar(stats, false, true, "", 60)), "-2d +2f -2.0K")
}

// Absolute, each top bar shows what its own side holds, with no sign.
func TestPanelTopBarTotalsFollowTheSide(t *testing.T) {
	stats := &TreeStats{LeftDirs: 3, RightDirs: 1, LeftFiles: 10, RightFiles: 12, LeftSize: 3 << 10, RightSize: 1 << 10}
	for _, c := range []struct {
		left bool
		want string
	}{{true, "3d 10f 3.0K"}, {false, "1d 12f 1.0K"}} {
		bar := ansi.Strip(RenderPanelTopBar(stats, c.left, false, "", 60))
		if !strings.Contains(bar, c.want) || strings.ContainsAny(bar, "+-") {
			t.Errorf("left %v: the bar is %q, want %q and no sign", c.left, bar, c.want)
		}
	}
}

func TestDialogsRenderWithinTheScreen(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	for _, k := range []string{"?", "esc", "i", "esc", "~", "esc", "=", "esc", "y", "esc", "/", "esc"} {
		press(m, k)
	}
	if m.openModal() != nil {
		t.Fatalf("%T still open after esc", m.openModal())
	}
	m.help.Open()
	m.info.Open(*m.cachedStats, "L", "R", "sha1", nil, nil)
	m.logView.Open()
	m.confirm.Open("Sure?", []string{"one", "two"}, true)
	m.input.Open("Rename:", "name", nil)
	m.openDlg.Open("fake://a", "fake://b")
	m.settings.Open()
	for _, d := range []modal{m.help, m.info, m.logView, m.confirm, m.input, m.openDlg, m.settings} {
		v := d.View(80, 24)
		if v == "" {
			t.Fatalf("%T renders nothing", d)
		}
		if w := maxLineWidth(v); w > 80 {
			t.Fatalf("%T is %d wide on an 80-column screen", d, w)
		}
	}
}

func TestViewShowsOnlyTheOpenModal(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	m.help.Open()
	if got, want := m.View(), m.help.View(m.width, m.height); got != want {
		t.Fatal("View with help open is not the help view")
	}
	m.help.Close()
	if !strings.Contains(ansi.Strip(m.View()), m.left.panel.title) {
		t.Fatal("main view lacks the panel title")
	}
}

// F17: the copy popup says how many files went without their times, and
// still has room for the files in flight on an 80-column screen.
func TestCopyPopupSaysFilesWithoutTimes(t *testing.T) {
	out := RenderCopyPopup(CopyPopupData{DoneFiles: 3, TotalFiles: 9, FailedFiles: 1, TimesNotSet: 2, InFlight: 1, Parallel: 4, TotalElapsed: time.Second}, popupWidth(80))
	// The count has a row of its own since S20, beside the files without
	// their modes.
	if !strings.Contains(out, "1 FAILED  [1/4 in flight]") || !strings.Contains(out, "│ 2 NO TIMES ") {
		t.Errorf("the popup does not count both, or cuts the header:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if w := lipgloss.Width(line); w > 80 {
			t.Errorf("a line is %d wide on an 80-column screen: %q", w, line)
		}
	}
}

// The popup the model draws takes the count from the running copy.
func TestCopyPopupTakesFilesWithoutTimesFromTheCopy(t *testing.T) {
	m, _, _ := recordedModel(t)
	m.copyProgress.Reset(2, true)
	m.copyProgress.TimesNotSet.Store(3)
	if out := m.copyPopup(); !strings.Contains(out, "3 NO TIMES") {
		t.Errorf("the popup does not show the copy's count:\n%s", out)
	}
}
