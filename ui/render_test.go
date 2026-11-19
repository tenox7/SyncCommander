package ui

import (
	"strconv"
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
		Slots: []CopySlotView{{File: "a.bin", Size: 100, Bytes: 25, Rate: 25, Spinner: "⠹"}},
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

// The total's rate is at the right edge, under the rates of the files in
// flight: it stood after the sizes, out of their column. A batch's file
// rate is above it the same way. A narrow popup cuts the sizes, not the
// rate.
func TestCopyPopupLinesTheRatesUp(t *testing.T) {
	d := CopyPopupData{LeftToRight: true, DoneFiles: 1, TotalFiles: 4, InFlight: 1, Parallel: 2,
		BytesCopied: 3 << 20, TotalBytes: 200 << 30, TotalElapsed: 2 * time.Second, Rate: 1.5 * (1 << 20),
		Slots: []CopySlotView{{File: "IMG_2268 1.JPG", Size: 4 << 20, Bytes: 2 << 20, Rate: 2 << 20}}}
	// end is the column where rate ends on the line that holds row.
	end := func(s, row, rate string) int {
		t.Helper()
		for _, l := range strings.Split(s, "\n") {
			if !strings.Contains(l, row) {
				continue
			}
			i := strings.LastIndex(l, rate)
			if i < 0 {
				t.Fatalf("the %s line lacks %q: %q", row, rate, l)
			}
			return ansi.StringWidth(l[:i+len(rate)])
		}
		t.Fatalf("no %s line:\n%s", row, s)
		return 0
	}
	s := ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	if file, total := end(s, "IMG_2268", "2.0 MB/s"), end(s, "Total:", "1.5 MB/s"); file != total {
		t.Errorf("the total rate ends at column %d, the file's at %d:\n%s", total, file, s)
	}
	// A batch sends one file at a time: the file's speed is the session's.
	d.Batched, d.BatchFile, d.BatchFileBytes, d.BatchFileSize = true, "big.iso", 1<<20, 4<<20
	s = ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	if file, total := end(s, "File:", "1.5 MB/s"), end(s, "Total:", "1.5 MB/s"); file != total {
		t.Errorf("batch: the total rate ends at column %d, the file's at %d:\n%s", total, file, s)
	}
	d.Batched = false
	end(ansi.Strip(RenderCopyPopup(d, popupWidth(30))), "Total", "1.5 MB/s")
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

// lineWith is the first line of s that holds sub.
func lineWith(t *testing.T, s, sub string) string {
	t.Helper()
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			return l
		}
	}
	t.Fatalf("no line with %q:\n%s", sub, s)
	return ""
}

// etaSecs reads the ETA on the Total line of a popup: m:ss or h:mm:ss.
func etaSecs(t *testing.T, s string) float64 {
	t.Helper()
	f := strings.Fields(lineWith(t, s, "Total:"))
	for i := range len(f) - 1 {
		if f[i] != "ETA" {
			continue
		}
		secs := 0
		for _, p := range strings.Split(f[i+1], ":") {
			n, err := strconv.Atoi(p)
			if err != nil {
				t.Fatalf("ETA %q", f[i+1])
			}
			secs = secs*60 + n
		}
		return float64(secs)
	}
	t.Fatalf("no ETA in %q", f)
	return 0
}

// The screenshot that asked: files at 1.2 to 2.2 MB/s over a Total of
// 5.6 MB/s and an ETA of 9:20:23, both from the average of four hours. Now
// the Total is the speed now, the ETA is the one the clock keeps, and the
// average is labelled avg on the chart's row. A copy that moved nothing in
// the last nowSpan shows no speed and no ETA.
func TestCopyPopupTotalIsTheSpeedNow(t *testing.T) {
	d := CopyPopupData{LeftToRight: true, DoneFiles: 120, TotalFiles: 400, InFlight: 7, Parallel: 7,
		BytesCopied: 80 << 30, TotalBytes: 268 << 30, TotalElapsed: 4 * time.Hour,
		Rate: 12.2 * mib, ETA: 15779, Speed: []float64{5 * mib, 12 * mib}, SpeedPeak: 14 * mib}
	for _, r := range []float64{1.2, 1.7, 1.7, 1.8, 2.1, 1.5, 2.2} {
		d.Slots = append(d.Slots, CopySlotView{File: "IMG_2268.JPG", Size: 4 * mib, Bytes: 2 * mib, Rate: r * mib})
	}
	s := ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	if total := lineWith(t, s, "Total:"); !strings.Contains(total, "ETA 4:22:59") || !strings.Contains(total, "12.2 MB/s") {
		t.Fatalf("Total line %q", total)
	}
	wantAll(t, "copy popup", s, "avg 5.7 MB/s  max 14.0 MB/s")
	d.Rate = 0
	s = ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	if total := lineWith(t, s, "Total:"); !strings.Contains(total, "ETA --:--") || !strings.Contains(total, "— B/s") {
		t.Fatalf("stalled: Total line %q", total)
	}
}

// The status bar shows the copy's speed now, as the popup's Total does.
func TestStatusBarShowsTheSpeedNow(t *testing.T) {
	s := ansi.Strip(RenderStatusBar(StatusInfo{State: "COPY", FilesDone: 3, FilesTotal: 9, BytesCopied: 1 << 30, Rate: 12.2 * mib}, 160))
	if !strings.Contains(s, "COPY: 3/9 files") || !strings.Contains(s, " 12.2 MB/s") {
		t.Fatalf("status bar: %s", s)
	}
}

// From the ticks of a copy: the popup's Total and the status bar show the
// speed now, the ETA follows that speed and not the average, and the
// average is on the chart's row. Then nothing moves: no speed, no ETA.
func TestCopyTicksShowTheSpeedNow(t *testing.T) {
	src := openBackendOrSkip(t, "fake://tiny")
	m := newTestModel(t, src, src)
	p := m.copyProgress
	p.Reset(2, true)
	runningOp(m, opCopy, nil)
	t0 := time.Now().Add(-15 * time.Minute)
	p.Start.Store(t0.UnixNano())
	p.TotalBytes.Store(10 << 30)
	at, b := t0, 0.0
	run := func(d time.Duration, rate float64) {
		for range int(d / tick100) {
			at, b = at.Add(tick100), b+rate/10
			p.CompletedBytes.Store(int64(b))
			m.tickCopyAt(at)
		}
	}
	run(5*time.Minute, mib)
	run(10*time.Minute, 7*mib)
	s := ansi.Strip(m.copyPopup())
	if total := lineWith(t, s, "Total:"); !strings.Contains(total, "7.0 MB/s") {
		t.Fatalf("Total line %q", total)
	}
	wantAll(t, "copy popup", s, "avg 5.0 MB/s")
	remaining := float64(10<<30) - b
	if eta, want := etaSecs(t, s), remaining/(7*mib); !near(eta, want, 0.03) {
		t.Fatalf("ETA %s, want about %s, not the average's %s", formatETASecs(eta), formatETASecs(want), formatETASecs(remaining/(5*mib)))
	}
	if bar := ansi.Strip(RenderStatusBar(m.buildStatus(m.scanner.Progress()), 160)); !strings.Contains(bar, " 7.0 MB/s") {
		t.Fatalf("status bar: %s", bar)
	}
	run(nowSpan, 0)
	s = ansi.Strip(m.copyPopup())
	if total := lineWith(t, s, "Total:"); !strings.Contains(total, "ETA --:--") || !strings.Contains(total, "— B/s") {
		t.Fatalf("stalled: Total line %q", total)
	}
}
