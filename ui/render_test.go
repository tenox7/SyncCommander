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

func TestCopyPopupShowsFilesAndPercent(t *testing.T) {
	d := CopyPopupData{LeftToRight: true, DoneFiles: 1, TotalFiles: 4, BytesCopied: 50, TotalBytes: 200, Parallel: 2,
		Slots: []CopySlotView{{File: "a.bin", Size: 100, Bytes: 25, Elapsed: time.Second}}}
	s := ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	wantAll(t, "copy popup", s, "COPY  1/4 files", "a.bin", "25%")
	if w := maxLineWidth(s); w > 80 {
		t.Fatalf("copy popup is %d wide on an 80-column screen", w)
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
	wantAll(t, "left top bar", ansi.Strip(RenderPanelTopBar(stats, true, "", 60)), "+2d -2f +")
	wantAll(t, "right top bar", ansi.Strip(RenderPanelTopBar(stats, false, "", 60)), "-2d +2f -")
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
	if !strings.Contains(ansi.Strip(m.View()), m.leftPanel.title) {
		t.Fatal("main view lacks the panel title")
	}
}
