package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// S20: the copy popup counts the files copied without their modes beside
// those without their times, on a row of their own: on the header the two
// cut off the files in flight. It takes the count from the running copy,
// and fits an 80-column screen.
func TestCopyPopupSaysFilesWithoutModes(t *testing.T) {
	out := RenderCopyPopup(CopyPopupData{DoneFiles: 3, TotalFiles: 9, FailedFiles: 1, TimesNotSet: 2, ModesNotSet: 4, InFlight: 1, Parallel: 4, TotalElapsed: time.Second}, popupWidth(80))
	if !strings.Contains(out, "1 FAILED  [1/4 in flight]") || !strings.Contains(out, "│ 2 NO TIMES  4 NO MODES ") {
		t.Errorf("the popup does not count all three, or cuts the header:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if w := lipgloss.Width(line); w > 80 {
			t.Errorf("a line is %d wide on an 80-column screen: %q", w, line)
		}
	}
	out = RenderCopyPopup(CopyPopupData{DoneFiles: 3, TotalFiles: 9, ModesNotSet: 4, InFlight: 1, Parallel: 4, TotalElapsed: time.Second}, popupWidth(80))
	if !strings.Contains(out, "│ 4 NO MODES ") || strings.Contains(out, "NO TIMES") {
		t.Errorf("the popup does not count the modes alone:\n%s", out)
	}
	m, _, _ := recordedModel(t)
	m.copyProgress.Reset(2, true)
	m.copyProgress.ModesNotSet.Store(3)
	if out := m.copyPopup(); !strings.Contains(out, "3 NO MODES") {
		t.Errorf("the popup does not show the copy's count:\n%s", out)
	}
}
