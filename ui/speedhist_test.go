package ui

import (
	"slices"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
)

func TestSpeedHistoryClosesBucketsAndTracksPeak(t *testing.T) {
	var h speedHistory
	for range 10 {
		h.add(100, 100*time.Millisecond)
	}
	if len(h.rates) != 1 || h.rates[0] != 1000 || h.peak != 1000 {
		t.Fatalf("after one second: rates=%v peak=%v", h.rates, h.peak)
	}
	h.add(5000, 500*time.Millisecond)
	if s := h.samples(); len(s) != 2 || s[1] != 1000 {
		t.Fatalf("open bucket should be clamped to the peak, got %v", s)
	}
}

func TestSpeedHistoryMergesWhenFull(t *testing.T) {
	var h speedHistory
	for i := range speedBuckets {
		h.add(int64(i), time.Second)
	}
	if len(h.rates) != speedBuckets/2 || h.span != 2*time.Second {
		t.Fatalf("merge left %d buckets at span %v", len(h.rates), h.span)
	}
	if h.rates[0] != 0.5 || h.rates[1] != 2.5 || h.peak != speedBuckets-1 {
		t.Fatalf("merged pairs wrong: %v... peak %v", h.rates[:2], h.peak)
	}
}

func TestResampleAveragesDown(t *testing.T) {
	if got := resample([]float64{1, 2, 3, 4}, 3); !slices.Equal(got, []float64{1, 2, 3.5}) {
		t.Fatalf("resample to 3 = %v", got)
	}
	if got := resample([]float64{1, 2}, 4); !slices.Equal(got, []float64{1, 2}) {
		t.Fatalf("short input should pass through, got %v", got)
	}
}

func TestSparklineFillsBrailleColumnsBottomUp(t *testing.T) {
	got := sparkline([]float64{0, 1, 2, 3, 4}, 4, 3)
	if want := "⢀⣴⡇"; got != want {
		t.Fatalf("sparkline = %q, want %q", got, want)
	}
	if w := lipgloss.Width(sparkline(nil, 0, 7)); w != 7 {
		t.Fatalf("empty sparkline is %d cells wide, want 7", w)
	}
}
