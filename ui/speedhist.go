package ui

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"sc/model"
)

const (
	speedBuckets = 256
	speedSpan    = time.Second
)

// speedHistory keeps a transfer's whole throughput curve in a bounded number
// of equal-length buckets. When they run out, adjacent pairs merge and the
// bucket span doubles, so the chart compresses horizontally as time passes.
type speedHistory struct {
	rates []float64 // bytes/sec per closed bucket
	peak  float64   // highest closed bucket, the chart's y ceiling
	span  time.Duration
	bytes int64 // open bucket
	dur   time.Duration
}

// add folds one tick's bytes into the open bucket and closes it once it
// covers a full span.
func (h *speedHistory) add(bytes int64, dt time.Duration) {
	if h.span == 0 {
		h.span = speedSpan
	}
	h.bytes += bytes
	h.dur += dt
	if h.dur < h.span {
		return
	}
	rate := float64(h.bytes) / h.dur.Seconds()
	h.peak = max(h.peak, rate)
	h.rates = append(h.rates, rate)
	h.bytes, h.dur = 0, 0
	if len(h.rates) < speedBuckets {
		return
	}
	half := len(h.rates) / 2
	for i := range half {
		h.rates[i] = (h.rates[2*i] + h.rates[2*i+1]) / 2
	}
	h.rates = h.rates[:half]
	h.span *= 2
}

// samples is the closed buckets plus the open one, which is clamped to the
// peak so a half-second burst can't rescale the chart.
func (h *speedHistory) samples() []float64 {
	if h.dur <= 0 {
		return h.rates
	}
	cur := float64(h.bytes) / h.dur.Seconds()
	return append(h.rates[:len(h.rates):len(h.rates)], min(cur, h.peak))
}

// speedRow is the popup's throughput chart, indented under the total bar
// with the y ceiling labelled on the right.
func speedRow(rates []float64, peak float64, inner int) string {
	label := ""
	if peak > 0 {
		label = "max " + model.FormatRate(peak)
	}
	label = fmt.Sprintf(" %14s", label)
	return "    " + sparkline(rates, peak, max(inner-4-lipgloss.Width(label), 4)) + label
}

// Braille dots for a column filled from the bottom, by height 0..4.
var (
	brailleLeft  = [5]rune{0, 0x40, 0x44, 0x46, 0x47}
	brailleRight = [5]rune{0, 0x80, 0xA0, 0xB0, 0xB8}
)

// sparkline charts rates as filled braille columns, two per cell, scaled
// from zero to peak. The history is drawn one sample per column until it
// outgrows the width, then averaged down so the whole run always fits.
func sparkline(rates []float64, peak float64, cells int) string {
	cols := resample(rates, 2*cells)
	var sb strings.Builder
	for i := range cells {
		dots := brailleLeft[sparkLevel(cols, 2*i, peak)] | brailleRight[sparkLevel(cols, 2*i+1, peak)]
		if dots == 0 {
			sb.WriteByte(' ')
			continue
		}
		sb.WriteRune(0x2800 | dots)
	}
	return sb.String()
}

func sparkLevel(cols []float64, i int, peak float64) int {
	if i >= len(cols) || peak <= 0 {
		return 0
	}
	return min(int(math.Ceil(cols[i]/peak*4)), 4)
}

// resample averages v down to at most w columns.
func resample(v []float64, w int) []float64 {
	if len(v) <= w {
		return v
	}
	out := make([]float64, w)
	for c := range out {
		lo, hi := c*len(v)/w, (c+1)*len(v)/w
		sum := 0.0
		for _, x := range v[lo:hi] {
			sum += x
		}
		out[c] = sum / float64(hi-lo)
	}
	return out
}
