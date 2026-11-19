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

const (
	// nowSpan is how far back a speed now looks. The files in flight and the
	// total look back the same span, so while the same files run, the total
	// is the sum of their speeds.
	nowSpan = 5 * time.Second
	// etaSpan is the memory of the ETA's speed: the speed of etaSpan ago
	// counts e times less than the speed now. After a change of speed, the
	// ETA is right in about five minutes; a stall moves it by a few
	// percent, not by hours.
	etaSpan = 2 * time.Minute
	// etaEase is how fast the ETA shown closes its gap to the remaining
	// bytes over the ETA's speed: by 1-1/e of it every etaEase.
	etaEase = 30 * time.Second
)

// speedNow is the speed of one byte counter now: what it moved in the last
// nowSpan, or since start while it is younger. The counter is 0 at start.
type speedNow struct {
	start time.Time
	at    []time.Time // samples, oldest first
	bytes []int64
	rate  float64
}

// add samples the counter, b, at now and sets rate. It returns what the
// counter moved since the sample before, or since start, and in what time.
func (s *speedNow) add(now time.Time, b int64) (int64, time.Duration) {
	prevAt, prevB := s.start, int64(0)
	if n := len(s.at); n > 0 {
		prevAt, prevB = s.at[n-1], s.bytes[n-1]
	}
	s.at, s.bytes = append(s.at, now), append(s.bytes, b)
	fromAt, fromB := s.start, int64(0)
	for i := len(s.at) - 1; i >= 0; i-- {
		if now.Sub(s.at[i]) >= nowSpan {
			fromAt, fromB = s.at[i], s.bytes[i]
			s.at, s.bytes = s.at[i:], s.bytes[i:]
			break
		}
	}
	s.rate = 0
	if dt := now.Sub(fromAt); dt >= 100*time.Millisecond {
		s.rate = max(float64(b-fromB), 0) / dt.Seconds()
	}
	return b - prevB, now.Sub(prevAt)
}

// etaSpeed is the speed the ETA uses: bytes over time, both made e times
// smaller every etaSpan, so the last minute counts most and a burst little.
type etaSpeed struct{ bytes, secs float64 }

func (e *etaSpeed) add(b int64, dt time.Duration) {
	k := math.Exp(-dt.Seconds() / etaSpan.Seconds())
	e.bytes = e.bytes*k + float64(b)
	e.secs = e.secs*k + dt.Seconds()
}

func (e *etaSpeed) rate() float64 {
	if e.secs <= 0 {
		return 0
	}
	return max(e.bytes/e.secs, 0)
}

// etaClock is the ETA the popup shows. The remaining bytes over etaSpeed
// still jump by seconds or minutes from one tick to the next on a copy of
// hours, as a counter moves in steps of a buffer. So the ETA shown counts
// down with the clock, and each tick closes part of its gap to that ETA.
type etaClock struct {
	speed etaSpeed
	secs  float64   // the ETA shown; 0: none
	at    time.Time // when secs was set; zero: no ETA yet
}

// add folds a tick's bytes (moved in dt) into the speed and moves the ETA
// shown. moving is false when nothing moved in the last nowSpan: the popup
// shows no ETA, and the stall's time stays out of the speed, so the ETA
// after it is the ETA before it. age is the time since the subtree was
// listed: while it is short, the speed is not settled yet and the ETA
// shown follows faster.
func (e *etaClock) add(now time.Time, moved int64, dt time.Duration, remaining int64, age time.Duration, moving bool) {
	if !moving {
		return
	}
	e.speed.add(moved, dt)
	rate := e.speed.rate()
	switch {
	case rate <= 0:
		return
	case remaining <= 0:
		e.secs, e.at = 0, now
		return
	}
	target := float64(remaining) / rate
	if e.at.IsZero() || e.secs <= 0 {
		e.secs, e.at = target, now
		return
	}
	step := now.Sub(e.at).Seconds()
	shown := e.secs - step
	k := 1.0
	if tau := min(etaEase, age/3).Seconds(); tau > 0 {
		k = -math.Expm1(-step / tau)
	}
	e.secs, e.at = max(shown+(target-shown)*k, 0), now
}

// formatETASecs is an ETA in seconds as h:mm:ss, or --:-- for none.
func formatETASecs(secs float64) string {
	if secs <= 0 || secs >= 1e12 {
		return "--:--"
	}
	return formatHMS(int64(secs + 0.5))
}

// speedRow is the popup's throughput chart, indented under the total bar,
// with the copy's average speed and the y ceiling on the right. A row with
// no room for the average next to a chart 10 cells wide shows the ceiling
// only.
func speedRow(rates []float64, peak, avg float64, inner int) string {
	var parts []string
	if avg > 0 {
		parts = append(parts, "avg "+model.FormatRate(avg))
	}
	if peak > 0 {
		parts = append(parts, "max "+model.FormatRate(peak))
	}
	// The label is 28 cells, as " avg 5.6 MB/s  max 15.2 MB/s", where that
	// leaves the chart 10 cells, else 15, as " max 15.2 MB/s". The choice
	// depends on the width only, so the chart holds still while the numbers
	// change.
	label := fmt.Sprintf(" %27s", strings.Join(parts, "  "))
	if inner-4-28 < 10 {
		label = ""
		if peak > 0 {
			label = "max " + model.FormatRate(peak)
		}
		label = fmt.Sprintf(" %14s", label)
	}
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
