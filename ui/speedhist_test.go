package ui

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"

	"sc/model"
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

const (
	mib     = 1 << 20
	tick100 = 100 * time.Millisecond // the UI's tick
)

// near reports whether got is within frac of want.
func near(got, want, frac float64) bool { return math.Abs(got-want) <= frac*math.Abs(want) }

// A counter younger than nowSpan has its speed since start; an older one
// the speed of its last nowSpan; one that moved nothing for nowSpan, none.
func TestSpeedNowLooksBackOverNowSpan(t *testing.T) {
	t0 := time.Unix(1000, 0)
	s, at, b := speedNow{start: t0}, t0, 0.0
	run := func(d time.Duration, rate float64) {
		for range int(d / tick100) {
			at, b = at.Add(tick100), b+rate/10
			s.add(at, int64(b))
		}
	}
	run(2*time.Second, 3*mib)
	if !near(s.rate, 3*mib, 0.01) {
		t.Fatalf("2 s at 3 MB/s: %s", model.FormatRate(s.rate))
	}
	run(time.Minute, mib)
	run(10*time.Second, 4*mib)
	if !near(s.rate, 4*mib, 0.01) {
		t.Fatalf("a minute at 1 MB/s, then 10 s at 4 MB/s: %s", model.FormatRate(s.rate))
	}
	if n := len(s.at); n > int(nowSpan/tick100)+2 {
		t.Fatalf("keeps %d samples for a span of %d ticks", n, nowSpan/tick100)
	}
	run(nowSpan, 0)
	if s.rate != 0 {
		t.Fatalf("nothing for nowSpan: %s", model.FormatRate(s.rate))
	}
}

// The question that started this: seven files at 1.2 to 2.2 MB/s over a
// Total of 5.6 MB/s, the average of hours. Sampled at the same ticks, the
// total's speed now is the sum of the files' speeds now.
func TestSpeedNowOfTheTotalIsTheSumOfTheFiles(t *testing.T) {
	t0 := time.Unix(1000, 0)
	total, at, done := speedNow{start: t0}, t0, 0.0
	for range int(10 * time.Minute / tick100) { // file after file at 1 MB/s
		at, done = at.Add(tick100), done+mib/10
		total.add(at, int64(done))
	}
	speeds := []float64{1.2, 1.7, 1.7, 1.8, 2.1, 1.5, 2.2}
	files, moved := make([]speedNow, len(speeds)), make([]float64, len(speeds))
	for i := range files {
		files[i].start = at
	}
	for range int(time.Minute / tick100) {
		at = at.Add(tick100)
		sum := done
		for i, v := range speeds {
			moved[i] += v * mib / 10
			files[i].add(at, int64(moved[i]))
			sum += moved[i]
		}
		total.add(at, int64(sum))
	}
	var sum float64
	for i, f := range files {
		if !near(f.rate, speeds[i]*mib, 0.001) {
			t.Errorf("file %d: %s, want %.1f MB/s", i, model.FormatRate(f.rate), speeds[i])
		}
		sum += f.rate
	}
	if got := model.FormatRate(total.rate); got != "12.2 MB/s" || !near(total.rate, sum, 0.001) {
		t.Fatalf("the total's speed now is %s, the files' sum %s", got, model.FormatRate(sum))
	}
}

// etaRun drives an etaClock as tickSpeedNow does. The counter moves in
// steps of 1 MiB: the worst a transport's buffer makes it.
type etaRun struct {
	t0, at         time.Time
	now            speedNow
	eta            etaClock
	total, done, n float64
}

func newETARun(total float64) *etaRun {
	t0 := time.Unix(1000, 0)
	return &etaRun{t0: t0, at: t0, now: speedNow{start: t0}, total: total}
}

func (r *etaRun) run(d time.Duration, rate float64, each func()) {
	for range int(d / tick100) {
		r.at, r.n = r.at.Add(tick100), r.n+rate/10
		for r.n >= mib {
			r.n, r.done = r.n-mib, r.done+mib
		}
		moved, dt := r.now.add(r.at, int64(r.done))
		r.eta.add(r.at, moved, dt, int64(r.total-r.done), r.at.Sub(r.t0), r.now.rate > 0)
		if each != nil {
			each()
		}
	}
}

// truth is the ETA at rate.
func (r *etaRun) truth(rate float64) float64 { return (r.total - r.done) / rate }

// The ETA counts down with the clock while the counter moves in steps, is
// right within minutes of a change of speed, and a stall of a minute moves
// it by a few percent, not by hours.
func TestETAClockCountsDownFollowsAndHolds(t *testing.T) {
	r := newETARun(200 << 30)
	r.run(10*time.Minute, 5.6*mib, nil)
	if want := r.truth(5.6 * mib); !near(r.eta.secs, want, 0.02) {
		t.Fatalf("10 min at 5.6 MB/s: ETA %s, want about %s", formatETASecs(r.eta.secs), formatETASecs(want))
	}
	prev := r.eta.secs
	r.run(time.Minute, 5.6*mib, func() {
		if jump := math.Abs(prev - tick100.Seconds() - r.eta.secs); jump > 1 {
			t.Fatalf("the ETA jumped %.1f s from one tick to the next, to %s", jump, formatETASecs(r.eta.secs))
		}
		prev = r.eta.secs
	})
	r.run(8*time.Minute, 12.2*mib, nil)
	if want := r.truth(12.2 * mib); !near(r.eta.secs, want, 0.03) {
		t.Fatalf("8 min at 12.2 MB/s: ETA %s, want about %s", formatETASecs(r.eta.secs), formatETASecs(want))
	}
	r.run(time.Minute, 0, nil)
	if r.now.rate != 0 {
		t.Fatalf("a minute with nothing moved: speed now %s", model.FormatRate(r.now.rate))
	}
	worst := 0.0
	r.run(3*time.Minute, 12.2*mib, func() { worst = max(worst, r.eta.secs/r.truth(12.2*mib)-1) })
	if worst > 0.05 {
		t.Fatalf("after a stall of a minute the ETA was %.0f%% too long", worst*100)
	}
}

// The chart's row shows the copy's average next to the ceiling; a row too
// narrow for both keeps the ceiling.
func TestSpeedRowShowsTheAverage(t *testing.T) {
	rates := []float64{5 * mib, 12 * mib}
	if row := speedRow(rates, 14*mib, 5.6*mib, 56); !strings.HasSuffix(row, "avg 5.6 MB/s  max 14.0 MB/s") || lipgloss.Width(row) != 56 {
		t.Fatalf("wide row %q (%d cells)", row, lipgloss.Width(row))
	}
	if row := speedRow(rates, 14*mib, 5.6*mib, 32); strings.Contains(row, "avg") || !strings.HasSuffix(row, "max 14.0 MB/s") {
		t.Fatalf("narrow row %q", row)
	}
}
