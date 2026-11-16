package ui

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
	"sc/transfer"
)

// parallelRecorder is a backend that remembers the pool size it was given.
type parallelRecorder struct {
	model.Backend
	n int
}

func (b *parallelRecorder) SetParallel(n int) { b.n = n }

func recordedModel(t *testing.T) (m *Model, left, right *parallelRecorder) {
	t.Helper()
	left = &parallelRecorder{Backend: openBackendOrSkip(t, "fake://tiny")}
	right = &parallelRecorder{Backend: openBackendOrSkip(t, "fake://tiny")}
	return newTestModel(t, left, right), left, right // starts at 2
}

// + goes past the start value, - goes down to 1, and both reach the running
// copy's semaphore and both sides' connection pools.
func TestPlusMinusChangeFilesInFlight(t *testing.T) {
	m, left, right := recordedModel(t)
	sem := transfer.NewDynSem(2)
	m.copyProgress.Reset(2, true)
	m.copyProgress.Sem.Store(sem)
	runningOp(m, opCopy, nil)

	for range 3 {
		press(m, "+")
	}
	if m.copyParallel != 5 || m.copyProgress.Parallel.Load() != 5 {
		t.Fatalf("after +++ from 2: copyParallel %d, popup %d, want 5", m.copyParallel, m.copyProgress.Parallel.Load())
	}
	if left.n != 5 || right.n != 5 {
		t.Fatalf("connection pools sized %d/%d, want 5", left.n, right.n)
	}
	for i := range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := sem.Acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("file %d of 5 could not start", i+1)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if sem.Acquire(ctx) == nil {
		t.Fatal("a 6th file started with 5 allowed")
	}

	for range 10 {
		press(m, "-")
	}
	if m.copyParallel != 1 || left.n != 1 {
		t.Fatalf("- stopped at %d (pool %d), want 1", m.copyParallel, left.n)
	}
	for range 2 * maxCopyParallel {
		press(m, "+")
	}
	if m.copyParallel != maxCopyParallel {
		t.Fatalf("+ stopped at %d, want %d", m.copyParallel, maxCopyParallel)
	}

	m.copyProgress.Batched.Store(true)
	press(m, "-")
	if m.copyParallel != maxCopyParallel {
		t.Fatal("- changed a batch transfer, which is one session")
	}
}

// The settings dialog goes past the start value too, and the next copy sizes
// the connection pools from it.
func TestSettingsParallelReachesPoolsAtNextCopy(t *testing.T) {
	m, left, right := recordedModel(t)
	m.scanner.Scan(context.Background(), *m.cmpOpts)

	press(m, "=")
	for i := 0; i < len(m.settings.options) && m.settings.options[m.settings.cursor].Label != "Parallel copies"; i++ {
		press(m, "down")
	}
	for range 5 {
		press(m, "+")
	}
	press(m, "esc")
	if m.copyParallel != 7 {
		t.Fatalf("settings left parallel copies at %d, want 7", m.copyParallel)
	}

	file := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return !n.IsDir })
	if file == nil {
		t.Fatal("fake tree has no file")
	}
	if _, ok := runFirst(m.copyNode(file, true, copyChoices{})).(copyDoneMsg); !ok {
		t.Fatal("copy did not finish")
	}
	if left.n != 7 || right.n != 7 {
		t.Fatalf("connection pools sized %d/%d at copy start, want 7", left.n, right.n)
	}
}

func TestCopyPopupShowsFilesInFlightAndKeys(t *testing.T) {
	d := CopyPopupData{LeftToRight: true, TotalFiles: 9, InFlight: 1, Parallel: 1}
	wantAll(t, "copy popup", ansi.Strip(RenderCopyPopup(d, popupWidth(80))), "[1/1 in flight]", "X=cancel  +/-=more/fewer in flight")

	d.InFlight, d.Parallel = 5, 3
	wantAll(t, "popup after -", ansi.Strip(RenderCopyPopup(d, popupWidth(80))), "[5→3 in flight]")

	d.Batched = true
	if s := ansi.Strip(RenderCopyPopup(d, popupWidth(80))); strings.Contains(s, "+/-") || strings.Contains(s, "in flight") {
		t.Fatalf("batch popup offers +/-:\n%s", s)
	}
}

// With more files in flight than the screen has rows, the popup lists what
// fits and counts the rest, so the totals and keys stay on screen.
func TestCopyPopupFitsManyFilesInFlight(t *testing.T) {
	d := CopyPopupData{LeftToRight: true, TotalFiles: 100, InFlight: 40, Parallel: 40, MaxSlotRows: 5}
	for i := range 40 {
		d.Slots = append(d.Slots, CopySlotView{File: fmt.Sprintf("file%02d.bin", i), Size: 100})
	}
	s := ansi.Strip(RenderCopyPopup(d, popupWidth(80)))
	wantAll(t, "copy popup", s, "file03.bin", "… 36 more", "Total:", "X=cancel")
	if strings.Contains(s, "file04.bin") {
		t.Fatalf("popup shows more file rows than allowed:\n%s", s)
	}
	if got, want := strings.Count(s, "\n")+1, d.MaxSlotRows+copyPopupFixedRows-2; got != want {
		t.Fatalf("popup is %d lines, want %d:\n%s", got, want, s)
	}
}
