package ui

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
	"sc/transfer"
)

// The copy popup gave a row to the files in flight only. A file that ended
// a moment before the next one took its slot took its row with it for a
// tick: the popup showed 7 rows under "8/8 in flight", and jumped by a row
// every few seconds. Each slot has its row in its place now; a slot with no
// file is a blank row.

func popupLines(d CopyPopupData) []string {
	return strings.Split(ansi.Strip(RenderCopyPopup(d, popupWidth(80))), "\n")
}

func TestCopyPopupHoldsItsHeightWithAFreeSlot(t *testing.T) {
	full := CopyPopupData{LeftToRight: true, TotalFiles: 100, InFlight: 8, Parallel: 8}
	for i := range 8 {
		full.Slots = append(full.Slots, CopySlotView{File: fmt.Sprintf("file%02d.bin", i), Size: 100, Bytes: 50})
	}
	want := len(popupLines(full))

	gap := full
	gap.Slots = slices.Clone(full.Slots)
	gap.Slots[6] = CopySlotView{}
	lines := popupLines(gap)
	if len(lines) != want {
		t.Fatalf("a free slot made the popup %d lines, want %d:\n%s", len(lines), want, strings.Join(lines, "\n"))
	}
	// The border, the header, then a row per slot: slot 6 is line 8.
	if row := strings.Trim(lines[8], "│ "); row != "" {
		t.Errorf("the free slot's row is %q, want it blank", row)
	}

	none := full
	none.Slots = make([]CopySlotView, 8)
	lines = popupLines(none)
	if len(lines) != want || !strings.Contains(lines[2], "(waiting…)") {
		t.Errorf("with no file in flight the popup is %d lines, want %d with (waiting…) first:\n%s", len(lines), want, strings.Join(lines, "\n"))
	}

	// On a short screen the last row counts the hidden files; with none
	// hidden it is blank, and still there.
	short := gap
	short.MaxSlotRows = 5
	want = len(popupLines(short))
	short.Slots = slices.Clone(gap.Slots)
	for i := 4; i < 8; i++ {
		short.Slots[i] = CopySlotView{}
	}
	lines = popupLines(short)
	// " more", as "… 3 more": the key line has "=more".
	if len(lines) != want || strings.Contains(strings.Join(lines, "\n"), " more") {
		t.Errorf("with the hidden rows free the popup is %d lines, want %d and no \"… N more\":\n%s", len(lines), want, strings.Join(lines, "\n"))
	}
}

// gatedCopies holds each copy until the test lets one go.
type gatedCopies struct {
	model.Backend
	entered chan string
	release chan struct{}
}

func (g gatedCopies) CopyFrom(ctx context.Context, rel string, r io.Reader, p model.PutParams) error {
	g.entered <- rel
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.Backend.CopyFrom(ctx, rel, r, p)
}

func TestCopyPopupKeepsItsRowsWhenAFileEnds(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	for i := range 8 {
		if err := os.WriteFile(filepath.Join(left, fmt.Sprintf("f%d.txt", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dst := gatedCopies{Backend: openBackendOrSkip(t, right), entered: make(chan string, 8), release: make(chan struct{})}
	m := newTestModel(t, openBackendOrSkip(t, left), dst)
	m.width, m.height = 100, 40
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	m.copyParallel = 8
	msgs := runCmd(m.runCopy(m.scanner.Tree(), true, transfer.Plan{}))
	for range 8 {
		select {
		case <-dst.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("8 files did not go in flight")
		}
	}
	rows := func(now time.Time) (lines, files int) {
		s := ansi.Strip(m.copyPopupAt(now))
		return strings.Count(s, "\n") + 1, strings.Count(s, ".txt")
	}
	lines, files := rows(time.Now())
	if files != 8 {
		t.Fatalf("%d files in the popup, want 8", files)
	}
	// One file ends, and no file is left to take its slot.
	dst.release <- struct{}{}
	for deadline := time.Now().Add(5 * time.Second); m.copyProgress.Done.Load() < 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the file let go did not end")
		}
	}
	if l, f := rows(time.Now()); l != lines || f != 8 {
		t.Errorf("as a file ended: %d lines and %d files, want %d and 8: its row went", l, f, lines)
	}
	if l, f := rows(time.Now().Add(2 * freeSlotHold)); l != lines || f != 7 {
		t.Errorf("a while after a file ended: %d lines and %d files, want %d and 7", l, f, lines)
	}
	close(dst.release)
	msg, done := waitFor[copyDoneMsg](msgs, 5*time.Second)
	if !done {
		t.Fatal("the copy did not end")
	}
	m.Update(msg)
}
