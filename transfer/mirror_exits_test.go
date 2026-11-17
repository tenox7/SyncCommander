package transfer

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"sc/model"
	"sc/transport"
)

// T1: the mirror pass deletes, and four of its ways out had no test: a run
// canceled before it, a copy that stopped, a cancel between two deletes,
// and a folder that cannot be listed again. Each keeps what it did not get
// to delete.

// mirrorTopRow runs a mirror copy of the top row, left to right, on ctx and
// onto dst, as the user agreed to its plan. The plan must hold deletes.
func (s *scenario) mirrorTopRow(t *testing.T, ctx context.Context, dst model.Backend) *Progress {
	t.Helper()
	node := s.node(t, "")
	planned := PlanCopy(s.sc, node, true, scenarioOpts)
	if len(planned.Deletes) == 0 {
		t.Fatal("the plan holds no deletes")
	}
	p := &Progress{}
	Copy(ctx, Request{Src: s.lb, Dst: dst, Scanner: s.sc, Node: node, LeftToRight: true, Opts: scenarioOpts,
		Parallel: 1, Progress: p, Plan: Plan{Newer: planned.Newer, Deletes: planned.Deletes, Mirror: true}})
	return p
}

// cancelOnCopy cancels the run once a file is copied.
type cancelOnCopy struct {
	model.Backend
	cancel context.CancelFunc
}

func (b cancelOnCopy) CopyFrom(ctx context.Context, relPath string, r io.Reader, p model.PutParams) error {
	err := b.Backend.CopyFrom(ctx, relPath, r, p)
	b.cancel()
	return err
}

// fullDisk takes no file: the copy stops, as on a full destination.
type fullDisk struct{ model.Backend }

func (fullDisk) CopyFrom(context.Context, string, io.Reader, model.PutParams) error {
	return model.ErrNoSpace
}

// cancelOnRemove cancels the run once a file is deleted.
type cancelOnRemove struct {
	model.Backend
	cancel context.CancelFunc
}

func (b cancelOnRemove) Remove(ctx context.Context, relPath string) error {
	err := b.Backend.Remove(ctx, relPath)
	b.cancel()
	return err
}

// noFreshList fails the mirror's fresh listing of one folder.
type noFreshList struct {
	model.Backend
	dir string
}

func (b noFreshList) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if model.WantsFreshListing(ctx) && relDir == b.dir {
		return nil, errors.New("the server went away")
	}
	return b.Backend.List(ctx, relDir)
}

func TestScenarioMirrorSkippedWhenTheRunIsCanceled(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "new")
	writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
	s := newScenario(t, left, right)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mark := len(transport.Log.Lines())
	s.mirrorTopRow(t, ctx, cancelOnCopy{Backend: s.rb, cancel: cancel})
	if !logged(mark, "mirror delete skipped: copy canceled") {
		t.Error("the log does not say the mirror was skipped for the cancel")
	}
	if !onDisk(filepath.Join(right, "old.txt")) {
		t.Error("old.txt was deleted after the run was canceled")
	}
}

func TestScenarioMirrorSkippedWhenTheCopyStops(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "new")
	writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
	s := newScenario(t, left, right)
	mark := len(transport.Log.Lines())
	s.mirrorTopRow(t, context.Background(), fullDisk{s.rb})
	if !logged(mark, "COPY stopped: no file can get through") || !logged(mark, "mirror delete skipped: the copy stopped") {
		t.Error("the log does not say the copy stopped and the mirror was skipped for it")
	}
	if !onDisk(filepath.Join(right, "old.txt")) {
		t.Error("old.txt was deleted after the copy stopped")
	}
}

func TestScenarioMirrorStopsWhenCanceledBetweenDeletes(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "same")
	writeFile(t, filepath.Join(right, "a.txt"), "same")
	olds := []string{"old1.txt", "old2.txt", "old3.txt"}
	for _, n := range olds {
		writeFile(t, filepath.Join(right, n), "only on the destination")
	}
	s := newScenario(t, left, right)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.mirrorTopRow(t, ctx, cancelOnRemove{Backend: s.rb, cancel: cancel})
	gone := 0
	for _, n := range olds {
		if !onDisk(filepath.Join(right, n)) {
			gone++
		}
	}
	if gone != 1 {
		t.Errorf("%d of the 3 files deleted, want 1: the cancel came after the first", gone)
	}
}

func TestScenarioMirrorSkipsAFolderItCannotListAgain(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	for _, side := range []string{left, right} {
		writeFile(t, filepath.Join(side, "a", "keep.txt"), "same")
		writeFile(t, filepath.Join(side, "b", "keep.txt"), "same")
	}
	writeFile(t, filepath.Join(right, "a", "x.txt"), "only on the destination")
	writeFile(t, filepath.Join(right, "b", "y.txt"), "only on the destination")
	s := newScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.mirrorTopRow(t, context.Background(), noFreshList{Backend: s.rb, dir: "a"}); p.Failed.Load() != 0 {
		t.Errorf("failed=%d, want 0", p.Failed.Load())
	}
	if !logged(mark, "mirror delete in a skipped: listing the destination: the server went away") {
		t.Error("the log does not say the mirror skipped a")
	}
	wantTree(t, "after the mirror", right, "a/", "a/keep.txt", "a/x.txt", "b/", "b/keep.txt")
}

// A folder only on the destination is one delete; the dialog counts it
// with what it holds, and the mirror removes it whole.
func TestScenarioMirrorDeletesAFolderOnlyOnTheDestination(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "keep.txt"), "same")
	writeFile(t, filepath.Join(right, "keep.txt"), "same")
	writeFile(t, filepath.Join(right, "old", "a.txt"), "only on the destination")
	writeFile(t, filepath.Join(right, "old", "sub", "b.txt"), "only on the destination")
	s := newScenario(t, left, right)
	planned := PlanCopy(s.sc, s.node(t, ""), true, scenarioOpts)
	if files, dirs := DeleteCounts(planned.Deletes); files != 2 || dirs != 2 {
		t.Errorf("the plan deletes %d files, %d folders, want 2 and 2 (old/ and old/sub/): %+v", files, dirs, planned.Deletes)
	}
	if p := s.mirrorTopRow(t, context.Background(), s.rb); p.Failed.Load() != 0 {
		t.Errorf("failed=%d, want 0", p.Failed.Load())
	}
	wantTree(t, "after the mirror", right, "keep.txt")
}
