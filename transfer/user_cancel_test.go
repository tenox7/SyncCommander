package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
	"sc/transport"
)

// x stops a copy, and the log opened after each stop: the mirror that did
// not run was an ERR line, and the files in flight were failures, so the
// copy "finished with 2 failure(s)" in a FAIL line. What x cut off did not
// fail: the next copy sends it. A fatal abort cancels the same context with
// no cause, and what it cuts off still fails.

// errLost is what a backend says when the cancel closes its connection. It
// has no "context canceled" in it, which the log drops by itself.
var errLost = errors.New("connection lost")

// userStop is a cancel while the copy writes: cancel gets cause, as x gives
// transport.ErrUserCanceled, and the write fails as a closed connection
// makes it.
type userStop struct {
	model.Backend
	cancel context.CancelCauseFunc
	cause  error
}

func (b userStop) CopyFrom(context.Context, string, io.Reader, model.PutParams) error {
	b.cancel(b.cause)
	return errLost
}

func (b userStop) Mkdir(context.Context, string, os.FileMode) error {
	b.cancel(b.cause)
	return errLost
}

func (b userStop) Remove(context.Context, string) error {
	b.cancel(b.cause)
	return errLost
}

// batchStop takes a folder in one session, as rsync does, and x stops the
// session after its first file.
type batchStop struct {
	model.Backend
	cancel context.CancelCauseFunc
}

func (b batchStop) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.Backend.Capabilities(ctx)
	caps.Batch = true
	return caps, err
}

func (b batchStop) SendLocalTree(_ context.Context, _, _ string, files []string, onFile func(string)) error {
	onFile(files[0])
	b.cancel(transport.ErrUserCanceled)
	return errLost
}

// problemsSince are the lines logged since mark that open the log: ERR,
// FAIL and FATAL.
func problemsSince(mark int) []string {
	var out []string
	for _, l := range transport.Log.Lines()[mark:] {
		if f := strings.Fields(l); len(f) > 2 && (f[2] == "ERR" || f[2] == "FAIL" || f[2] == "FATAL") {
			out = append(out, l)
		}
	}
	return out
}

// wantNoProblems fails t when the lines logged since mark would open the
// log, or count as errors or failures in the status bar.
func wantNoProblems(t *testing.T, mark, errs, fails int) {
	t.Helper()
	if p := problemsSince(mark); len(p) > 0 {
		t.Errorf("x logged errors: %q", p)
	}
	if transport.Log.ErrCount() != errs || transport.Log.FailedCount() != fails {
		t.Errorf("err:%d fail:%d after x, want err:%d fail:%d", transport.Log.ErrCount(), transport.Log.FailedCount(), errs, fails)
	}
}

func TestUserCancelCutsWhatIsInFlightOffWithNoError(t *testing.T) {
	for _, c := range []struct {
		what, cut, write string // what x cuts off: its log name, and the write that is cut
	}{
		{"a file", "COPY a.txt", "a.txt"},
		// A mkdir is tried once, and its error comes as the backend gives it.
		{"a folder", "mkdir d", "d/"},
	} {
		t.Run(c.what, func(t *testing.T) {
			root := t.TempDir()
			left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
			if strings.HasSuffix(c.write, "/") {
				if err := os.MkdirAll(filepath.Join(left, c.write), 0o755); err != nil {
					t.Fatal(err)
				}
			} else {
				writeFile(t, filepath.Join(left, c.write), "new")
			}
			writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
			s := newScenario(t, left, right)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			errs, fails, mark := transport.Log.ErrCount(), transport.Log.FailedCount(), len(transport.Log.Lines())
			p := s.mirrorTopRow(t, ctx, userStop{Backend: s.rb, cancel: cancel, cause: transport.ErrUserCanceled})
			if p.Failed.Load() != 0 {
				t.Errorf("failed=%d after x, want 0", p.Failed.Load())
			}
			wantNoProblems(t, mark, errs, fails)
			for _, want := range []string{c.cut + ": canceled", "mirror delete skipped: copy canceled", "COPY canceled: 0 of 1 done"} {
				if !logged(mark, want) {
					t.Errorf("the log does not say %q: %q", want, transport.Log.Lines()[mark:])
				}
			}
			if !onDisk(filepath.Join(right, "old.txt")) {
				t.Error("old.txt was deleted after x")
			}
		})
	}
}

// A fatal abort (an rsync module that is read only) cancels with no cause:
// the file it cut off failed, and the copy says so.
func TestFatalAbortStillFailsWhatIsInFlight(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "new")
	writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
	s := newScenario(t, left, right)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	mark := len(transport.Log.Lines())
	p := s.mirrorTopRow(t, ctx, userStop{Backend: s.rb, cancel: cancel})
	if p.Failed.Load() != 1 || !logged(mark, "COPY finished with 1 failure(s) of 1") {
		t.Errorf("failed=%d, want 1 and a FAIL line: %q", p.Failed.Load(), transport.Log.Lines()[mark:])
	}
}

func TestUserCancelWhileListingIsNoFailure(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "d", "a.txt"), "under d")
	s := newScenarioDepth(t, left, right, false) // d is not listed yet
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(transport.ErrUserCanceled)
	errs, fails, mark := transport.Log.ErrCount(), transport.Log.FailedCount(), len(transport.Log.Lines())
	p := &Progress{}
	Copy(ctx, Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: s.node(t, ""), LeftToRight: true,
		Opts: scenarioOpts, Parallel: 1, Progress: p})
	if p.Failed.Load() != 0 {
		t.Errorf("failed=%d after x, want 0", p.Failed.Load())
	}
	wantNoProblems(t, mark, errs, fails)
	if !logged(mark, "COPY canceled while listing /") {
		t.Errorf("the log does not say the copy was canceled: %q", transport.Log.Lines()[mark:])
	}
}

func TestUserCancelDuringTheMirrorDeleteIsNoFailure(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(right, "old1.txt"), "only on the destination")
	writeFile(t, filepath.Join(right, "old2.txt"), "only on the destination")
	s := newScenario(t, left, right)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	errs, fails, mark := transport.Log.ErrCount(), transport.Log.FailedCount(), len(transport.Log.Lines())
	p := s.mirrorTopRow(t, ctx, userStop{Backend: s.rb, cancel: cancel, cause: transport.ErrUserCanceled})
	if p.Failed.Load() != 0 {
		t.Errorf("failed=%d after x, want 0", p.Failed.Load())
	}
	wantNoProblems(t, mark, errs, fails)
	if !logged(mark, "mirror delete canceled") {
		t.Errorf("the log does not say the mirror was canceled: %q", transport.Log.Lines()[mark:])
	}
	if !onDisk(filepath.Join(right, "old2.txt")) {
		t.Error("old2.txt was deleted after x")
	}
}

func TestUserCancelStopsABatchWithNoError(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "d", "a.txt"), "a")
	writeFile(t, filepath.Join(left, "d", "b.txt"), "b")
	s := newScenario(t, left, right)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	errs, fails, mark := transport.Log.ErrCount(), transport.Log.FailedCount(), len(transport.Log.Lines())
	p := &Progress{}
	Copy(ctx, Request{Src: s.lb, Dst: batchStop{Backend: s.rb, cancel: cancel}, Scanner: s.sc, Node: s.node(t, "d"),
		RelPath: "d", LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Batch: true})
	// A batch that failed went file by file from 0; one that x stopped
	// keeps what it sent.
	if p.Failed.Load() != 0 || p.Done.Load() != 1 || p.Total.Load() != 2 {
		t.Errorf("failed=%d done=%d total=%d after x, want 0, 1, 2", p.Failed.Load(), p.Done.Load(), p.Total.Load())
	}
	wantNoProblems(t, mark, errs, fails)
	if !logged(mark, "COPY canceled: 1 of 2 done") || logged(mark, "BATCH d OK") {
		t.Errorf("the log does not say the batch was canceled: %q", transport.Log.Lines()[mark:])
	}
}
