package transfer

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// F17: what the copy could not do shows where the user looks, not in the
// log alone. A mirror delete that fails is a failure. A file whose times
// cannot be set is copied, but compares as different and is sent again by
// the next copy: it is counted on its own, and is no failure, so a mirror
// still runs.

// noRemove refuses every delete, as a read-only destination does.
type noRemove struct{ model.Backend }

func (noRemove) Remove(context.Context, string) error    { return fs.ErrPermission }
func (noRemove) RemoveAll(context.Context, string) error { return fs.ErrPermission }

// noTimes takes files but not their times, failing with err; with the local
// backend under it, it writes through part files.
type noTimes struct {
	*transport.LocalBackend
	err error
}

func (b noTimes) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return b.err
}

// noTimesPlain is noTimes on a destination without part files.
type noTimesPlain struct {
	model.Backend
	err error
}

func (b noTimesPlain) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return b.err
}

func TestMirrorDeleteThatFailsIsCounted(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "same")
	writeFile(t, filepath.Join(right, "a.txt"), "same")
	writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
	s := newScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.mirrorTopRow(t, context.Background(), noRemove{s.rb}); p.Failed.Load() != 1 {
		t.Errorf("failed=%d, want 1: the delete of old.txt", p.Failed.Load())
	}
	if !logged(mark, "mirror delete finished with 1 failure(s) of 1") {
		t.Error("the log does not sum the mirror's failures up")
	}
	if logged(mark, "COPY finished with") {
		t.Error("the copy's own summary counts a delete: no copy failed")
	}
	if !onDisk(filepath.Join(right, "old.txt")) {
		t.Error("old.txt is gone")
	}
}

func TestCopyCountsFilesWithoutTheirTimes(t *testing.T) {
	denied := errors.New("operation not permitted")
	for _, c := range []struct {
		name string
		dst  func(right string) model.Backend
		want int64 // files without their times
	}{
		{"part files", func(right string) model.Backend { return noTimes{transport.NewLocalBackend(right), denied} }, 1},
		{"no part files", func(right string) model.Backend { return noTimesPlain{transport.NewLocalBackend(right), denied} }, 1},
		// A destination that keeps no times (restic) is no file without
		// its times: there is nothing to set.
		{"part files, keeps no times", func(right string) model.Backend {
			return noTimes{transport.NewLocalBackend(right), transport.ErrUnsupported}
		}, 0},
		{"no part files, keeps no times", func(right string) model.Backend {
			return noTimesPlain{transport.NewLocalBackend(right), transport.ErrUnsupported}
		}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
			writeFile(t, filepath.Join(left, "a.txt"), "new")
			writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
			s := newScenario(t, left, right)
			mark := len(transport.Log.Lines())
			p := s.mirrorTopRow(t, context.Background(), c.dst(right))
			if p.Failed.Load() != 0 || p.TimesNotSet.Load() != c.want {
				t.Errorf("failed=%d without times=%d, want 0 and %d", p.Failed.Load(), p.TimesNotSet.Load(), c.want)
			}
			// A WARN line: FAIL would count in the status bar as a failed run.
			if got := logged(mark, "copy WARN COPY: 1 file(s) copied without their times"); got != (c.want == 1) {
				t.Errorf("the log sums files without their times up in a WARN line: %v, want %v", got, c.want == 1)
			}
			if got := logged(mark, "settimes a.txt"); got != (c.want == 1) {
				t.Errorf("the log has a settimes line: %v, want %v", got, c.want == 1)
			}
			if got := readFile(t, filepath.Join(right, "a.txt")); got != "new" {
				t.Errorf("a.txt = %q", got)
			}
			if onDisk(filepath.Join(right, "old.txt")) {
				t.Error("the mirror did not run after a file without its times")
			}
		})
	}
}

// The UI reuses one Progress for every copy: what one copy counted must not
// show in the next. Every counter, also one added later.
func TestResetZeroesEveryCounter(t *testing.T) {
	p := &Progress{}
	v := reflect.ValueOf(p).Elem()
	var names []string
	for i := range v.NumField() {
		if !v.Type().Field(i).IsExported() {
			continue
		}
		if c, ok := v.Field(i).Addr().Interface().(*atomic.Int64); ok {
			c.Store(7)
			names = append(names, v.Type().Field(i).Name)
		}
	}
	p.Reset(3, true)
	for _, n := range names {
		got := v.FieldByName(n).Addr().Interface().(*atomic.Int64).Load()
		switch n {
		case "Parallel":
			if got != 3 {
				t.Errorf("Parallel = %d after Reset(3), want 3", got)
			}
		case "Start":
			if got == 7 {
				t.Error("Start kept the last copy's start")
			}
		default:
			if got != 0 {
				t.Errorf("%s = %d after Reset, want 0", n, got)
			}
		}
	}
	if !slices.Contains(names, "TimesNotSet") || !slices.Contains(names, "Failed") {
		t.Fatalf("the walk missed counters: %v", names)
	}
}
