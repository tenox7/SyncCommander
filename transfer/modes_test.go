package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"sc/model"
	"sc/transport"
)

// modeRefused is what a destination says of a file it wrote but could not
// give its mode, as an SFTP server that refuses SETSTAT.
func modeRefused(relPath string) error {
	return fmt.Errorf("test: chmod %s: %w: %w", relPath, model.ErrModeNotSet, errors.New("operation unsupported"))
}

// noModes is a destination with part files that takes no mode: a write,
// and a commit, does its work and then says so, as write and commit say.
// writes counts the writes.
type noModes struct {
	*transport.LocalBackend
	writes        *atomic.Int64
	write, commit bool
}

func (b noModes) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	b.writes.Add(1)
	err := b.LocalBackend.CopyFrom(ctx, relPath, src, p)
	if err == nil && b.write {
		err = modeRefused(relPath)
	}
	return err
}

func (b noModes) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	b.writes.Add(1)
	err := b.LocalBackend.AppendFrom(ctx, relPath, src, p, offset)
	if err == nil && b.write {
		err = modeRefused(relPath)
	}
	return err
}

func (b noModes) CommitPart(ctx context.Context, relPath string, mode os.FileMode) error {
	err := b.LocalBackend.CommitPart(ctx, relPath, mode)
	if err == nil && b.commit {
		err = modeRefused(relPath)
	}
	return err
}

// noModesPlain is a destination without part files whose writes take no
// mode.
type noModesPlain struct {
	model.Backend
	writes *atomic.Int64
}

func (b noModesPlain) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	b.writes.Add(1)
	err := b.Backend.CopyFrom(ctx, relPath, src, p)
	if err == nil {
		err = modeRefused(relPath)
	}
	return err
}

// S20: a destination that wrote a file but would not take its mode failed
// the copy, which sent the file up to six times, and a commit that could
// not give the mode left the file under its part's name. The copy goes
// through now: the file is sent once and counted once, and a mirror still
// runs.
func TestCopyCountsFilesWithoutTheirModes(t *testing.T) {
	for _, c := range []struct {
		name string
		dst  func(right string, writes *atomic.Int64) model.Backend
	}{
		{"part files, the write", func(right string, w *atomic.Int64) model.Backend {
			return noModes{transport.NewLocalBackend(right), w, true, false}
		}},
		{"part files, the commit", func(right string, w *atomic.Int64) model.Backend {
			return noModes{transport.NewLocalBackend(right), w, false, true}
		}},
		{"part files, both", func(right string, w *atomic.Int64) model.Backend {
			return noModes{transport.NewLocalBackend(right), w, true, true}
		}},
		{"no part files", func(right string, w *atomic.Int64) model.Backend {
			return noModesPlain{transport.NewLocalBackend(right), w}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
			writeFile(t, filepath.Join(left, "a.txt"), "new")
			writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
			s := newScenario(t, left, right)
			var writes atomic.Int64
			mark := len(transport.Log.Lines())
			p := s.mirrorTopRow(t, context.Background(), c.dst(right, &writes))
			if f, m, n := p.Failed.Load(), p.ModesNotSet.Load(), p.TimesNotSet.Load(); f != 0 || m != 1 || n != 0 {
				t.Errorf("failed=%d without modes=%d without times=%d, want 0, 1 and 0", f, m, n)
			}
			if n := writes.Load(); n != 1 {
				t.Errorf("the file was written %d times, want once", n)
			}
			if !onDisk(filepath.Join(right, "a.txt")) || readFile(t, filepath.Join(right, "a.txt")) != "new" {
				t.Error("a.txt is not the copy")
			}
			if onDisk(filepath.Join(right, model.PartPath("a.txt"))) {
				t.Error("the part is still there")
			}
			if onDisk(filepath.Join(right, "old.txt")) {
				t.Error("the mirror did not run after a file without its mode")
			}
			// A WARN line: FAIL would count in the status bar as a failed run.
			if !logged(mark, "copy WARN COPY: 1 file(s) copied without their modes") {
				t.Error("the log does not sum the files without their modes up in a WARN line")
			}
			if !logged(mark, "chmod a.txt: ") {
				t.Error("the log has no line for a.txt's mode")
			}
		})
	}
}

// S20: a part resumed onto a destination that takes no mode: the rest is
// appended once, and the file counted once.
func TestResumedPartWithoutItsMode(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	body := strings.Repeat("0123456789", 100)
	writeFile(t, filepath.Join(left, "a.txt"), body)
	writeFile(t, filepath.Join(right, model.PartPath("a.txt")), body[:500])
	var writes atomic.Int64
	s := scanned(t, left, right, transport.NewLocalBackend(left), noModes{transport.NewLocalBackend(right), &writes, true, true})
	p := s.copy(t, "a.txt", true, false)
	if f, m := p.Failed.Load(), p.ModesNotSet.Load(); f != 0 || m != 1 {
		t.Errorf("failed=%d without modes=%d, want 0 and 1", f, m)
	}
	if n := writes.Load(); n != 1 {
		t.Errorf("%d writes, want the append alone", n)
	}
	if !onDisk(filepath.Join(right, "a.txt")) || readFile(t, filepath.Join(right, "a.txt")) != body {
		t.Error("a.txt is not the copy")
	}
	if onDisk(filepath.Join(right, model.PartPath("a.txt"))) {
		t.Error("the part is still there")
	}
}
