package transport

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// R16: a download from an rsync daemon is written whole to the temp folder
// before it is read, and nothing looked at the room there first: a big
// file filled the disk, then failed. Now a file the copy says is bigger
// than the room fails before the daemon sends a byte, for good, and leaves
// nothing; a resume needs room for the tail alone, and a caller that gives
// no size is let through.
func TestDaemonDownloadNeedsRoom(t *testing.T) {
	mod := t.TempDir()
	body := strings.Repeat("d", 3<<20)
	writeTestFile(t, mod, "big.bin", body)
	b, err := NewRsyncBackend(startGorsyncdModule(t, mod, false))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	setTemp(t, tmp)
	size := int64(len(body))
	ctx := ContextWithProgressBudget(context.Background(), size)
	read := func(rc io.ReadCloser, err error) (string, error) {
		if err != nil {
			return "", err
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		return string(got), err
	}
	refused := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, ErrTempRoom) || !isPermanentError(err) || GivesUp(err) {
			t.Errorf("%s: %v; want ErrTempRoom, final for the file (%v), not for the copy (%v)", what, err, isPermanentError(err), GivesUp(err))
		}
		if left := leftInTemp(t, tmp); len(left) > 0 {
			t.Errorf("%s: the temp folder holds %v", what, left)
		}
	}

	withFree(t, size-10+tempMargin-1)
	_, err = read(b.Open(ctx, "big.bin"))
	refused("Open, no room for the file", err)
	_, err = read(b.OpenAt(ctx, "big.bin", 10))
	refused("OpenAt 10, no room for the tail", err)

	withFree(t, size-10+tempMargin)
	_, err = read(b.Open(ctx, "big.bin"))
	refused("Open, room for the tail alone", err)
	if got, err := read(b.OpenAt(ctx, "big.bin", 10)); err != nil || got != body[10:] {
		t.Errorf("OpenAt 10 with room for the tail: %d bytes, %v; want %d", len(got), err, len(body)-10)
	}

	withFree(t, size+tempMargin)
	if got, err := read(b.Open(ctx, "big.bin")); err != nil || got != body {
		t.Errorf("Open with room: %d bytes, %v; want %d", len(got), err, len(body))
	}

	withFree(t, 0)
	if got, err := read(b.Open(context.Background(), "big.bin")); err != nil || got != body {
		t.Errorf("Open with no size given: %d bytes, %v; want %d", len(got), err, len(body))
	}

	// Through the reconnecting wrapper, as a copy calls it: the refusal is
	// given back at once, not tried six times with some 15 s of waits.
	lazy := NewLazyBackend("rsync://test/m", func(context.Context) (model.Backend, error) { return b, nil })
	lctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	began := time.Now()
	_, err = read(lazy.Open(lctx, "big.bin"))
	refused("Open through the wrapper", err)
	if d := time.Since(began); d > time.Second {
		t.Errorf("the refusal through the wrapper took %v: it was tried again", d)
	}
}
