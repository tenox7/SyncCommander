package transport

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"sc/internal/sshtest"
	"sc/model"
)

// withFree makes every disk say it has free bytes free.
func withFree(t *testing.T, free int64) {
	t.Helper()
	old := freeSpace
	freeSpace = func(string) (int64, error) { return free, nil }
	t.Cleanup(func() { freeSpace = old })
}

// readCount is a source that counts what is read from it.
type readCount struct {
	r io.Reader
	n atomic.Int64
}

func (c *readCount) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// spoolSource is a source with no local path: an upload spools it.
type spoolSource struct{ body string }

func (s spoolSource) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.body)), nil
}

func (s spoolSource) OpenAt(_ context.Context, off int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.body[off:])), nil
}

func (s spoolSource) Size() int64       { return int64(len(s.body)) }
func (s spoolSource) LocalPath() string { return "" }

type appender interface {
	AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error
}

// R16: an upload to rsync is sent from a file on disk, written to the temp
// folder first. With no room there, the spool found out when the disk was
// full, after it had read most of the file, and the full disk read as a
// full destination: the whole copy stopped. Now a file with no room fails
// at once, before a byte is read, for good, and the copy goes on; nothing
// is left in the temp folder.
func TestUploadSpoolNeedsRoom(t *testing.T) {
	for name, open := range map[string]func(t *testing.T, mod string) model.Backend{
		"rsync": func(t *testing.T, mod string) model.Backend {
			b, err := NewRsyncBackend(startGorsyncdModule(t, mod, true))
			if err != nil {
				t.Fatal(err)
			}
			return b
		},
		"rsync+ssh": func(t *testing.T, mod string) model.Backend {
			if _, err := exec.LookPath("rsync"); err != nil {
				t.Skip("no rsync binary")
			}
			b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+sshtest.Start(t, false)+mod, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { b.Close() })
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			mod := t.TempDir()
			b := open(t, mod)
			ctx := context.Background()
			tmp := t.TempDir()
			setTemp(t, tmp)
			body := strings.Repeat("b", 3<<20)
			p := model.PutParams{Size: int64(len(body)), Mode: 0o644}
			withFree(t, 3<<20+tempMargin-1)

			src := &readCount{r: strings.NewReader(body)}
			err := b.CopyFrom(ctx, "big.bin", src, p)
			if !errors.Is(err, ErrTempRoom) || !isPermanentError(err) || GivesUp(err) {
				t.Errorf("CopyFrom with no room: %v; want ErrTempRoom, final for the file (%v), not for the copy (%v)", err, isPermanentError(err), GivesUp(err))
			}
			if n := src.n.Load(); n != 0 {
				t.Errorf("CopyFrom read %d bytes before it found there was no room", n)
			}
			writeTestFile(t, mod, "part.bin", body[:10])
			if err := b.(appender).AppendFrom(ctx, "part.bin", spoolSource{body}, p, 10); !errors.Is(err, ErrTempRoom) {
				t.Errorf("AppendFrom with no room: %v, want ErrTempRoom", err)
			}
			if left := leftInTemp(t, tmp); len(left) > 0 {
				t.Errorf("the temp folder holds %v after the refused uploads", left)
			}
			if _, err := os.Stat(filepath.Join(mod, "big.bin")); err == nil {
				t.Error("big.bin is on the destination after a refused upload")
			}

			withFree(t, 3<<20+tempMargin)
			if err := b.CopyFrom(ctx, "big.bin", strings.NewReader(body), p); err != nil {
				t.Errorf("CopyFrom with room: %v", err)
			}
			if err := b.(appender).AppendFrom(ctx, "part.bin", spoolSource{body}, p, 10); err != nil {
				t.Errorf("AppendFrom with room: %v", err)
			}
			for _, rel := range []string{"big.bin", "part.bin"} {
				if got, err := os.ReadFile(filepath.Join(mod, rel)); err != nil || string(got) != body {
					t.Errorf("%s there: %d bytes, %v; want %d", rel, len(got), err, len(body))
				}
			}
		})
	}
}

// A temp disk that fills while a file is written there fails that file,
// and it is tried again: other copies may give their room back. It read
// as a full destination, which no file gets through, and stopped the copy.
func TestAFullTempFailsTheFileNotTheCopy(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full")
	}
	withFree(t, 1<<40) // what fills is the write, not the check
	ctx := context.Background()
	_, _, err := stageUpload(ctx, strings.NewReader("data"), "/dev/full", model.PutParams{Mode: 0o644})
	fullTemp(t, "stageUpload", err)
	err = spoolTo(ctx, spoolSource{"data"}, "/dev/full", model.PutParams{Mode: 0o644})
	fullTemp(t, "spoolTo", err)
}

func fullTemp(t *testing.T, what string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Errorf("%s into a full disk went through", what)
	case GivesUp(err) || isPermanentError(err):
		t.Errorf("%s into a full temp disk: %v; it stops the copy (%v) or is final for the file (%v)", what, err, GivesUp(err), isPermanentError(err))
	case !strings.Contains(err.Error(), "temp folder is full"):
		t.Errorf("%s into a full temp disk: %v; it does not say the temp folder is full", what, err)
	}
}

func TestRoomFor(t *testing.T) {
	withFree(t, 100+tempMargin)
	for _, c := range []struct {
		size int64
		ok   bool
	}{{-1, true}, {0, true}, {100, true}, {101, false}} {
		if err := roomFor("x", c.size); (err == nil) != c.ok || err != nil && !errors.Is(err, ErrTempRoom) {
			t.Errorf("roomFor %d with room for 100: %v", c.size, err)
		}
	}
	withFree(t, 10)
	if err := roomFor("x", 0); err != nil {
		t.Errorf("a file of a size not known was refused: %v", err)
	}
	freeSpace = func(string) (int64, error) { return 0, errors.New("no statfs here") }
	if err := roomFor("x", 1<<40); err != nil {
		t.Errorf("a disk that does not say what it has free refused a file: %v", err)
	}
	if free, err := diskFree(t.TempDir()); errors.Is(err, errors.ErrUnsupported) {
		t.Log("this system does not say what a disk has free")
	} else if err != nil || free <= 0 {
		t.Errorf("diskFree of the temp folder = %d, %v", free, err)
	}
}
