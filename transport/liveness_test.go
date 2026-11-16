package transport

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// D5, F5: the stall guard watched the byte counter only, so a step that
// waits on the server's work (a checksum of a large file) was taken for a
// stall. A step that says how long the server will be silent is given that
// long.
func TestStallGuardWaitsForExpectedSilence(t *testing.T) {
	var counter atomic.Int64
	err := WithStallGuard(context.Background(), &counter, time.Second, func(ctx context.Context) error {
		ExpectSilence(ctx, 3*time.Second)
		select {
		case <-time.After(2500 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Errorf("a step silent for 2.5 s of the 3 s it said = %v, want it left alone", err)
	}
}

// D5, R7: a step that works here without moving the bytes the user sees
// (reading to compare, spooling to disk) touches the guard as it goes.
func TestStallGuardSeesWork(t *testing.T) {
	var counter atomic.Int64
	err := WithStallGuard(context.Background(), &counter, time.Second, func(ctx context.Context) error {
		for range 10 {
			select {
			case <-time.After(300 * time.Millisecond):
				Touch(ctx)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		t.Errorf("a step that worked for 3 s without progress = %v, want it left alone", err)
	}
}

// What a step says does not stop the guard: silence past it is a stall, and
// the step can tell it was one.
func TestStallGuardCatchesSilencePastWhatWasSaid(t *testing.T) {
	var counter atomic.Int64
	var stalled bool
	t0 := time.Now()
	err := WithStallGuard(context.Background(), &counter, time.Second, func(ctx context.Context) error {
		ExpectSilence(ctx, 500*time.Millisecond)
		<-ctx.Done()
		stalled = Stalled(ctx)
		return ctx.Err()
	})
	if !errors.Is(err, ErrStall) {
		t.Errorf("err = %v, want ErrStall", err)
	}
	if !stalled {
		t.Error("the step could not tell its ctx was ended by a stall")
	}
	if d := time.Since(t0); d < 1500*time.Millisecond || d > 4*time.Second {
		t.Errorf("the stall came after %v, want after the 0.5 s said plus the 1 s idle", d)
	}
}

// slowSource gives one byte every 300 ms, as a remote that is slow but alive.
type slowSource struct{ n int64 }

func (s *slowSource) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(&slowReader{left: s.n}), nil
}
func (s *slowSource) OpenAt(context.Context, int64) (io.ReadCloser, error) {
	return nil, ErrUnsupported
}
func (s *slowSource) Size() int64       { return s.n }
func (s *slowSource) LocalPath() string { return "" }

type slowReader struct{ left int64 }

func (r *slowReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	time.Sleep(300 * time.Millisecond)
	r.left--
	p[0] = 'x'
	return 1, nil
}

// R7: spooling a source that is not local, for rsync to send from disk,
// moved no counter: a spool longer than the stall timeout was cancelled
// and the copy started over.
func TestSpoolIsSeenWorking(t *testing.T) {
	var counter atomic.Int64
	dst := filepath.Join(t.TempDir(), "f")
	err := WithStallGuard(context.Background(), &counter, time.Second, func(ctx context.Context) error {
		return spoolTo(ctx, &slowSource{n: 10}, dst, 0o644)
	})
	if err != nil {
		t.Fatalf("a 3 s spool of a slow source = %v, want it to finish", err)
	}
	if fi, err := os.Stat(dst); err != nil || fi.Size() != 10 {
		t.Errorf("spooled %v, %v; want 10 bytes", fi, err)
	}
}

// D5: an rsync download is counted as it comes in, into a temp file. The
// destination then reads that file only as fast as it takes it (an upload
// of minutes), and no counter showed that: the stall guard ended it.
func TestSelfCountedDownloadIsSeenRead(t *testing.T) {
	var counter atomic.Int64
	err := WithStallGuard(context.Background(), &counter, time.Second, func(ctx context.Context) error {
		ctx = ContextWithFileSize(ContextWithProgress(ctx, &counter), 10)
		rc, err := rsyncOpenViaTemp(ctx, "f.bin", 0, func(dstDir string) error {
			return os.WriteFile(filepath.Join(dstDir, "f.bin"), []byte("0123456789"), 0o644)
		})
		if err != nil {
			return err
		}
		defer rc.Close()
		buf := make([]byte, 1)
		for {
			if _, err := rc.Read(buf); err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			time.Sleep(300 * time.Millisecond)
		}
	})
	if err != nil {
		t.Errorf("reading a downloaded file for 3 s = %v, want it left alone", err)
	}
}

// D5: a push whose source counted its own bytes (an rsync download read
// back from its temp file) credits nothing. It ran without --progress, so
// nothing showed it moving, and the stall guard ended any upload longer
// than the stall timeout.
func TestUncreditedPushIsSeenMoving(t *testing.T) {
	url, mod := startGorsyncd(t)
	b, err := NewRsyncBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	SetBandwidthOut(100 << 10)
	defer SetBandwidthOut(0)
	body := strings.Repeat("z", 400<<10) // 3 s at 100 KiB/s after a 1 s burst
	var counter atomic.Int64
	err = WithStallGuard(context.Background(), &counter, time.Second, func(ctx context.Context) error {
		ctx = ContextWithFileSize(ContextWithProgress(ctx, &counter), int64(len(body)))
		return b.CopyFrom(ctx, "f.bin", WrapPreCounted(io.NopCloser(strings.NewReader(body))), 0o644)
	})
	if err != nil {
		t.Fatalf("a 3 s push = %v, want it to finish", err)
	}
	if got, err := os.ReadFile(filepath.Join(mod, "f.bin")); err != nil || len(got) != len(body) {
		t.Errorf("the daemon holds %d bytes (%v), want %d", len(got), err, len(body))
	}
}

// ownerStub counts its own copy progress, as the rsync backends do, and
// notes whether it saw that its source had counted itself.
type ownerStub struct {
	copyStub
	sawPreCounted bool
}

func (s *ownerStub) OwnsCopyProgress() bool { return true }
func (s *ownerStub) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	s.sawPreCounted = IsPreCounted(src)
	return s.copyStub.CopyFrom(ctx, relPath, src, mode)
}

// F21: every remote backend runs inside the reconnecting wrapper, which
// neither passed on the rsync backend's "I count my own progress" nor let it
// see that the source had counted its own bytes: each copy into rsync was
// counted twice.
func TestWrapperPassesOnProgressOwnership(t *testing.T) {
	ctx := context.Background()
	inner := &ownerStub{}
	b := NewLazyBackend("rsync://host/m", func(context.Context) (model.Backend, error) { return inner, nil })
	defer CloseBackend(b)
	if ok, err := Supports[ProgressOwner](ctx, b); err != nil || !ok {
		t.Fatalf("Supports = %v, %v; want the backend's own counting seen", ok, err)
	}
	if owner, ok := b.(ProgressOwner); !ok || !owner.OwnsCopyProgress() {
		t.Error("the wrapper does not say its backend counts its own copy progress")
	}
	if err := b.CopyFrom(ctx, "f", WrapPreCounted(io.NopCloser(strings.NewReader("abc"))), 0o644); err != nil {
		t.Fatal(err)
	}
	if !inner.sawPreCounted {
		t.Error("through the wrapper the backend did not see that its source counted its own bytes")
	}

	plain := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) { return &connStub{}, nil })
	defer CloseBackend(plain)
	if ok, err := Supports[ProgressOwner](ctx, plain); err != nil || ok {
		t.Errorf("Supports for a backend that does not count = %v, %v; want false", ok, err)
	}
	if owner, ok := plain.(ProgressOwner); ok && owner.OwnsCopyProgress() {
		t.Error("the wrapper says a backend that does not count its progress does")
	}
}
