package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// L16: with --parallel N the N-th transfer ran on the primary and held it
// for its whole file: a listing, and the other workers' SetTimes, waited
// behind it. Each transfer now has a connection of its own, and the
// primary is free for the calls in between. The same with N set while
// connected (+ and - in a copy).
func TestFTPTransfersLeaveThePrimaryFree(t *testing.T) {
	for _, c := range []struct {
		parallel int
		set      bool // opened with 1, then SetParallel(parallel)
	}{{1, false}, {2, false}, {4, false}, {2, true}, {4, true}} {
		name := fmt.Sprintf("parallel=%d", c.parallel)
		if c.set {
			name = fmt.Sprintf("SetParallel(%d)", c.parallel)
		}
		t.Run(name, func(t *testing.T) {
			files := map[string]fakeFTPFile{}
			for i := range c.parallel {
				files[fmt.Sprintf("/%d.bin", i)] = fakeFTPFile{"x", "226 Transfer complete."}
			}
			f := &fakeFTPD{files: files, mfmt: true}
			if !c.set {
				s := openStallFTP(t, f, c.parallel)
				checkPrimaryFree(t, s, c.parallel)
				return
			}
			s := openStallFTP(t, f, 1)
			s.SetParallel(c.parallel)
			checkPrimaryFree(t, s, c.parallel)
		})
	}
}

// checkPrimaryFree opens n downloads on s, 0.bin to n-1.bin, and checks
// that a listing and a SetTimes do not wait for them.
func checkPrimaryFree(t *testing.T, s *stallFTP, n int) {
	t.Helper()
	// Each download holds its connection until Close. A pool short of one
	// makes the last wait: until this ctx ends, not for good.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range n {
		s.open(t, ctx, fmt.Sprintf("%d.bin", i))
	}
	lctx, lcancel := context.WithTimeout(context.Background(), time.Second)
	defer lcancel()
	if _, err := s.List(lctx, ""); err != nil {
		t.Errorf("a listing with %d downloads open: %v", n, err)
	}
	if err := s.SetTimes(lctx, "a", time.Unix(1e9, 0), time.Time{}, time.Time{}); err != nil {
		t.Errorf("SetTimes with %d downloads open: %v", n, err)
	}
}

// L16: a server that takes fewer connections than --parallel turned one
// login away per transfer: each found the extras in use, dialed, was
// refused, and ran on the primary. Now a refused dial waits before the
// next one: the transfers take turns on the extras there are, or, with
// none, on the primary.
func TestFTPServerThatTakesFewerConnections(t *testing.T) {
	// No second dial while the test runs, however slow it is.
	redial, redialMax := poolRedial, poolRedialMax
	poolRedial, poolRedialMax = time.Hour, time.Hour
	t.Cleanup(func() { poolRedial, poolRedialMax = redial, redialMax })
	for _, c := range []struct {
		name   string
		extras int // connections the server takes beyond the primary and the checksum one
	}{{"one extra", 1}, {"no extra", 0}} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeFTPD{store: map[string][]byte{}, files: map[string]fakeFTPFile{"/a.bin": {"x", "226 Transfer complete."}},
				refuse: func(n int) bool { return n > 2+c.extras }}
			s := openStallFTP(t, f, 4)
			var held io.Closer
			if c.extras > 0 {
				held = s.open(t, context.Background(), "a.bin") // holds the one extra
			}
			conns := f.connCount()
			done := goErr(func() error {
				for i := range 5 {
					if err := s.CopyFrom(context.Background(), fmt.Sprintf("u%d.txt", i), strings.NewReader("u"), model.PutParams{}); err != nil {
						return err
					}
				}
				return nil
			})
			if held != nil {
				time.Sleep(100 * time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if _, err := s.List(ctx, ""); err != nil {
					t.Errorf("a listing while the uploads wait for the extra: %v", err)
				}
				cancel()
				if err := held.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := waitErr(t, done, 5*time.Second, "five uploads"); err != nil {
				t.Fatal(err)
			}
			if n := f.connCount() - conns; n != 1 {
				t.Errorf("%d connections dialed for 5 uploads; want 1, the one the server turned away", n)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for i := range 5 {
				if got := string(f.store[fmt.Sprintf("/u%d.txt", i)]); got != "u" {
					t.Errorf("u%d.txt holds %q, want u", i, got)
				}
			}
		})
	}
}

// L16: an extra a transfer failed on went back to the pool as good for a
// minute: when the server had ended it, the next transfer in that minute
// failed on it too. It is now asked first (NOOP): one found dead is
// dropped for a new one, one that answers is kept. The same for a
// download that failed and an upload.
func TestFTPExtraATransferFailedOnIsAskedFirst(t *testing.T) {
	f := &fakeFTPD{store: map[string][]byte{}, files: map[string]fakeFTPFile{
		"/a.bin": {"x", "226 Transfer complete."},
		"/s.bin": {"x", "stall"}, // the reply never comes
	}}
	s := openStallFTP(t, f, 2) // an extra on the old code too, which ran --parallel 1 on the primary
	up := func(name string) error {
		return s.CopyFrom(context.Background(), name, strings.NewReader("u"), model.PutParams{})
	}
	if err := up("1.txt"); err != nil {
		t.Fatal(err)
	}
	conns := f.connCount() // the primary, the checksum one, the extra
	if _, err := s.Open(context.Background(), "missing.bin"); err == nil {
		t.Fatal("Open of a missing file: no error")
	}
	if err := up("2.txt"); err != nil {
		t.Fatalf("an upload after a failure the server answered: %v", err)
	}
	if n := f.connCount() - conns; n != 0 {
		t.Errorf("%d new connections after a failure the server answered; want none: the extra answers", n)
	}

	f.kill(conns)
	if err := up("3.txt"); err == nil {
		t.Fatal("an upload on a connection the server ended worked")
	}
	if err := up("4.txt"); err != nil {
		t.Errorf("the upload after one on a connection the server ended: %v", err)
	}
	if n := f.connCount() - conns; n != 1 {
		t.Errorf("%d new connections after the server ended the extra; want 1", n)
	}

	r, err := s.Open(context.Background(), "s.bin")
	if err != nil {
		t.Fatal(err)
	}
	f.kill(conns + 1)
	if err := r.Close(); err == nil {
		t.Fatal("a download on a connection the server ended closed with no error")
	}
	if err := up("5.txt"); err != nil {
		t.Errorf("the upload after a download on a connection the server ended: %v", err)
	}
	if n := f.connCount() - conns; n != 2 {
		t.Errorf("%d new connections after the server ended the extra twice; want 2", n)
	}
}

// L16: a transfer that waits for the extra in use ends with its ctx, as one
// that waits for the primary does (L8).
func TestFTPTransferWaitingForAnExtraEndsOnCancel(t *testing.T) {
	f := &fakeFTPD{store: map[string][]byte{}, files: map[string]fakeFTPFile{"/a.bin": {"x", "226 Transfer complete."}}}
	s := openStallFTP(t, f, 1)
	s.open(t, context.Background(), "a.bin") // holds the one extra
	for _, c := range []struct {
		name string
		call func(context.Context) error
	}{
		{"an upload", func(ctx context.Context) error {
			return s.CopyFrom(ctx, "b.bin", strings.NewReader("b"), model.PutParams{})
		}},
		{"a download", func(ctx context.Context) error {
			r, err := s.Open(ctx, "a.bin")
			if r != nil {
				r.Close()
			}
			return err
		}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err := waitErr(t, goErr(func() error { return c.call(ctx) }), 5*time.Second, c.name+" that waits for the extra")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s that waits for the extra past its deadline: %v", c.name, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.List(ctx, ""); err != nil {
		t.Errorf("a listing while the extra is in use: %v", err)
	}
}
