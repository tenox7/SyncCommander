package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// setFTPIdle sets ftpIdle for one test; a connection keeps the value it
// was dialed with.
func setFTPIdle(t *testing.T, d time.Duration) {
	t.Helper()
	SetFTPIdleTimeout(d)
	t.Cleanup(func() { ftpIdle.n.Store(0) })
}

// goErr runs f and hands back its error.
func goErr(f func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- f() }()
	return ch
}

// waitErr waits for a call's error, and fails the test when it has not
// come after d. A call still running then is ended by the server's end.
func waitErr(t *testing.T, done <-chan error, d time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("%s: still waiting after %v", what, d)
		return nil
	}
}

// zeros is an upload that never ends.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// waitData waits for the server to take a data connection for cmd.
func waitData(t *testing.T, f *fakeFTPD, cmd string) {
	t.Helper()
	for {
		select {
		case c := <-f.dataOpen:
			if c == cmd {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("the server took no data connection for %s", cmd)
		}
	}
}

// stallFTP is a backend on a fake server whose stalls the test ends. Its
// cleanup ends the server first, then closes the readers the test opened,
// then the backend: on code that waits for good, the server's end is what
// lets a stuck call, and so Close, return.
type stallFTP struct {
	*FTPBackend
	readers []io.Closer
}

func openStallFTP(t *testing.T, f *fakeFTPD, parallel int) *stallFTP {
	t.Helper()
	f.dataOpen = make(chan string, 16)
	if f.ls == nil {
		f.ls = map[string][]string{"/": {lsLine("a", false)}}
	}
	b, err := NewFTPBackend(context.Background(), "ftp://u:p@"+f.start(t)+"/", false, parallel)
	if err != nil {
		t.Fatal(err)
	}
	s := &stallFTP{FTPBackend: b}
	t.Cleanup(func() {
		f.closeAll()
		for _, r := range s.readers {
			r.Close()
		}
		b.Close()
	})
	return s
}

func (s *stallFTP) open(t *testing.T, ctx context.Context, name string) io.ReadCloser {
	t.Helper()
	r, err := s.Open(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	s.readers = append(s.readers, r)
	return r
}

// L8: a cancel closed only the control socket, and an upload the server
// stopped reading stayed in its write to the data socket until TCP gave
// up. A cancel now closes the data socket too, on the primary connection
// and on an extra one.
func TestFTPCancelEndsAStalledUpload(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			f := &fakeFTPD{store: map[string][]byte{}, storStall: true}
			s := openStallFTP(t, f, parallel)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := goErr(func() error { return s.CopyFrom(ctx, "big.bin", zeros{}, model.PutParams{}) })
			waitData(t, f, "STOR")
			time.Sleep(200 * time.Millisecond) // the socket buffers fill
			cancel()
			if err := waitErr(t, done, 5*time.Second, "a cancelled upload"); err == nil {
				t.Error("a cancelled upload returned nil")
			}
		})
	}
}

// L8: the same for a download the server stopped sending.
func TestFTPCancelEndsAStalledDownload(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			f := &fakeFTPD{files: map[string]fakeFTPFile{"/slow.bin": {strings.Repeat("x", 100), "stall"}}}
			s := openStallFTP(t, f, parallel)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := s.open(t, ctx, "slow.bin")
			buf := make([]byte, 100)
			if _, err := io.ReadFull(r, buf); err != nil {
				t.Fatal(err)
			}
			done := goErr(func() error { _, err := r.Read(buf); return err })
			time.Sleep(200 * time.Millisecond)
			cancel()
			if err := waitErr(t, done, 5*time.Second, "a cancelled download"); err == nil {
				t.Error("a cancelled download read on")
			}
		})
	}
}

// L8: a RETR the server did not answer was not watched at all: the cancel
// watcher started once the reply had come.
func TestFTPCancelEndsADownloadTheServerDoesNotAnswer(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			f := &fakeFTPD{hang: "RETR", files: map[string]fakeFTPFile{"/a.bin": {"x", "226 Transfer complete."}}}
			s := openStallFTP(t, f, parallel)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := goErr(func() error {
				r, err := s.Open(ctx, "a.bin")
				if r != nil {
					r.Close()
				}
				return err
			})
			time.Sleep(300 * time.Millisecond)
			cancel()
			if err := waitErr(t, done, 5*time.Second, "a cancelled RETR with no reply"); err == nil {
				t.Error("a cancelled RETR with no reply opened")
			}
		})
	}
}

// L8: a call run again on a new connection, after its own died, is ended
// by a cancel too. The cancel watcher held the sockets the call started
// on, which the reconnect had replaced.
func TestFTPCancelEndsACallRunAgainAfterAReconnect(t *testing.T) {
	f := &fakeFTPD{listStall: true, dropOnce: "LIST"}
	s := openStallFTP(t, f, 1)
	logins := f.loginCount()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := goErr(func() error { _, err := s.List(ctx, ""); return err })
	waitData(t, f, "LIST")
	if f.loginCount() != logins+1 {
		t.Fatalf("logins: %d before the listing, %d now; want a reconnect", logins, f.loginCount())
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := waitErr(t, done, 5*time.Second, "a cancelled listing on a new connection"); err == nil {
		t.Error("a cancelled listing on a new connection returned nil")
	}
}

// An Open that fails stops its cancel watcher: a cancel of its ctx later
// would close the connection, back in use by then, under the next call.
func TestFTPFailedOpenLeavesTheConnectionAlone(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			f := &fakeFTPD{files: map[string]fakeFTPFile{"/a.bin": {"x", "226 Transfer complete."}}}
			s := openStallFTP(t, f, parallel)
			ctx, cancel := context.WithCancel(context.Background())
			if _, err := s.Open(ctx, "missing.bin"); err == nil {
				t.Fatal("Open of a missing file: no error")
			}
			cancel()
			time.Sleep(100 * time.Millisecond)
			r := s.open(t, context.Background(), "a.bin")
			if body, err := io.ReadAll(r); err != nil || string(body) != "x" {
				t.Errorf("a download after a failed Open whose ctx ended: %q, %v", body, err)
			}
		})
	}
}

// L8: and for a listing.
func TestFTPCancelEndsAStalledListing(t *testing.T) {
	f := &fakeFTPD{listStall: true}
	s := openStallFTP(t, f, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := goErr(func() error { _, err := s.List(ctx, ""); return err })
	waitData(t, f, "LIST")
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := waitErr(t, done, 5*time.Second, "a cancelled listing"); err == nil {
		t.Error("a cancelled listing returned nil")
	}
}

// L8: a download's Close stopped the cancel watcher, then waited for the
// server's final reply with no deadline: a server that never sent it held
// Close, and the copy, for good. A cancel now ends that wait, and so does
// ftpIdle.
func TestFTPCloseOfADownloadWithNoReply(t *testing.T) {
	file := map[string]fakeFTPFile{"/a.bin": {strings.Repeat("x", 4000), "none"}}
	t.Run("cancel", func(t *testing.T) {
		f := &fakeFTPD{files: file}
		s := openStallFTP(t, f, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := s.open(t, ctx, "a.bin")
		if _, err := io.ReadFull(r, make([]byte, 10)); err != nil {
			t.Fatal(err)
		}
		time.AfterFunc(200*time.Millisecond, cancel)
		if err := waitErr(t, goErr(r.Close), 5*time.Second, "Close of a cancelled download"); err == nil {
			t.Error("Close of a download with no reply: no error")
		}
	})
	t.Run("idle", func(t *testing.T) {
		setFTPIdle(t, 300*time.Millisecond)
		f := &fakeFTPD{files: file}
		s := openStallFTP(t, f, 1)
		r := s.open(t, context.Background(), "a.bin")
		if _, err := io.ReadFull(r, make([]byte, 10)); err != nil {
			t.Fatal(err)
		}
		if err := waitErr(t, goErr(r.Close), 5*time.Second, "Close of a download with no reply"); err == nil {
			t.Error("Close of a download with no reply: no error")
		}
	})
}

// L8: a call that waited for the primary connection waited on a mutex as
// long as the call that held it, whatever its own ctx said.
func TestFTPCallWaitingForTheConnectionEndsOnCancel(t *testing.T) {
	f := &fakeFTPD{store: map[string][]byte{}, files: map[string]fakeFTPFile{"/a.bin": {strings.Repeat("x", 4000), "226 Transfer complete."}}}
	s := openStallFTP(t, f, 1)
	r := s.open(t, context.Background(), "a.bin") // holds the primary until Close
	for _, c := range []struct {
		name string
		call func(context.Context) error
	}{
		{"a listing", func(ctx context.Context) error { _, err := s.List(ctx, ""); return err }},
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
		err := waitErr(t, goErr(func() error { return c.call(ctx) }), 5*time.Second, c.name+" that waits for the connection")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s that waits for the connection past its deadline: %v", c.name, err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(context.Background(), ""); err != nil {
		t.Errorf("a listing once the download is closed: %v", err)
	}
}

// L8: an extra connection idle for over a minute is asked whether it is
// still there (NOOP) before it is lent out. An answer that never came held
// the transfer for good: nothing watched the ask.
func TestFTPCancelEndsAProbeTheServerDoesNotAnswer(t *testing.T) {
	f := &fakeFTPD{files: map[string]fakeFTPFile{"/a.bin": {"x", "226 Transfer complete."}}}
	s := openStallFTP(t, f, 2)
	r := s.open(t, context.Background(), "a.bin") // on an extra connection
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
	r.Close()
	s.pool.mu.Lock()
	idle := len(s.pool.idle)
	if idle == 1 {
		s.pool.idle[0].lastUsed = time.Time{} // as if idle for a minute
	}
	s.pool.mu.Unlock()
	if idle != 1 {
		t.Fatalf("%d idle extra connections; want 1", idle)
	}
	f.mu.Lock()
	f.hang = "NOOP"
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := goErr(func() error {
		r, err := s.Open(ctx, "a.bin")
		if r != nil {
			r.Close()
		}
		return err
	})
	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := waitErr(t, done, 5*time.Second, "a cancelled download behind a probe with no answer"); err == nil {
		t.Error("a cancelled download behind a probe with no answer opened")
	}
}

// L8: Close waited for the primary connection, so a call stuck on it held
// quit for good. Close now returns at once and leaves the call that holds
// the connection to end, a healthy one too: the QUIT goes out after it. A
// stuck call ends by its cancel or ftpIdle, and then dials nothing new.
func TestFTPCloseWaitsForNoCall(t *testing.T) {
	t.Run("a download in progress", func(t *testing.T) {
		body := strings.Repeat("x", 4000)
		f := &fakeFTPD{files: map[string]fakeFTPFile{"/a.bin": {body, "226 Transfer complete."}}}
		s := openStallFTP(t, f, 1)
		r := s.open(t, context.Background(), "a.bin") // holds the primary
		waitErr(t, goErr(s.Close), time.Second, "Close under a download")
		if f.quitFrom(1) {
			t.Error("QUIT under a download in progress")
		}
		got, err := io.ReadAll(r)
		if err != nil || string(got) != body {
			t.Errorf("a download Close came under: %d bytes, %v", len(got), err)
		}
		if err := r.Close(); err != nil {
			t.Errorf("Close of a download Close came under: %v", err)
		}
		for until := time.Now().Add(3 * time.Second); !f.quitFrom(1); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(until) {
				t.Fatal("no QUIT once the download ended")
			}
		}
	})
	t.Run("a stuck listing", func(t *testing.T) {
		setFTPIdle(t, 300*time.Millisecond)
		f := &fakeFTPD{listStall: true}
		s := openStallFTP(t, f, 1)
		done := goErr(func() error { _, err := s.List(context.Background(), ""); return err })
		waitData(t, f, "LIST")
		conns := f.connCount()
		waitErr(t, goErr(s.Close), time.Second, "Close under a stuck listing")
		if err := waitErr(t, done, 5*time.Second, "a stuck listing after Close"); err == nil {
			t.Error("a stuck listing after Close returned nil")
		}
		if n := f.connCount(); n != conns {
			t.Errorf("a call that failed after Close dialed %d new connections", n-conns)
		}
	})
}

// L8: FTP had no deadline on a read or a write: a server that stopped
// answering held a call until TCP gave up, or for good behind a tunnel
// that stays up. A read or a write that waits ftpIdle now fails the call.
func TestFTPServerThatStopsAnsweringFailsTheCall(t *testing.T) {
	setFTPIdle(t, 300*time.Millisecond)
	t.Run("reply", func(t *testing.T) {
		s := openStallFTP(t, &fakeFTPD{hang: "LIST"}, 1)
		err := waitErr(t, goErr(func() error { _, err := s.List(context.Background(), ""); return err }), 10*time.Second, "a listing with no reply")
		if err == nil {
			t.Error("a listing with no reply returned nil")
		}
	})
	t.Run("download", func(t *testing.T) {
		f := &fakeFTPD{files: map[string]fakeFTPFile{"/slow.bin": {strings.Repeat("x", 100), "stall"}}}
		s := openStallFTP(t, f, 1)
		r := s.open(t, context.Background(), "slow.bin")
		err := waitErr(t, goErr(func() error { _, err := io.ReadAll(r); return err }), 10*time.Second, "a download that stopped")
		if err == nil {
			t.Error("a download that stopped ended with no error")
		}
	})
	t.Run("upload", func(t *testing.T) {
		f := &fakeFTPD{store: map[string][]byte{}, storStall: true}
		s := openStallFTP(t, f, 1)
		err := waitErr(t, goErr(func() error {
			return s.CopyFrom(context.Background(), "big.bin", zeros{}, model.PutParams{})
		}), 10*time.Second, "an upload the server stopped reading")
		if err == nil {
			t.Error("an upload the server stopped reading returned nil")
		}
	})
}

// The deadline is on each read and each write, not on the connection: a
// download longer than ftpIdle whose bytes keep coming completes, and the
// control connection, idle all through it, reads the final reply.
func TestFTPLongTransferOutlastsTheIdleTimeout(t *testing.T) {
	setFTPIdle(t, 300*time.Millisecond)
	body := strings.Repeat("y", 16*1024) // 16 pieces, 100 ms apart
	f := &fakeFTPD{files: map[string]fakeFTPFile{"/long.bin": {body, "226 Transfer complete."}}, retrPace: 100 * time.Millisecond}
	s := openStallFTP(t, f, 1)
	r := s.open(t, context.Background(), "long.bin")
	got, err := io.ReadAll(r)
	if err != nil || string(got) != body {
		t.Fatalf("a long download: %d bytes, %v", len(got), err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close of a long download: %v", err)
	}
	if _, err := s.List(context.Background(), ""); err != nil {
		t.Errorf("a listing after a long download: %v", err)
	}
}

// opDeadlineConn: a read or a write fails once it waits idle; time between
// them does not count; a large write that keeps moving does not run out;
// a deadline set from outside holds when it comes first, and one set while
// a read waits ends it.
func TestFTPDeadlineIsPerReadAndWrite(t *testing.T) {
	const idle = 400 * time.Millisecond
	pipe := func(t *testing.T, idle time.Duration) (*opDeadlineConn, net.Conn) {
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close(); b.Close() })
		return &opDeadlineConn{Conn: a, idle: idle}, b
	}
	timeout := func(err error) bool { return errors.Is(err, os.ErrDeadlineExceeded) }

	t.Run("a read that waits", func(t *testing.T) {
		c, peer := pipe(t, idle)
		start := time.Now()
		_, err := c.Read(make([]byte, 1))
		if d := time.Since(start); !timeout(err) || d < idle/2 {
			t.Errorf("a read with nothing to read: %v after %v; want a timeout after %v", err, d, idle)
		}
		// What comes after it would be read out of step: it is closed.
		if err := waitErr(t, goErr(func() error { _, err := peer.Write([]byte("late")); return err }), 3*time.Second, "a late reply"); err == nil {
			t.Error("a late reply reached a connection whose read ran out of time")
		}
	})
	t.Run("a write that waits", func(t *testing.T) {
		c, peer := pipe(t, idle)
		if _, err := c.Write([]byte("x")); !timeout(err) {
			t.Errorf("a write nobody reads: %v; want a timeout", err)
		}
		if err := waitErr(t, goErr(func() error { _, err := peer.Read(make([]byte, 1)); return err }), 3*time.Second, "a read after the write ran out"); err == nil {
			t.Error("a connection whose write ran out of time is still open")
		}
	})
	t.Run("time between", func(t *testing.T) {
		c, peer := pipe(t, idle)
		time.Sleep(2 * idle)
		go peer.Write([]byte("x"))
		if _, err := c.Read(make([]byte, 1)); err != nil {
			t.Errorf("a read after a pause: %v", err)
		}
		time.Sleep(2 * idle)
		go io.ReadFull(peer, make([]byte, 1))
		if _, err := c.Write([]byte("y")); err != nil {
			t.Errorf("a write after a pause: %v", err)
		}
	})
	t.Run("a large write that keeps moving", func(t *testing.T) {
		c, peer := pipe(t, idle)
		go func() { // 32 KiB every 60 ms: 1 MiB in about 2 s
			buf := make([]byte, 32<<10)
			for {
				if _, err := io.ReadFull(peer, buf); err != nil {
					return
				}
				time.Sleep(60 * time.Millisecond)
			}
		}()
		if n, err := c.Write(make([]byte, 1<<20)); err != nil || n != 1<<20 {
			t.Errorf("a 1 MiB write the reader takes in steadily: %d, %v", n, err)
		}
	})
	t.Run("a deadline set from outside", func(t *testing.T) {
		c, _ := pipe(t, time.Hour)
		c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		if err := waitErr(t, goErr(func() error { _, err := c.Write([]byte("z")); return err }), 3*time.Second, "a write past its deadline"); !timeout(err) {
			t.Errorf("a write past the deadline set: %v", err)
		}
		c, _ = pipe(t, time.Hour)
		c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if err := waitErr(t, goErr(func() error { _, err := c.Read(make([]byte, 1)); return err }), 3*time.Second, "a read past its deadline"); !timeout(err) {
			t.Errorf("a read past the deadline set: %v", err)
		}
	})
	t.Run("a deadline cleared", func(t *testing.T) {
		c, peer := pipe(t, time.Hour)
		c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		c.SetDeadline(time.Time{})
		time.AfterFunc(200*time.Millisecond, func() { peer.Write([]byte("x")) })
		if err := waitErr(t, goErr(func() error { _, err := c.Read(make([]byte, 1)); return err }), 3*time.Second, "a read once the deadline is cleared"); err != nil {
			t.Errorf("a read once the deadline is cleared: %v", err)
		}
	})
	t.Run("a deadline set while a read waits", func(t *testing.T) {
		c, _ := pipe(t, time.Hour)
		done := goErr(func() error { _, err := c.Read(make([]byte, 1)); return err })
		time.Sleep(100 * time.Millisecond)
		c.SetDeadline(time.Now())
		if err := waitErr(t, done, 3*time.Second, "a read whose deadline came"); !timeout(err) {
			t.Errorf("a read whose deadline was set while it waited: %v", err)
		}
	})
}
