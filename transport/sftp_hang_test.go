package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sc/internal/sshtest"
	"sc/model"
)

// hangLimit is how long a call may take to end after its ctx was canceled
// on a server that does not answer: the grace, and room for a slow machine.
const hangLimit = 5 * time.Second

// answerGate holds the sftp server while it is shut, as a server whose file
// system hangs: the server process reads and answers nothing, and the ssh
// connection stays up (S11).
type answerGate struct {
	mu   sync.Mutex
	shut chan struct{} // nil while open
	held chan struct{} // gets a value when the server is held
}

func (g *answerGate) close() {
	g.mu.Lock()
	if g.shut == nil {
		g.shut = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *answerGate) open() {
	g.mu.Lock()
	if g.shut != nil {
		close(g.shut)
		g.shut = nil
	}
	g.mu.Unlock()
}

// wait holds the server while g is shut.
func (g *answerGate) wait() {
	g.mu.Lock()
	shut := g.shut
	g.mu.Unlock()
	if shut != nil {
		select {
		case g.held <- struct{}{}:
		default:
		}
		<-shut
	}
}

func (g *answerGate) stream(rw io.ReadWriter) io.ReadWriter { return gatedStream{g, rw} }

type gatedStream struct {
	g  *answerGate
	rw io.ReadWriter
}

func (s gatedStream) Read(p []byte) (int, error) {
	s.g.wait()
	return s.rw.Read(p)
}

func (s gatedStream) Write(p []byte) (int, error) {
	s.g.wait()
	return s.rw.Write(p)
}

// hangingSFTP serves a new folder over sftp behind a gate, and opens it as
// a pane does, through the reconnecting wrapper, connected.
func hangingSFTP(t *testing.T, parallel int) (model.Backend, *answerGate, string) {
	t.Helper()
	b, g, dir := hangingSFTPUnconnected(t, parallel)
	if _, err := b.List(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	return b, g, dir
}

// hangingSFTPUnconnected is hangingSFTP before the first call connects.
func hangingSFTPUnconnected(t *testing.T, parallel int) (model.Backend, *answerGate, string) {
	t.Helper()
	dir := t.TempDir()
	g := &answerGate{held: make(chan struct{}, 1)}
	addr := sshtest.StartWith(t, sshtest.Options{SFTP: true, SFTPStream: g.stream})
	b, err := TryOpenBackend("sftp://x:pw@"+addr+dir, true, parallel)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { CloseBackend(b) })
	// Runs first: a call the code left waiting can end.
	t.Cleanup(g.open)
	return b, g, dir
}

// endsOnCancel runs call and cancels its ctx once the server is held. It fails the test when the call has not returned hangLimit
// later, and then opens the gate, so the call ends in any case.
func endsOnCancel(t *testing.T, g *answerGate, call func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- call(ctx) }()
	select {
	case <-g.held:
	case err := <-done:
		t.Fatalf("the call ended before the server was held: %v", err)
	case <-time.After(10 * time.Second):
		g.open()
		t.Fatal("the server was not held")
	}
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(hangLimit):
		t.Errorf("the call had not ended %v after its ctx was canceled", hangLimit)
		g.open()
		return <-done
	}
}

// hangZeros reads as zeros without end.
type hangZeros struct{}

func (hangZeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// shutAfter reads r and calls shut once n bytes have gone through.
type shutAfter struct {
	r    io.Reader
	n    int64
	shut func()
}

func (s *shutAfter) Read(p []byte) (int, error) {
	k, err := s.r.Read(p)
	if s.n -= int64(k); s.n <= 0 && s.shut != nil {
		s.shut()
		s.shut = nil
	}
	return k, err
}

// sftpInner is the backend the wrapper has connected now.
func sftpInner(t *testing.T, b model.Backend) *SFTPBackend {
	t.Helper()
	s, ok := b.(*lazyBackend).current().(*SFTPBackend)
	if !ok {
		t.Fatalf("connected to %T", b.(*lazyBackend).current())
	}
	return s
}

// S11: pkg/sftp takes no ctx, so every call on a server that stopped
// answering waited for ever: a cancel, the stall guard and the scanner's
// listing timeout ended none of them. Each ends now, and the next call
// works on its first try: the wrapper connects again at once.
func TestSFTPCallsOnAHungServerEndOnCancel(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	for _, c := range []struct {
		name string
		call func(context.Context, model.Backend) error
	}{
		{"List", func(ctx context.Context, b model.Backend) error { _, err := b.List(ctx, "d"); return err }},
		{"Stat", func(ctx context.Context, b model.Backend) error { _, err := b.Stat(ctx, "a.txt"); return err }},
		{"Mkdir", func(ctx context.Context, b model.Backend) error { return b.Mkdir(ctx, "new", 0o755) }},
		{"Rename", func(ctx context.Context, b model.Backend) error { return b.Rename(ctx, "a.txt", "b.txt") }},
		{"Remove", func(ctx context.Context, b model.Backend) error { return b.Remove(ctx, "a.txt") }},
		{"RemoveAll", func(ctx context.Context, b model.Backend) error { return b.RemoveAll(ctx, "d") }},
		{"SetTimes", func(ctx context.Context, b model.Backend) error {
			return b.SetTimes(ctx, "a.txt", time.Unix(1e9, 0), time.Time{}, time.Time{})
		}},
		{"PartSize", func(ctx context.Context, b model.Backend) error {
			_, err := b.(model.PartWriter).PartSize(ctx, "a.txt")
			return err
		}},
		{"CommitPart", func(ctx context.Context, b model.Backend) error {
			return b.(model.PartWriter).CommitPart(ctx, "a.txt", 0o644)
		}},
		{"Open", func(ctx context.Context, b model.Backend) error {
			rc, err := b.Open(ctx, "a.txt")
			if err == nil {
				rc.Close()
			}
			return err
		}},
		{"CopyFrom", func(ctx context.Context, b model.Backend) error {
			return b.CopyFrom(ctx, "new.txt", strings.NewReader("new"), model.PutParams{Mode: 0o644})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, g, dir := hangingSFTP(t, 1)
			if err := os.Mkdir(filepath.Join(dir, "d"), 0o755); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a.txt", "d/x.txt", model.PartPath("a.txt")} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("hello"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			g.close()
			err := endsOnCancel(t, g, func(ctx context.Context) error { return c.call(ctx, b) })
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s on a server that does not answer: %v, want it canceled", c.name, err)
			}
			g.open()
			if entries, err := b.List(context.Background(), ""); err != nil || len(entries) == 0 {
				t.Errorf("the next List: %+v, %v", entries, err)
			}
		})
	}
}

// S11: starting the sftp session waited for the server's answer too: the
// first call, which connects, and an upload that dials an extra connection
// hung on a server that does not answer.
func TestSFTPConnectToAHungServerEndsOnCancel(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	t.Run("the first call", func(t *testing.T) {
		b, g, _ := hangingSFTPUnconnected(t, 1)
		g.close()
		err := endsOnCancel(t, g, func(ctx context.Context) error { _, err := b.List(ctx, ""); return err })
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the connect: %v, want it canceled", err)
		}
		g.open()
		if _, err := b.List(context.Background(), ""); err != nil {
			t.Errorf("the next List: %v", err)
		}
	})
	t.Run("an extra connection", func(t *testing.T) {
		b, g, _ := hangingSFTP(t, 2)
		g.close()
		err := endsOnCancel(t, g, func(ctx context.Context) error {
			return b.CopyFrom(ctx, "new.txt", strings.NewReader("new"), model.PutParams{Mode: 0o644})
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the upload: %v, want it canceled", err)
		}
		g.open()
		if sftpInner(t, b).main.cut.Load() {
			t.Error("the main connection was closed for an extra one")
		}
		if err := b.CopyFrom(context.Background(), "new.txt", strings.NewReader("new"), model.PutParams{Mode: 0o644}); err != nil {
			t.Errorf("the next upload: %v", err)
		}
	})
}

// S11, the reviewer's probe: an upload stuck in the middle, on a server
// whose file system hangs, was still running 20 s after its cancel. The
// cancel closed the file, and closing it waits behind the transfer. On an
// extra connection only that one is closed; the main one and the other
// transfers go on.
func TestSFTPUploadToAHungServerEndsOnCancel(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			b, g, dir := hangingSFTP(t, parallel)
			err := endsOnCancel(t, g, func(ctx context.Context) error {
				src := &shutAfter{r: io.LimitReader(hangZeros{}, 64<<20), n: 1 << 20, shut: g.close}
				return b.CopyFrom(ctx, "big.bin", src, model.PutParams{Mode: 0o644})
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("CopyFrom: %v, want it canceled", err)
			}
			g.open()
			if parallel > 1 {
				s := sftpInner(t, b)
				if s.main.cut.Load() {
					t.Error("the main connection was closed for a transfer on an extra one")
				}
				s.pool.mu.Lock()
				alive, idle := s.pool.size, len(s.pool.idle)
				s.pool.mu.Unlock()
				if alive != 0 || idle != 0 {
					t.Errorf("the pool keeps the closed connection: %d alive, %d idle", alive, idle)
				}
			}
			ctx := context.Background()
			if err := b.CopyFrom(ctx, "small.txt", strings.NewReader("small"), model.PutParams{Mode: 0o644}); err != nil {
				t.Errorf("the next upload: %v", err)
			}
			if got, err := os.ReadFile(filepath.Join(dir, "small.txt")); err != nil || string(got) != "small" {
				t.Errorf("the next upload wrote %q, %v", got, err)
			}
		})
	}
}

// S11: a download stuck in the middle ignored its cancel the same way: the
// copy closes the reader on cancel, and that waits behind the read.
func TestSFTPDownloadFromAHungServerEndsOnCancel(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			b, g, dir := hangingSFTP(t, parallel)
			if err := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 8<<20), 0o644); err != nil {
				t.Fatal(err)
			}
			err := endsOnCancel(t, g, func(ctx context.Context) error {
				rc, err := b.Open(ctx, "big.bin")
				if err != nil {
					return err
				}
				defer rc.Close()
				defer CancelCloser(ctx, rc)()
				if _, err := io.ReadFull(rc, make([]byte, 64<<10)); err != nil {
					return err
				}
				g.close()
				_, err = io.Copy(io.Discard, rc)
				return err
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("the read: %v, want it canceled", err)
			}
			g.open()
			if parallel > 1 {
				s := sftpInner(t, b)
				if s.main.cut.Load() {
					t.Error("the main connection was closed for a read on an extra one")
				}
				s.pool.mu.Lock()
				alive, idle := s.pool.size, len(s.pool.idle)
				s.pool.mu.Unlock()
				if alive != 0 || idle != 0 {
					t.Errorf("the pool keeps the closed connection: %d alive, %d idle", alive, idle)
				}
			}
			rc, err := b.Open(context.Background(), "big.bin")
			if err != nil {
				t.Fatalf("the next Open: %v", err)
			}
			defer rc.Close()
			if n, err := io.Copy(io.Discard, rc); err != nil || n != 8<<20 {
				t.Errorf("the next download read %d bytes, %v", n, err)
			}
		})
	}
}

// S11: a call the backend stopped says so: its ctx's error, and that the
// server did not answer. Through the wrapper a pane sees the ctx's error
// either way; the backend's own log line is the one that says what
// happened.
func TestSFTPStoppedCallSaysWhy(t *testing.T) {
	dir := t.TempDir()
	g := &answerGate{held: make(chan struct{}, 1)}
	addr := sshtest.StartWith(t, sshtest.Options{SFTP: true, SFTPStream: g.stream})
	b, err := NewSFTPBackend(context.Background(), "sftp://x:pw@"+addr+dir, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	t.Cleanup(g.open)
	g.close()
	err = endsOnCancel(t, g, func(ctx context.Context) error {
		_, err := b.Stat(ctx, "a.txt")
		return err
	})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "did not answer") {
		t.Errorf("a stopped Stat: %v, want the ctx's error and why", err)
	}
}

// S11: a call that ends by itself soon after its ctx ends keeps its
// connection, so a stop costs no new login for each call it ends.
func TestSFTPCallThatEndsByItselfKeepsItsConnection(t *testing.T) {
	b, _, dir := hangingSFTP(t, 1)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rc, err := b.Open(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(rc); err != nil || string(got) != "hello" {
		t.Fatalf("read %q, %v", got, err)
	}
	cancel()
	time.Sleep(sftpGrace / 5)
	rc.Close()
	if sftpInner(t, b).main.cut.Load() {
		t.Error("the connection was closed under a call that had ended")
	}
}

// S11: closing the backend waited for a call the server did not answer:
// pkg/sftp's Close waits until the server ends the session. It ends the
// call now. (A closed wrapper still dials for the call's retries: F23.)
func TestSFTPCloseEndsACallTheServerDoesNotAnswer(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	b, g, _ := hangingSFTP(t, 1)
	g.close()
	done := make(chan error, 1)
	go func() {
		_, err := b.Stat(context.Background(), "a.txt")
		done <- err
	}()
	select {
	case <-g.held:
	case <-time.After(10 * time.Second):
		g.open()
		t.Fatal("the server was not held")
	}
	closed := make(chan struct{})
	go func() {
		CloseBackend(b)
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(hangLimit):
		t.Errorf("Close had not returned after %v", hangLimit)
		g.open()
		<-closed
	}
	select {
	case <-done:
	case <-time.After(hangLimit):
		t.Errorf("the call had not ended %v after Close", hangLimit)
		g.open()
		<-done
	}
}

// cutStub is a connection that can close itself, as SFTP's does.
type cutStub struct {
	connStub
	cut   atomic.Bool
	lists atomic.Int32
}

func (c *cutStub) connCut() bool { return c.cut.Load() }

func (c *cutStub) List(context.Context, string) ([]model.FileEntry, error) {
	c.lists.Add(1)
	return nil, nil
}

// S11: once a backend closed its own connection to stop a call, the next
// call goes to a new one; it used to fail first on the closed one.
func TestLazyReconnectsWhenTheBackendClosedItsConnection(t *testing.T) {
	var conns []*cutStub
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		c := &cutStub{}
		conns = append(conns, c)
		return c, nil
	})
	ctx := context.Background()
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	conns[0].cut.Store(true)
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 || conns[0].lists.Load() != 1 || conns[1].lists.Load() != 1 || !conns[0].closed.Load() {
		t.Errorf("%d connections; lists on the closed one %d; closed %v", len(conns), conns[0].lists.Load(), conns[0].closed.Load())
	}
}
