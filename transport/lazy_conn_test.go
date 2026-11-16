package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"sc/model"
)

// connStub is one connection the lazy wrapper dials.
type connStub struct {
	copyStub
	closed   atomic.Bool
	listErr  atomic.Pointer[error] // List returns it once
	sumGate  chan struct{}         // when set, Checksum waits on it, then fails with sumErr
	sumEnter chan struct{}
	sumErr   error
}

func (c *connStub) List(context.Context, string) ([]model.FileEntry, error) {
	if e := c.listErr.Swap(nil); e != nil {
		return nil, *e
	}
	return nil, nil
}

func (c *connStub) Checksum(context.Context, string) (string, error) {
	if c.sumGate != nil {
		close(c.sumEnter)
		<-c.sumGate
		return "", c.sumErr
	}
	return "sum", nil
}

func (c *connStub) Close() error { c.closed.Store(true); return nil }

// F8: a call that failed late on an old connection, after another call had
// found it dead and the wrapper had dialed a new one, closed the new one,
// which was healthy: every call on it failed.
func TestLateErrorLeavesTheNewConnectionAlone(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	var mu sync.Mutex
	var conns []*connStub
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		mu.Lock()
		defer mu.Unlock()
		c := &connStub{}
		if len(conns) == 0 {
			c.sumGate, c.sumEnter = make(chan struct{}), make(chan struct{})
			c.sumErr = errors.New("read tcp 10.0.0.2:50000->10.0.0.1:22: read: connection reset by peer")
		}
		conns = append(conns, c)
		return c, nil
	})
	ctx := context.Background()
	if _, err := b.List(ctx, ""); err != nil { // dials connection 1
		t.Fatal(err)
	}
	slow := make(chan error, 1)
	go func() { // a slow call on connection 1
		_, err := b.Checksum(ctx, "big.bin")
		slow <- err
	}()
	<-conns[0].sumEnter

	// Connection 1 dies under another call; the next call dials connection 2.
	eof := error(io.ErrUnexpectedEOF)
	conns[0].listErr.Store(&eof)
	if _, err := b.List(ctx, ""); err == nil {
		t.Fatal("List on the dead connection did not fail")
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	second := conns[1]
	mu.Unlock()

	// The slow call now fails on connection 1.
	close(conns[0].sumGate)
	<-slow
	if second.closed.Load() {
		t.Error("the late error from connection 1 closed connection 2, which was healthy")
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Errorf("List after the late error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 2 {
		t.Errorf("%d connections dialed, want 2", len(conns))
	}
}

// sumStub checksums like an ssh remote: only after a probe found the
// commands and the scanner set the algorithm.
type sumStub struct {
	connStub
	cmds    map[string]string
	algo    string
	dropSum atomic.Bool // the next Checksum finds the connection dead
}

func (s *sumStub) ProbeChecksums() []string {
	s.cmds = map[string]string{"md5": "md5sum"}
	return []string{"md5"}
}

func (s *sumStub) SetChecksumAlgo(algo string) { s.algo = algo }

func (s *sumStub) Checksum(_ context.Context, relPath string) (string, error) {
	if s.dropSum.Swap(false) {
		return "", io.ErrUnexpectedEOF
	}
	if s.cmds[s.algo] == "" {
		return "", fmt.Errorf("sftp: no %q checksum command on the remote", s.algo)
	}
	return "sum of " + relPath, nil
}

// F6: after one dropped connection, the wrapper dialed a new backend
// object that knew neither the probed commands nor the algorithm: every
// checksum failed from then on, each after its retries.
func TestChecksumsWorkAfterAReconnect(t *testing.T) {
	SetMaxRetries(1)
	defer SetMaxRetries(5)
	var mu sync.Mutex
	var conns []*sumStub
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		mu.Lock()
		defer mu.Unlock()
		s := &sumStub{}
		conns = append(conns, s)
		return s, nil
	})
	ctx := context.Background()
	p := b.(model.ChecksumProber)
	if got := p.ProbeChecksums(); !slices.Equal(got, []string{"md5"}) {
		t.Fatalf("probe = %q", got)
	}
	p.SetChecksumAlgo("md5")
	if sum, err := b.Checksum(ctx, "a"); err != nil || sum != "sum of a" {
		t.Fatalf("checksum before the drop = %q, %v", sum, err)
	}
	mu.Lock()
	conns[0].dropSum.Store(true)
	mu.Unlock()
	for _, f := range []string{"b", "c"} {
		if sum, err := b.Checksum(ctx, f); err != nil || sum != "sum of "+f {
			t.Errorf("checksum of %s after the drop = %q, %v", f, sum, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 2 {
		t.Errorf("%d connections dialed, want 2", len(conns))
	}
}
