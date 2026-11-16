package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"sc/model"
)

// connReset is what a read or write returns on a connection the peer reset,
// as the net package gives it on this system.
func connReset(op string) error {
	return &net.OpError{Op: op, Net: "tcp", Err: os.NewSyscallError(op, connLostErrnos[0])}
}

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

func (c *connStub) Checksum(context.Context, string, string) (string, error) {
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
			c.sumErr = connReset("read")
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
		_, err := b.Checksum(ctx, "big.bin", "md5")
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

// sumStub checksums like an ssh remote: with the commands its own
// connection's probe found, which runs when it first needs them.
type sumStub struct {
	connStub
	mu      sync.Mutex
	cmds    map[string]string
	dropSum atomic.Bool // the next Checksum finds the connection dead
}

func (s *sumStub) probed() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmds == nil {
		s.cmds = map[string]string{"md5": "md5sum"}
	}
	return s.cmds
}

func (s *sumStub) ChecksumAlgos(context.Context) ([]string, error) {
	s.probed()
	return []string{"md5"}, nil
}

func (s *sumStub) Checksum(_ context.Context, relPath, algo string) (string, error) {
	if s.dropSum.Swap(false) {
		return "", io.ErrUnexpectedEOF
	}
	if s.probed()[algo] == "" {
		return "", fmt.Errorf("sftp: no %q checksum command on the remote", algo)
	}
	return "sum of " + relPath, nil
}

// F6: after one dropped connection, the wrapper dialed a new backend
// object that knew neither the probed commands nor the algorithm: every
// checksum failed from then on, each after its retries. Each call now says
// which algorithm, and a new connection probes its server when it first
// needs the commands (D2).
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
	if got, err := b.ChecksumAlgos(ctx); err != nil || !slices.Equal(got, []string{"md5"}) {
		t.Fatalf("checksum algorithms = %q, %v", got, err)
	}
	if sum, err := b.Checksum(ctx, "a", "md5"); err != nil || sum != "sum of a" {
		t.Fatalf("checksum before the drop = %q, %v", sum, err)
	}
	mu.Lock()
	conns[0].dropSum.Store(true)
	mu.Unlock()
	for _, f := range []string{"b", "c"} {
		if sum, err := b.Checksum(ctx, f, "md5"); err != nil || sum != "sum of "+f {
			t.Errorf("checksum of %s after the drop = %q, %v", f, sum, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 2 {
		t.Errorf("%d connections dialed, want 2", len(conns))
	}
}
