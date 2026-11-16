package transport

import (
	"context"
	"errors"
	"io"
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
