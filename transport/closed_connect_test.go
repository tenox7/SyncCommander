package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// F23: a call that came after Close dialed the server, saw that the
// backend was closed, closed the new connection, and failed with an error
// Retry tries again: up to six connects for one late call. Now a closed
// backend fails a call at once, with no connect and no retry, and the
// error stops a copy (GivesUp) rather than fail each of its files.
func TestAClosedBackendDoesNotConnect(t *testing.T) {
	var dials atomic.Int64
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		dials.Add(1)
		return &connStub{}, nil
	})
	if _, err := b.List(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	b.Close()
	retries := Log.RetryCount()
	_, err := b.List(context.Background(), "")
	if err == nil {
		t.Fatal("a List after Close went through")
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("connected %d times, want once: a List after Close connected again", n)
	}
	if n := Log.RetryCount() - retries; n != 0 {
		t.Errorf("a List after Close was tried %d more times: %v", n, err)
	}
	if !GivesUp(err) {
		t.Errorf("a copy goes on to its next file after %v", err)
	}
}

// F23: a Close while a connect is under way. The connect ends, the
// connection it made is closed at once, and the call fails as after
// Close: no other connect, no retry.
func TestACloseDuringAConnectEndsTheCall(t *testing.T) {
	var dials atomic.Int64
	dialing, release := make(chan struct{}), make(chan struct{})
	conns := make(chan *connStub, 8)
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		if dials.Add(1) == 1 {
			close(dialing)
			<-release
		}
		c := &connStub{}
		conns <- c
		return c, nil
	})
	retries := Log.RetryCount()
	done := make(chan error, 1)
	go func() {
		_, err := b.List(context.Background(), "")
		done <- err
	}()
	<-dialing
	b.Close()
	close(release)
	var err error
	select {
	case err = <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("the List has not ended 60s after Close")
	}
	if err == nil {
		t.Fatal("a List whose connect a Close overtook went through")
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("connected %d times, want once: the call connected again after Close", n)
	}
	if n := Log.RetryCount() - retries; n != 0 {
		t.Errorf("the call was tried %d more times after Close: %v", n, err)
	}
	if !GivesUp(err) {
		t.Errorf("a copy goes on to its next file after %v", err)
	}
	if c := <-conns; !c.closed.Load() {
		t.Error("the connection made after Close is left open")
	}
}
