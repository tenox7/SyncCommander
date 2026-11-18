package ui

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// slowClose is a backend whose Close waits until it is let go.
type slowClose struct {
	model.Backend
	letGo  chan struct{}
	closed atomic.Bool
}

func (b *slowClose) Close() error {
	<-b.letGo
	b.closed.Store(true)
	return b.Backend.Close()
}

// U25: a backend a reopen replaced closes in the background, and the
// program ended without waiting for it: an rclone remote replaced just
// before quitting had its batched uploads cut off. Close now waits for
// it, and the reopen still does not.
func TestModelCloseWaitsForReplacedBackends(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	old := &slowClose{Backend: m.right.backend, letGo: make(chan struct{})}
	var once sync.Once
	letGo := func() { once.Do(func() { close(old.letGo) }) }
	t.Cleanup(letGo)
	m.right.backend = old

	reopened := make(chan string, 1)
	go func() {
		_, errMsg := m.reopenBackends(m.left.arg, "fake://small")
		reopened <- errMsg
	}()
	select {
	case errMsg := <-reopened:
		if errMsg != "" {
			t.Fatal(errMsg)
		}
	case <-time.After(5 * time.Second):
		letGo()
		<-reopened
		t.Fatal("the reopen waited for the old backend to close")
	}
	m.cancelOps()

	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a replaced backend was still closing")
	case <-time.After(100 * time.Millisecond):
	}
	letGo()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return once the replaced backend had closed")
	}
	if !old.closed.Load() {
		t.Error("the replaced backend was not closed")
	}
}
