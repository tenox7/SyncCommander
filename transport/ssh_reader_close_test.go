package transport

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// S7: a copy closes its source reader itself, and a watcher closes it again
// when the copy is cancelled or stalls (transfer/resume.go, ui/diffload.go:
// defer r.Close(); defer CancelCloser(ctx, r)()). The second Close of a
// shell reader closed a closed channel: "panic: close of closed channel" in
// a copy worker took sc down. It also gave its pooled connection back twice.
func TestShellReaderClosesTwice(t *testing.T) {
	mod := t.TempDir()
	writeTestFile(t, mod, "big.bin", strings.Repeat("x", 1<<20))
	be, err := OpenBackend("ssh://x:pw@"+startExecSSHServer(t)+mod, true, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(be)
	b, ok := be.(*SCPBackend)
	if !ok {
		t.Fatalf("the test server has no SFTP, want the shell backend, got %T", be)
	}
	idle := func() int {
		b.pool.mu.Lock()
		defer b.pool.mu.Unlock()
		return len(b.pool.idle)
	}

	// Closed twice in a row.
	r, err := b.Open(context.Background(), "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(r, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	r.Close()
	r.Close()
	if n := idle(); n != 1 {
		t.Errorf("after two Closes the pool holds %d idle connections, want 1: one was given back twice", n)
	}

	// Cancelled: the watcher closes it, then the deferred Close runs.
	ctx, cancel := context.WithCancel(context.Background())
	r, err = b.Open(ctx, "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	stop := CancelCloser(ctx, r)
	if _, err := io.ReadFull(r, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := r.Read(make([]byte, 1<<16)); err != nil {
			break // the watcher closed it
		}
		if time.Now().After(deadline) {
			t.Fatal("the reader still reads 5 s after the cancel")
		}
	}
	stop()
	r.Close()

	// Closed from two goroutines at once (run with -race).
	r, err = b.Open(context.Background(), "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); r.Close() }()
	}
	wg.Wait()
}

// The stop func of CancelCloser may run more than once.
func TestCancelCloserStopTwice(t *testing.T) {
	stop := CancelCloser(context.Background(), io.NopCloser(nil))
	stop()
	stop()
}
