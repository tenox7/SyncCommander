package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// silentDaemon accepts connections and answers nothing, as the local end of
// an ssh -L whose far end is dead does. It returns its host:port.
func silentDaemon(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go io.Copy(io.Discard, c)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

func setRsyncIdle(t *testing.T, d time.Duration) {
	t.Helper()
	old := rsyncIdle.n.Load()
	rsyncIdle.set(d)
	t.Cleanup(func() { rsyncIdle.n.Store(old) })
}

// endsWithin runs call and returns its error, or fails the test when call
// has not ended after d; then it cancels call and waits for it.
func endsWithin(t *testing.T, d time.Duration, call func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- call(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Errorf("still waiting for the daemon after %v", d)
		cancel()
		return <-done
	}
}

// R17: only the dial of a daemon run had a deadline. A stat, a listing, a
// mkdir, a delete or a checksum fetch on a daemon that went silent waited
// until a cancel; under a mirror, the copy hung. Now a run that no stall
// guard watches fails once the daemon has sent nothing for -rsync-timeout.
func TestADaemonRunFailsWhenTheDaemonGoesSilent(t *testing.T) {
	setRsyncIdle(t, time.Second)
	b, err := NewRsyncBackend("rsync://" + silentDaemon(t) + "/mod")
	if err != nil {
		t.Fatal(err)
	}
	calls := []struct {
		name string
		call func(context.Context) error
	}{
		{"stat", func(ctx context.Context) error { _, err := b.Stat(ctx, "a.txt"); return err }},
		{"list", func(ctx context.Context) error { _, err := b.List(ctx, ""); return err }},
		{"mkdir", func(ctx context.Context) error { return b.Mkdir(ctx, "d", 0o755) }},
		{"remove", func(ctx context.Context) error { return b.Remove(ctx, "a.txt") }},
		{"checksum of a 1-byte file", func(ctx context.Context) error {
			_, err := b.Checksum(model.WithHashBytes(ctx, 1), "a.txt", "md4")
			return err
		}},
	}
	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			err := endsWithin(t, 20*time.Second, c.call)
			if !errors.Is(err, ErrStall) || !strings.Contains(err.Error(), "-rsync-timeout") {
				t.Errorf("err = %v, want a stall that names -rsync-timeout", err)
			}
		})
	}
}

// R17: with -c the daemon hashes each file it lists before it sends its
// entry, and says nothing while it does. A run gets the time its caller
// says the hash takes, then -rsync-timeout; one whose caller says nothing
// is not cut: its time cannot be told.
func TestADaemonChecksumRunGetsTheTimeItsHashTakes(t *testing.T) {
	setRsyncIdle(t, time.Second)
	b, err := NewRsyncBackend("rsync://" + silentDaemon(t) + "/mod")
	if err != nil {
		t.Fatal(err)
	}
	hashed := int64(3 * minHashRate) // HashTime: 3s
	start := time.Now()
	err = endsWithin(t, 30*time.Second, func(ctx context.Context) error {
		_, err := b.PrefetchChecksums(model.WithHashBytes(ctx, hashed), "", "md4", true)
		return err
	})
	if el := time.Since(start); el < 3*time.Second {
		t.Errorf("the fetch gave up after %v, before the 3s its hash takes", el.Round(time.Millisecond))
	}
	if !errors.Is(err, ErrStall) {
		t.Errorf("err = %v, want a stall", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Checksum(ctx, "a.txt", "md4")
		done <- err
	}()
	select {
	case err := <-done:
		t.Errorf("a checksum whose hash time is not told ended after less than 3s: %v", err)
	case <-time.After(3 * time.Second):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("the checksum did not end on cancel")
	}
}

// A transfer runs under the copy's stall guard: that guard watches the
// daemon run, with the silences the copy expects, and -rsync-timeout does
// not cut it first.
func TestADaemonRunUnderAStallGuardIsTheGuards(t *testing.T) {
	setRsyncIdle(t, time.Second)
	b, err := NewRsyncBackend("rsync://" + silentDaemon(t) + "/mod")
	if err != nil {
		t.Fatal(err)
	}
	var moved atomic.Int64
	start := time.Now()
	err = endsWithin(t, 30*time.Second, func(ctx context.Context) error {
		return WithStallGuard(ctx, &moved, 4*time.Second, func(ctx context.Context) error {
			_, err := b.List(ctx, "")
			return err
		})
	})
	if el := time.Since(start); el < 3*time.Second {
		t.Errorf("the run ended after %v, before its stall guard's 4s", el.Round(time.Millisecond))
	}
	if !errors.Is(err, ErrStall) {
		t.Errorf("err = %v, want the guard's stall", err)
	}
}
