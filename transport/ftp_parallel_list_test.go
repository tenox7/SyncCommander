package transport

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// openFakeFTPN is openFakeFTP with n parallel transfers: n extras.
func openFakeFTPN(t *testing.T, f *fakeFTPD, n int) *FTPBackend {
	t.Helper()
	b, err := NewFTPBackend(context.Background(), "ftp://u:p@"+f.start(t)+"/", false, n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// lastListConn is the connection the last LIST came on.
func lastListConn(f *fakeFTPD) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.listConns) == 0 {
		return 0
	}
	return f.listConns[len(f.listConns)-1]
}

// waitForLog waits up to 10 s for a log line after mark that holds what.
func waitForLog(t *testing.T, mark int, what string) {
	t.Helper()
	for range 1000 {
		if slices.ContainsFunc(Log.Lines()[mark:], func(l string) bool { return strings.Contains(l, what) }) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %q in the log after 10 s", what)
}

// listWithin is b.List, failing the test when it runs more than 10 s.
func listWithin(t *testing.T, ctx context.Context, b *FTPBackend, dir string) ([]model.FileEntry, error) {
	t.Helper()
	type result struct {
		got []model.FileEntry
		err error
	}
	done := make(chan result, 1)
	go func() {
		got, err := b.List(ctx, dir)
		done <- result{got, err}
	}()
	select {
	case r := <-done:
		return r.got, r.err
	case <-time.After(10 * time.Second):
		t.Fatalf("List(%q) still runs after 10 s", dir)
		return nil, nil
	}
}

// L15: every FTP listing ran on the primary connection, one at a time: a
// scan of 8 folders at once listed them in turn. A listing now takes an
// extra when the primary is busy, and the listings run side by side.
func TestFTPListingsRunSideBySide(t *testing.T) {
	ls := map[string][]string{"/": {lsLine(".", true), lsLine("..", true)}}
	for d := range 4 {
		dir := fmt.Sprintf("d%d", d)
		ls["/"] = append(ls["/"], lsLine(dir, true))
		ls["/"+dir] = []string{lsLine(".", true), lsLine("..", true)}
		for i := range 5 {
			ls["/"+dir] = append(ls["/"+dir], lsLine(fmt.Sprintf("f%d.txt", i), false))
		}
	}
	f := &fakeFTPD{ls: ls, listPace: 20 * time.Millisecond}
	b := openFakeFTPN(t, f, 4)
	ctx := context.Background()
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if n := lastListConn(f); n != 1 {
		t.Errorf("a listing with the primary free ran on connection %d, not on the primary", n)
	}
	var wg sync.WaitGroup
	for d := range 4 {
		wg.Go(func() {
			if got, err := b.List(ctx, fmt.Sprintf("d%d", d)); err != nil || len(got) != 5 {
				t.Errorf("List(d%d) = %d entries, %v; want 5", d, len(got), err)
			}
		})
	}
	wg.Wait()
	f.mu.Lock()
	most := f.mostListing
	f.mu.Unlock()
	if most < 2 {
		t.Errorf("four listings at once: the server sent at most %d at a time", most)
	}
}

// A listing on an extra reads its stream as one on the primary does: a
// line sc cannot read fails the listing, where a mirror would have taken
// the entry for gone.
func TestFTPListingOnAnExtraChecksItsLines(t *testing.T) {
	f := &fakeFTPD{ls: map[string][]string{"/": {
		lsLine(".", true), lsLine("..", true), lsLine("a.txt", false), "this is no listing line",
	}}}
	b := openFakeFTPN(t, f, 2)
	ctx := context.Background()
	if err := b.lockPrimary(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.unlockPrimary()
	_, err := listWithin(t, ctx, b, "")
	if n := lastListConn(f); n == 1 || n == 0 {
		t.Fatalf("with the primary held, the listing ran on connection %d", n)
	}
	if err == nil || !strings.Contains(err.Error(), "cannot read") {
		t.Errorf("a listing on an extra with a line sc cannot read: %v; want it to fail", err)
	}
}

// An extra the server ended fails the listing on it; the listing then
// runs on the primary, and the extra is dropped, so the next listing gets
// a new one.
func TestFTPListingOnADeadExtraRunsOnThePrimary(t *testing.T) {
	f := &fakeFTPD{ls: map[string][]string{"/": {lsLine(".", true), lsLine("..", true), lsLine("a.txt", false)}}}
	b := openFakeFTPN(t, f, 1)
	ctx := context.Background()
	if err := b.lockPrimary(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := listWithin(t, ctx, b, ""); err != nil {
		b.unlockPrimary()
		t.Fatal(err)
	}
	extra := lastListConn(f)
	f.kill(extra)
	mark := len(Log.Lines())
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		got, err := b.List(ctx, "")
		done <- result{len(got), err}
	}()
	waitForLog(t, mark, "an extra connection is gone: dropped")
	b.unlockPrimary()
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Errorf("List after its extra died = %d entries, %v; want a.txt", r.n, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the listing still waits 10 s after the primary was let go")
	}
	if n := lastListConn(f); n != 1 {
		t.Errorf("the listing after the dead extra ran on connection %d, not on the primary", n)
	}
	if err := b.lockPrimary(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.unlockPrimary()
	if _, err := listWithin(t, ctx, b, ""); err != nil {
		t.Fatal(err)
	}
	if n := lastListConn(f); n == 1 || n == extra {
		t.Errorf("with the primary held, the listing after the dead extra ran on connection %d; want a new extra", n)
	}
}

// A listing waits for no extra a transfer holds, only for the primary, and
// the time it waits is not the listing's: the time for a listing ran out
// while it waited for the lock.
func TestFTPListingWaitsForThePrimaryOnly(t *testing.T) {
	f := &fakeFTPD{ls: map[string][]string{"/": {lsLine(".", true), lsLine("..", true), lsLine("a.txt", false)}}}
	b := openFakeFTPN(t, f, 1)
	ctx := context.Background()
	_, release, ok := b.acquireExtra(ctx, true) // the one extra, as a transfer holds it
	if !ok {
		t.Fatal("no extra")
	}
	defer release(nil)
	for _, limit := range []time.Duration{0, 300 * time.Millisecond} {
		if err := b.lockPrimary(ctx); err != nil {
			t.Fatal(err)
		}
		time.AfterFunc(500*time.Millisecond, b.unlockPrimary)
		lctx, cancel := ctx, context.CancelFunc(func() {})
		if limit > 0 {
			lctx, cancel = model.WithListTimeout(ctx, limit)
		}
		start := time.Now()
		got, err := listWithin(t, lctx, b, "")
		cancel()
		if err != nil || len(got) != 1 {
			t.Errorf("List with the extra held, the primary let go after 500 ms, %v for the listing: %d entries, %v (after %v)", limit, len(got), err, time.Since(start).Round(time.Millisecond))
		}
	}
}

// A server at its limit turns the extra away. The listing runs on the
// primary, and the refusal is a warning: an error would open the log on
// every scan of a server that takes few connections, which went fine. The
// copies come down to what it took (ConnLimit).
func TestFTPListingOnARefusedExtraIsNoError(t *testing.T) {
	f := &fakeFTPD{
		ls:     map[string][]string{"/": {lsLine(".", true), lsLine("..", true), lsLine("a.txt", false)}},
		refuse: func(n int) bool { return n > 1 },
	}
	b := openFakeFTPN(t, f, 2)
	ctx := context.Background()
	if err := b.lockPrimary(ctx); err != nil {
		t.Fatal(err)
	}
	mark := len(Log.Lines())
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		got, err := b.List(ctx, "")
		done <- result{len(got), err}
	}()
	waitForLog(t, mark, "took no more connections")
	b.unlockPrimary()
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Errorf("List with the extra refused = %d entries, %v; want a.txt", r.n, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the listing still waits 10 s after the primary was let go")
	}
	if n := lastListConn(f); n != 1 {
		t.Errorf("with the extra refused, the listing ran on connection %d, not on the primary", n)
	}
	if n := b.ConnLimit(); n != 1 {
		t.Errorf("ConnLimit %d with every extra refused; want 1", n)
	}
	for _, l := range Log.Lines()[mark:] {
		if strings.Contains(l, "took no more connections") && !strings.Contains(l, " ftp WARN ") {
			t.Errorf("a listing's refused extra is logged as more than a warning: %q", l)
		}
	}
}
