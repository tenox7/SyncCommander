package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// runGate opens once; the end of the test opens it too, so no preload
// waits on it after.
type runGate struct {
	ch   chan struct{}
	once sync.Once
}

func newRunGate(t *testing.T) *runGate {
	g := &runGate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *runGate) open() { g.once.Do(func() { close(g.ch) }) }

// gatedRun is a preload that sends nothing until g opens, then does what
// end says.
func gatedRun(g *runGate, end func(emit func(string, []model.FileEntry), complete func(string)) error) recursiveLister {
	return func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		select {
		case <-g.ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		return end(emit, complete)
	}
}

type callerKey struct{}

type serveOut struct {
	entries   []model.FileEntry
	err       error
	lived     bool
	liveLeft  time.Duration // time left on the live listing's ctx
	liveValue any           // its callerKey value
}

// serveWith runs c.serve(ctx, dir) in a goroutine. Its live listing fails,
// as a real one does, when its ctx has ended.
func serveWith(c *listCache, ctx context.Context, dir string) <-chan serveOut {
	ch := make(chan serveOut, 1)
	go func() {
		var o serveOut
		o.entries, o.err = c.serve(ctx, dir, func(lctx context.Context, _ string) ([]model.FileEntry, error) {
			o.lived = true
			o.liveValue = lctx.Value(callerKey{})
			if dl, ok := lctx.Deadline(); ok {
				o.liveLeft = time.Until(dl)
			}
			if err := lctx.Err(); err != nil {
				return nil, err
			}
			return []model.FileEntry{{Name: "live"}}, nil
		})
		ch <- o
	}()
	return ch
}

func served(t *testing.T, ch <-chan serveOut) serveOut {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(5 * time.Second):
		t.Fatal("serve still waits 5s later")
		return serveOut{}
	}
}

func stillWaits(t *testing.T, ch <-chan serveOut) {
	t.Helper()
	select {
	case o := <-ch:
		t.Fatalf("serve did not wait for the preload: err %v, live %v", o.err, o.lived)
	case <-time.After(100 * time.Millisecond):
	}
}

// W-M2: a List that waited for the preload ignored its own context. A copy
// that was stopped stayed blocked until the scan's whole listing ended.
// Now a stop ends the wait.
func TestListCacheWaitEndsOnTheCallersStop(t *testing.T) {
	c := newListCache()
	g := newRunGate(t)
	c.start(context.Background(), "", gatedRun(g, func(func(string, []model.FileEntry), func(string)) error { return nil }))
	ctx, stop := context.WithCancel(context.Background())
	ch := serveWith(c, ctx, "a")
	stillWaits(t, ch)
	stop()
	if o := served(t, ch); !errors.Is(o.err, context.Canceled) || o.lived {
		t.Errorf("serve = err %v, live %v; want the stop, and no live listing", o.err, o.lived)
	}
}

// W-M2: a stop under the scan's listing timeout ends the wait too, also
// after that timeout ran out.
func TestListCacheWaitEndsOnAStopAfterTheListingTimeout(t *testing.T) {
	c := newListCache()
	g := newRunGate(t)
	c.start(context.Background(), "", gatedRun(g, func(func(string, []model.FileEntry), func(string)) error { return nil }))
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := model.WithListTimeout(parent, 100*time.Millisecond)
	defer cancel()
	ch := serveWith(c, ctx, "a")
	time.Sleep(300 * time.Millisecond) // the listing's timeout ran out
	stillWaits(t, ch)
	stop()
	if o := served(t, ch); !errors.Is(o.err, context.Canceled) || o.lived {
		t.Errorf("serve = err %v, live %v; want the stop, and no live listing", o.err, o.lived)
	}
}

// The wait is not the listing's time: a listing timeout that runs out
// while the preload is on its way to the folder does not end the wait.
func TestListCacheWaitOutlivesTheListingTimeout(t *testing.T) {
	c := newListCache()
	g := newRunGate(t)
	c.start(context.Background(), "", gatedRun(g, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}})
		complete("a")
		return nil
	}))
	ctx, cancel := model.WithListTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ch := serveWith(c, ctx, "a")
	time.Sleep(300 * time.Millisecond)
	stillWaits(t, ch)
	g.open()
	if o := served(t, ch); o.err != nil || o.lived || len(o.entries) != 1 {
		t.Errorf("serve = %d entries, err %v, live %v; want the preload's 1 entry", len(o.entries), o.err, o.lived)
	}
}

// W-M2: the live listing after a wait ran on the caller's ctx, whose
// timeout the wait had used up. A preload that failed after the listing
// timeout failed each folder that waited for it, though a live listing
// worked. Now that listing gets the whole timeout, and the caller's
// values.
func TestListCacheListingAfterAWaitGetsItsWholeTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	c := newListCache()
	g := newRunGate(t)
	c.start(context.Background(), "", gatedRun(g, func(func(string, []model.FileEntry), func(string)) error {
		return errors.New("the link was reset")
	}))
	parent := context.WithValue(context.Background(), callerKey{}, "the caller's")
	ctx, cancel := model.WithListTimeout(parent, timeout)
	defer cancel()
	ch := serveWith(c, ctx, "a")
	time.Sleep(timeout + 200*time.Millisecond)
	g.open()
	o := served(t, ch)
	if o.err != nil || !o.lived || len(o.entries) != 1 {
		t.Fatalf("serve = %d entries, err %v, live %v; want the live listing's 1 entry", len(o.entries), o.err, o.lived)
	}
	if o.liveLeft < timeout/2 {
		t.Errorf("the live listing had %v left, want about %v", o.liveLeft.Round(time.Millisecond), timeout)
	}
	if o.liveValue != "the caller's" {
		t.Errorf("the live listing's ctx holds %v, want the caller's value", o.liveValue)
	}
}

// W-M2: a List outside the running preload's scope waited for the whole
// run, then listed live. Now it lists live at once; one inside the scope
// still waits for the run.
func TestListCacheListOutsideTheRunsScopeDoesNotWait(t *testing.T) {
	c := newListCache()
	g := newRunGate(t)
	c.start(context.Background(), "a", gatedRun(g, func(func(string, []model.FileEntry), func(string)) error { return nil }))
	inside := serveWith(c, context.Background(), "a/x")
	stillWaits(t, inside)
	for _, dir := range []string{"b", "ab", ""} {
		if o := served(t, serveWith(c, context.Background(), dir)); o.err != nil || !o.lived {
			t.Errorf("%q, outside the run's scope: err %v, live %v; want a live listing", dir, o.err, o.lived)
		}
	}
	stillWaits(t, inside)
	g.open()
	if o := served(t, inside); o.err != nil {
		t.Errorf("a/x: %v", o.err)
	}
}

// W-M2: a fresh run over another scope left the waiters of the old run
// waiting for the new one; then it stopped the old run, and they listed
// live. Now the old run goes on beside the new one (R9): its waiter waits
// for it alone and gets its folder from it.
func TestListCacheFreshRunElsewhereLeavesAWaiterItsRun(t *testing.T) {
	c := newListCache()
	g1, g2 := newRunGate(t), newRunGate(t)
	c.start(context.Background(), "a", gatedRun(g1, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a/y", []model.FileEntry{{Name: "y1"}})
		complete("a/y")
		return nil
	}))
	waiter := serveWith(c, context.Background(), "a/y")
	stillWaits(t, waiter)
	c.start(model.WithFreshListing(context.Background()), "b", gatedRun(g2, func(func(string, []model.FileEntry), func(string)) error { return nil }))
	stillWaits(t, waiter)
	g1.open()
	if o := served(t, waiter); o.err != nil || o.lived || len(o.entries) != 1 || o.entries[0].Name != "y1" {
		t.Errorf("a/y: %+v, err %v, live %v; want its run's [y1]", o.entries, o.err, o.lived)
	}
}
