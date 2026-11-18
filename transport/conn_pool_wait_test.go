package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// testDialer dials extras for a pool test. refuse turns a dial away, as a
// server at its connection limit does; with gate set, a dial waits for a
// value on it, or for its ctx.
type testDialer struct {
	dials  atomic.Int64
	refuse atomic.Bool
	gate   chan struct{}
}

func (d *testDialer) dial(ctx context.Context) (*int64, error) {
	n := d.dials.Add(1)
	if d.gate != nil {
		select {
		case <-d.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if d.refuse.Load() {
		return nil, errors.New("421 There are too many connections from your internet address.")
	}
	return &n, nil
}

// L16: an acquire that found no idle extra dialed one, and a server at its
// connection limit turned each away: one refused login per file, which an
// ssh server counts against the client. A failed dial now waits before the
// next one, doubling while they fail; one that works ends the wait, and a
// dial its ctx ended says nothing of the server and starts none.
func TestConnPoolWaitsAfterAFailedDial(t *testing.T) {
	primary := new(int64)
	d := &testDialer{}
	d.refuse.Store(true)
	p := newConnPool("test", primary, 2, d.dial, func(*int64) {})
	get := func() *int64 {
		c, release := p.acquire(context.Background())
		release()
		return c
	}
	wait := func() time.Duration {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.dialWait
	}
	over := func() { // as if the wait were over
		p.mu.Lock()
		p.next = time.Time{}
		p.mu.Unlock()
	}
	for range 5 {
		if get() != primary {
			t.Fatal("an acquire the server turned away got an extra")
		}
	}
	if n := d.dials.Load(); n != 1 {
		t.Errorf("%d dials for 5 acquires; want 1: none for a while after one failed", n)
	}
	if w := wait(); w != poolRedial {
		t.Errorf("a wait of %v after a failed dial, want %v", w, poolRedial)
	}
	over()
	get()
	if n, w := d.dials.Load(), wait(); n != 2 || w != 2*poolRedial {
		t.Errorf("once the wait is over: %d dials and a wait of %v; want 2 and %v, doubled", n, w, 2*poolRedial)
	}
	for range 10 {
		over()
		get()
	}
	if w := wait(); w != poolRedialMax {
		t.Errorf("a wait of %v after 12 failed dials, want the most, %v", w, poolRedialMax)
	}
	d.refuse.Store(false)
	over()
	c, release := p.acquire(context.Background()) // kept: the next acquire must dial
	defer release()
	if c == primary {
		t.Fatal("no extra from a server that takes one")
	}
	if w := wait(); w != 0 {
		t.Errorf("a wait of %v after a dial that worked, want none", w)
	}
	d.refuse.Store(true)
	get()
	if w := wait(); w != poolRedial {
		t.Errorf("a wait of %v after a failure that follows one that worked, want %v: it starts again", w, poolRedial)
	}

	t.Run("a dial its ctx ended", func(t *testing.T) {
		d := &testDialer{gate: make(chan struct{})}
		p := newConnPool("test", primary, 2, d.dial, func(*int64) {})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		c, release := p.acquire(ctx)
		release()
		if c != primary {
			t.Fatal("an acquire whose dial its ctx ended got an extra")
		}
		close(d.gate)
		c, release = p.acquire(context.Background())
		defer release()
		if c == primary || d.dials.Load() != 2 {
			t.Errorf("after a dial its ctx ended: %d dials, an extra: %v; want the next acquire to dial at once", d.dials.Load(), c != primary)
		}
	})
}

// L16: FTP's pool waits. An acquire with every extra lent out took the
// primary, where the transfer held up every other call; it now waits for
// an extra to come back. With none lent out there is none to wait for, and
// it takes the primary at once. A wait ends with its ctx, a close of the
// pool, a higher limit, and a failed dial it may have waited on.
func TestConnPoolThatWaits(t *testing.T) {
	open := func(d *testDialer, limit int) *connPool[*int64] {
		p := newConnPool[*int64]("test", nil, limit, d.dial, func(*int64) {})
		p.waits = true
		return p
	}
	acquire := func(p *connPool[*int64], ctx context.Context) <-chan *int64 {
		got := make(chan *int64, 1)
		go func() {
			c, release := p.acquire(ctx)
			got <- c
			release()
		}()
		return got
	}
	waits := func(t *testing.T, got <-chan *int64, what string) {
		t.Helper()
		select {
		case <-got:
			t.Fatalf("%s did not wait", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
	gets := func(t *testing.T, got <-chan *int64, what string) *int64 {
		t.Helper()
		select {
		case c := <-got:
			return c
		case <-time.After(5 * time.Second):
			t.Fatalf("%s still waits", what)
			return nil
		}
	}

	t.Run("every extra lent out", func(t *testing.T) {
		d := &testDialer{}
		p := open(d, 2)
		c1, r1 := p.acquire(context.Background())
		_, r2 := p.acquire(context.Background())
		defer r2()
		got := acquire(p, context.Background())
		waits(t, got, "an acquire with both extras lent out")
		r1()
		if c := gets(t, got, "an acquire after an extra came back"); c != c1 {
			t.Errorf("an acquire after an extra came back got %v, want that extra", c)
		}
		if n := d.dials.Load(); n != 2 {
			t.Errorf("%d dials, want 2", n)
		}
	})
	t.Run("the server takes fewer than the limit", func(t *testing.T) {
		d := &testDialer{}
		p := open(d, 4)
		c1, r1 := p.acquire(context.Background())
		d.refuse.Store(true)
		got := acquire(p, context.Background())
		waits(t, got, "an acquire the server turned away, with an extra lent out")
		r1()
		if c := gets(t, got, "an acquire after the extra came back"); c != c1 {
			t.Errorf("an acquire after the extra came back got %v, want that extra", c)
		}
		if n := d.dials.Load(); n != 2 {
			t.Errorf("%d dials, want 2: the extra, the one turned away", n)
		}
	})
	t.Run("no extra at all", func(t *testing.T) {
		d := &testDialer{}
		d.refuse.Store(true)
		p := open(d, 2)
		for range 3 {
			if c := gets(t, acquire(p, context.Background()), "an acquire with no extra to have"); c != nil {
				t.Errorf("an acquire with no extra to have got %v, want the primary", c)
			}
		}
		if n := d.dials.Load(); n != 1 {
			t.Errorf("%d dials, want 1", n)
		}
	})
	t.Run("a failed dial wakes the acquires that waited for it", func(t *testing.T) {
		d := &testDialer{gate: make(chan struct{})}
		d.refuse.Store(true)
		p := open(d, 1)
		first := acquire(p, context.Background()) // dials, held at the gate
		for d.dials.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		second := acquire(p, context.Background()) // the pool is full: waits
		waits(t, second, "an acquire with the only extra being dialed")
		d.gate <- struct{}{}
		if c := gets(t, first, "the acquire whose dial failed"); c != nil {
			t.Errorf("the acquire whose dial failed got %v, want the primary", c)
		}
		if c := gets(t, second, "the acquire that waited for a dial that failed"); c != nil {
			t.Errorf("the acquire that waited for a dial that failed got %v, want the primary", c)
		}
	})
	t.Run("an extra dropped", func(t *testing.T) {
		p := open(&testDialer{}, 1)
		c1, _ := p.acquire(context.Background())
		got := acquire(p, context.Background())
		waits(t, got, "an acquire with the only extra lent out")
		p.discard(c1)
		if c := gets(t, got, "an acquire when the extra was dropped"); c == nil || c == c1 {
			t.Errorf("an acquire when the extra was dropped got %v, want a new extra", c)
		}
	})
	t.Run("ctx", func(t *testing.T) {
		p := open(&testDialer{}, 1)
		_, r1 := p.acquire(context.Background())
		defer r1()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if c := gets(t, acquire(p, ctx), "an acquire past its deadline"); c != nil {
			t.Errorf("an acquire past its deadline got %v, want the primary", c)
		}
	})
	t.Run("close", func(t *testing.T) {
		p := open(&testDialer{}, 1)
		_, r1 := p.acquire(context.Background())
		defer r1()
		got := acquire(p, context.Background())
		waits(t, got, "an acquire with the only extra lent out")
		p.close()
		if c := gets(t, got, "an acquire when the pool closed"); c != nil {
			t.Errorf("an acquire when the pool closed got %v, want the primary", c)
		}
	})
	t.Run("a higher limit", func(t *testing.T) {
		d := &testDialer{}
		p := open(d, 1)
		c1, r1 := p.acquire(context.Background())
		defer r1()
		got := acquire(p, context.Background())
		waits(t, got, "an acquire with the only extra lent out")
		p.resize(2)
		if c := gets(t, got, "an acquire when the limit went up"); c == nil || c == c1 {
			t.Errorf("an acquire when the limit went up got %v, want a new extra", c)
		}
	})
}
