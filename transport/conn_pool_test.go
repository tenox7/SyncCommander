package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A pool with a nil primary gives nil for "no extra to have": the caller
// falls back to a primary of its own. These tests pin that contract for a
// pool that does not wait; FTP's pool waits (L16): conn_pool_wait_test.go.

func TestConnPoolNilPrimaryFallback(t *testing.T) {
	var dialed int64
	type fakeConn struct{ id int64 }
	dial := func(context.Context) (*fakeConn, error) {
		return &fakeConn{id: atomic.AddInt64(&dialed, 1)}, nil
	}
	var closed int64
	p := newConnPool[*fakeConn]("test", nil, 2, dial, func(*fakeConn) { atomic.AddInt64(&closed, 1) })

	c1, r1 := p.acquire(context.Background())
	c2, _ := p.acquire(context.Background())
	if c1 == nil || c2 == nil {
		t.Fatal("first two acquires should dial extras")
	}

	c3, r3 := p.acquire(context.Background())
	if c3 != nil {
		t.Fatalf("acquire at capacity must return nil primary, got %v", c3)
	}
	r3() // primary release is a no-op; must not panic

	r1()
	if c4, _ := p.acquire(context.Background()); c4 == nil {
		t.Fatal("after release a pooled extra should be reusable")
	}
	if got := atomic.LoadInt64(&dialed); got != 2 {
		t.Fatalf("reuse should not dial again: want 2 dials, got %d", got)
	}
}

func TestConnPoolZeroExtrasAlwaysNil(t *testing.T) {
	dial := func(context.Context) (*int, error) { v := 0; return &v, nil }
	p := newConnPool[*int]("test", nil, 0, dial, func(*int) {})
	for i := 0; i < 3; i++ {
		c, r := p.acquire(context.Background())
		if c != nil {
			t.Fatal("maxExtras=0 (parallel=1) must always return nil primary")
		}
		r()
	}
}

// + and - resize the pool mid-copy: raising lets the next acquires dial,
// lowering leaves busy extras alone and closes them as they come back.
func TestConnPoolResizeWhileInUse(t *testing.T) {
	var dialed, closed atomic.Int64
	dial := func(context.Context) (*int64, error) { n := dialed.Add(1); return &n, nil }
	p := newConnPool[*int64]("test", nil, 1, dial, func(*int64) { closed.Add(1) })

	c1, r1 := p.acquire(context.Background())
	if c, _ := p.acquire(context.Background()); c1 == nil || c != nil {
		t.Fatal("with one extra the second acquire must fall back to primary")
	}
	p.resize(3)
	c2, r2 := p.acquire(context.Background())
	c3, r3 := p.acquire(context.Background())
	if c2 == nil || c3 == nil {
		t.Fatal("raising the limit must let the next acquires dial")
	}

	p.resize(1)
	if closed.Load() != 0 {
		t.Fatal("lowering the limit closed an extra that was still in use")
	}
	r1()
	r2()
	if got := closed.Load(); got != 2 {
		t.Fatalf("the first two extras back above the limit should close, %d closed", got)
	}
	r3()
	if got := closed.Load(); got != 2 {
		t.Fatalf("the extra within the limit should stay pooled, %d closed", got)
	}
	if c, _ := p.acquire(context.Background()); c != c3 {
		t.Fatal("the extra kept after lowering the limit should be reused")
	}
	if got := dialed.Load(); got != 3 {
		t.Fatalf("want 3 dials, got %d", got)
	}
}

// Lowering the limit between copies closes the idle surplus right away rather
// than keeping it until the backend closes.
func TestConnPoolShrinkClosesIdleExtras(t *testing.T) {
	var closed atomic.Int64
	dial := func(context.Context) (*int, error) { v := 0; return &v, nil }
	p := newConnPool[*int]("test", nil, 3, dial, func(*int) { closed.Add(1) })
	var releases []func()
	for range 3 {
		_, r := p.acquire(context.Background())
		releases = append(releases, r)
	}
	for _, r := range releases {
		r()
	}
	p.resize(1)
	deadline := time.Now().Add(5 * time.Second)
	for closed.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := closed.Load(); got != 2 {
		t.Fatalf("shrinking 3 idle extras to 1 closed %d, want 2", got)
	}
	p.close()
	if got := closed.Load(); got != 3 {
		t.Fatalf("close left %d of 3 extras open", 3-got)
	}
}
