package transport

import (
	"sync/atomic"
	"testing"
)

// The FTP backend builds its pool with a nil primary and treats a nil acquire
// result as "at capacity → fall back to the serialized primary conn". These
// tests pin that contract, which sftp/scp (non-nil primary) never exercise.

func TestSSHPoolNilPrimaryFallback(t *testing.T) {
	var dialed int64
	type fakeConn struct{ id int64 }
	dial := func() (*fakeConn, error) {
		return &fakeConn{id: atomic.AddInt64(&dialed, 1)}, nil
	}
	var closed int64
	p := newSSHPool[*fakeConn](nil, 2, dial, func(*fakeConn) { atomic.AddInt64(&closed, 1) })

	c1, r1 := p.acquire()
	c2, _ := p.acquire()
	if c1 == nil || c2 == nil {
		t.Fatal("first two acquires should dial extras")
	}

	c3, r3 := p.acquire()
	if c3 != nil {
		t.Fatalf("acquire at capacity must return nil primary, got %v", c3)
	}
	r3() // primary release is a no-op; must not panic

	r1()
	if c4, _ := p.acquire(); c4 == nil {
		t.Fatal("after release a pooled extra should be reusable")
	}
	if got := atomic.LoadInt64(&dialed); got != 2 {
		t.Fatalf("reuse should not dial again: want 2 dials, got %d", got)
	}
}

func TestSSHPoolZeroExtrasAlwaysNil(t *testing.T) {
	dial := func() (*int, error) { v := 0; return &v, nil }
	p := newSSHPool[*int](nil, 0, dial, func(*int) {})
	for i := 0; i < 3; i++ {
		c, r := p.acquire()
		if c != nil {
			t.Fatal("maxExtras=0 (parallel=1) must always return nil primary")
		}
		r()
	}
}
