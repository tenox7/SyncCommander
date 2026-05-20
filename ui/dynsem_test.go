package ui

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynSemBasicAcquireRelease(t *testing.T) {
	s := newDynSem(2)
	ctx := context.Background()
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.Acquire(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("third acquire should block")
	case <-time.After(50 * time.Millisecond):
	}

	s.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("third acquire should have unblocked")
	}
}

func TestDynSemResizeUpUnblocks(t *testing.T) {
	s := newDynSem(1)
	ctx := context.Background()
	_ = s.Acquire(ctx)

	done := make(chan struct{})
	go func() {
		_ = s.Acquire(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("should block at cap=1")
	case <-time.After(50 * time.Millisecond):
	}

	s.Resize(2)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resize up should unblock waiter")
	}
}

func TestDynSemResizeDownNoPreemption(t *testing.T) {
	s := newDynSem(4)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		_ = s.Acquire(ctx)
	}
	s.Resize(2)

	done := make(chan struct{})
	go func() {
		_ = s.Acquire(ctx)
		close(done)
	}()

	s.Release()
	select {
	case <-done:
		t.Fatal("acquire should still block: held=3, cap=2")
	case <-time.After(50 * time.Millisecond):
	}
	s.Release()
	select {
	case <-done:
		t.Fatal("acquire should still block: held=2, cap=2")
	case <-time.After(50 * time.Millisecond):
	}
	s.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acquire should unblock: held=1, cap=2")
	}
}

func TestDynSemAcquireCtxCancel(t *testing.T) {
	s := newDynSem(1)
	_ = s.Acquire(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Acquire(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected ctx error")
		}
	case <-time.After(time.Second):
		t.Fatal("ctx cancel did not unblock acquire")
	}
}

func TestDynSemConcurrentResize(t *testing.T) {
	s := newDynSem(2)
	ctx := context.Background()
	var inFlight int64
	var maxSeen int64
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		if err := s.Acquire(ctx); err != nil {
			return
		}
		n := atomic.AddInt64(&inFlight, 1)
		for {
			cur := atomic.LoadInt64(&maxSeen)
			if n <= cur || atomic.CompareAndSwapInt64(&maxSeen, cur, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
		s.Release()
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		for i := 0; i < 10; i++ {
			s.Resize(4)
			time.Sleep(2 * time.Millisecond)
			s.Resize(1)
			time.Sleep(2 * time.Millisecond)
		}
		s.Resize(8)
	}()

	wg.Wait()
	if maxSeen > 8 {
		t.Fatalf("inFlight %d exceeded highest cap ever set (8)", maxSeen)
	}
}
