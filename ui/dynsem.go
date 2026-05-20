package ui

import (
	"context"
	"sync"
)

// dynSem is a counting semaphore with runtime-resizable capacity. Acquire
// blocks until a permit is available or ctx is done. Resize raises or lowers
// the cap; lowering does not preempt — excess holders simply consume the
// surplus on Release until held catches up with cap.
type dynSem struct {
	mu     sync.Mutex
	cap    int
	held   int
	notify chan struct{}
}

func newDynSem(n int) *dynSem {
	if n < 1 {
		n = 1
	}
	return &dynSem{cap: n, notify: make(chan struct{})}
}

func (s *dynSem) Acquire(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.held < s.cap {
			s.held++
			s.mu.Unlock()
			return nil
		}
		ch := s.notify
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *dynSem) Release() {
	s.mu.Lock()
	s.held--
	s.broadcast()
	s.mu.Unlock()
}

func (s *dynSem) Resize(n int) {
	if n < 1 {
		n = 1
	}
	s.mu.Lock()
	s.cap = n
	s.broadcast()
	s.mu.Unlock()
}

func (s *dynSem) broadcast() {
	close(s.notify)
	s.notify = make(chan struct{})
}
