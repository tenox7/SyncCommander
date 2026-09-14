package transfer

import (
	"context"
	"sync"
)

// DynSem is a counting semaphore with runtime-resizable capacity. Acquire
// blocks until a permit is available or ctx is done. Resize raises or lowers
// the cap; lowering does not preempt, excess holders simply consume the
// surplus on Release until held catches up with cap.
type DynSem struct {
	mu     sync.Mutex
	cap    int
	held   int
	notify chan struct{}
}

func NewDynSem(n int) *DynSem {
	return &DynSem{cap: max(n, 1), notify: make(chan struct{})}
}

func (s *DynSem) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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

func (s *DynSem) Release() {
	s.mu.Lock()
	s.held--
	s.broadcast()
	s.mu.Unlock()
}

func (s *DynSem) Resize(n int) {
	s.mu.Lock()
	s.cap = max(n, 1)
	s.broadcast()
	s.mu.Unlock()
}

func (s *DynSem) broadcast() {
	close(s.notify)
	s.notify = make(chan struct{})
}
