package transport

import "sync"

// connPool is a lazy connection pool for backends whose transport handles one
// transfer at a time (ssh sessions, sftp clients, ftp control connections).
// The backend's primary connection stays separate and handles metadata ops;
// the pool supplies extras for heavy ops (CopyFrom, AppendFrom, Open, OpenAt)
// so each parallel copy worker gets its own connection.
//
// Extras are dialed on demand up to maxExtras. acquire returns a pooled
// extra if one is idle, dials a new one if capacity remains, or falls back
// to primary when at capacity. With maxExtras=0 the pool is a no-op and
// every acquire returns primary — matching the pre-pool single-conn
// behavior for --parallel=1. resize moves the limit while copies run.
type connPool[T any] struct {
	primary   T
	idle      []T // released extras, oldest first
	dial      func() (T, error)
	closeFn   func(T)
	mu        sync.Mutex
	size      int // extras alive: idle, lent out or being dialed
	maxExtras int
	closed    bool
}

func newConnPool[T any](primary T, maxExtras int, dial func() (T, error), closeFn func(T)) *connPool[T] {
	return &connPool[T]{primary: primary, dial: dial, closeFn: closeFn, maxExtras: max(maxExtras, 0)}
}

// acquire returns a connection and a release function. The release MUST be
// called exactly once. Returns primary (with a no-op release) when the
// pool is full, dial fails, or the pool has been closed.
func (p *connPool[T]) acquire() (T, func()) {
	if p == nil {
		var zero T
		return zero, func() {}
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return p.primary, func() {}
	}
	if c, ok := p.popIdle(); ok {
		p.mu.Unlock()
		return c, func() { p.release(c) }
	}
	if p.size < p.maxExtras {
		p.size++
		p.mu.Unlock()
		c, err := p.dial()
		if err != nil {
			p.mu.Lock()
			p.size--
			p.mu.Unlock()
			return p.primary, func() {}
		}
		return c, func() { p.release(c) }
	}
	p.mu.Unlock()
	return p.primary, func() {}
}

// popIdle takes the oldest idle extra; the caller holds mu.
func (p *connPool[T]) popIdle() (T, bool) {
	var zero T
	if len(p.idle) == 0 {
		return zero, false
	}
	c := p.idle[0]
	p.idle[0] = zero
	p.idle = p.idle[1:]
	return c, true
}

// release returns conn c to the pool. If close has already run, or the pool
// was shrunk below the extras it holds, closes c inline instead: that is how
// a lowered limit takes effect as transfers finish, and it keeps conns
// acquired before close out of the abandoned idle list.
func (p *connPool[T]) release(c T) {
	p.mu.Lock()
	if p.closed || p.size > p.maxExtras {
		p.size--
		p.mu.Unlock()
		p.closeFn(c)
		return
	}
	p.idle = append(p.idle, c)
	p.mu.Unlock()
}

// resize sets how many extras the pool may hold. Raising it lets the next
// acquires dial. Lowering it never takes a connection away from a running
// transfer: idle surplus closes now, busy surplus when it is released.
func (p *connPool[T]) resize(maxExtras int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.maxExtras = max(maxExtras, 0)
	var surplus []T
	for !p.closed && p.size > p.maxExtras {
		c, ok := p.popIdle()
		if !ok {
			break
		}
		surplus = append(surplus, c)
		p.size--
	}
	p.mu.Unlock()
	// Closing talks to the server (FTP sends QUIT) and the caller is the UI's
	// key handler, so it must not wait for that.
	if len(surplus) > 0 {
		go func() {
			for _, c := range surplus {
				p.closeFn(c)
			}
		}()
	}
}

// close marks the pool closed and closes every idle extra. Subsequent
// releases close their conn inline. Primary is not closed here — that's
// the backend's job.
func (p *connPool[T]) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, c := range idle {
		p.closeFn(c)
	}
}
