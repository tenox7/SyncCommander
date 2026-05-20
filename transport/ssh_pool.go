package transport

import "sync"

// sshPool is a lazy connection pool for backends that need parallel SSH
// transports (sftp.Client or ssh.Client). The backend's primary connection
// stays separate and handles metadata ops; the pool supplies extras for
// heavy ops (CopyFrom, AppendFrom, Open, OpenAt) so each goroutine in the
// UI's parallel worker pool gets its own TCP/SSH connection.
//
// Extras are dialed on demand up to maxExtras. acquire returns a pooled
// extra if one is idle, dials a new one if capacity remains, or falls back
// to primary when at capacity. With maxExtras=0 the pool is a no-op and
// every acquire returns primary — matching the pre-pool single-conn
// behavior for --parallel=1.
type sshPool[T any] struct {
	primary   T
	avail     chan T
	dial      func() (T, error)
	closeFn   func(T)
	mu        sync.Mutex
	size      int
	maxExtras int
	closed    bool
}

func newSSHPool[T any](primary T, maxExtras int, dial func() (T, error), closeFn func(T)) *sshPool[T] {
	if maxExtras < 0 {
		maxExtras = 0
	}
	return &sshPool[T]{
		primary:   primary,
		avail:     make(chan T, maxExtras),
		dial:      dial,
		closeFn:   closeFn,
		maxExtras: maxExtras,
	}
}

// acquire returns a connection and a release function. The release MUST be
// called exactly once. Returns primary (with a no-op release) when the
// pool is full, dial fails, or the pool has been closed.
func (p *sshPool[T]) acquire() (T, func()) {
	if p == nil {
		var zero T
		return zero, func() {}
	}
	select {
	case c := <-p.avail:
		return c, func() { p.release(c) }
	default:
	}
	p.mu.Lock()
	if p.closed {
		primary := p.primary
		p.mu.Unlock()
		return primary, func() {}
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

// release returns conn c to the pool. If close has already run, closes c
// inline instead — this prevents conns acquired before close from leaking
// into the abandoned channel.
func (p *sshPool[T]) release(c T) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.closeFn(c)
		return
	}
	p.avail <- c
	p.mu.Unlock()
}

// close marks the pool closed and closes every idle extra. Subsequent
// releases close their conn inline. Primary is not closed here — that's
// the backend's job.
func (p *sshPool[T]) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	for {
		select {
		case c := <-p.avail:
			p.closeFn(c)
		default:
			return
		}
	}
}
