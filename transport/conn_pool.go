package transport

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// connPool is a lazy connection pool for backends whose transport handles one
// transfer at a time (ssh sessions, sftp clients, ftp control connections).
// The backend's primary connection stays separate and handles metadata ops;
// the pool supplies extras for heavy ops (CopyFrom, AppendFrom, Open, OpenAt)
// so each parallel copy worker gets its own connection.
//
// Extras are dialed on demand up to maxExtras. acquire returns a pooled
// extra if one is idle, dials a new one if capacity remains, or else falls
// back to the primary: at once, or, in a pool that waits, when there is no
// extra lent out to wait for. With maxExtras=0 every acquire returns the
// primary. resize moves the limit while copies run.
//
// A dial that failed is not tried again for a while, a wait that doubles
// while dials fail (L16): a server that takes fewer connections than the
// limit turned away one login per file, and ssh servers count those. An
// extra whose connection has ended is dropped, not lent out (alive, S23).
type connPool[T any] struct {
	name      string // the backend, for the log
	primary   T
	idle      []T // released extras, oldest first
	dial      func(context.Context) (T, error)
	closeFn   func(T)
	mu        sync.Mutex
	size      int // extras alive: idle, lent out or being dialed
	maxExtras int
	closed    bool
	// waits makes an acquire that finds no extra to have wait for one that
	// is lent out, while there is one, in place of taking the primary. An
	// FTP control connection runs one command at a time: a transfer on it
	// held up every listing, Mkdir and SetTimes behind it (L16). freed,
	// made by a waiter, is closed when an extra comes back or goes, a dial
	// fails, the limit moves or the pool closes: each waiter looks again.
	waits    bool
	freed    chan struct{}
	dialWait time.Duration // the wait after the last failed dial; 0 once one works
	next     time.Time     // no dial before it
	// alive, when set, tells an extra whose connection works from one
	// whose connection has ended. Such an extra went back to the pool and
	// was lent out again, and the call on it failed with a lost
	// connection, on which the lazy wrapper drops the whole backend, the
	// primary's running call with it (S23). Now it is dropped at its
	// release, or when it is next taken, and the pool dials another.
	alive func(T) bool
}

// poolRedial is the wait after a failed dial of an extra connection; it
// doubles while dials fail, up to poolRedialMax.
var poolRedial, poolRedialMax = 5 * time.Second, 2 * time.Minute

func newConnPool[T any](name string, primary T, maxExtras int, dial func(context.Context) (T, error), closeFn func(T)) *connPool[T] {
	return &connPool[T]{name: name, primary: primary, dial: dial, closeFn: closeFn, maxExtras: max(maxExtras, 0)}
}

// acquire returns a connection and a release function. The release MUST be
// called exactly once. It returns the primary, with a no-op release, when
// the pool is closed, or no extra is idle and none is to be had: ctx has
// ended, none may be dialed (the pool is full, or a dial failed a short
// while ago) or the dial fails, and the pool does not wait or has none lent
// out to wait for. ctx bounds a dial of a new extra and a wait.
func (p *connPool[T]) acquire(ctx context.Context) (T, func()) {
	return p.acquireWait(ctx, p.waits, DirErr)
}

// tryAcquire is acquire that never waits for an extra lent out: an idle
// one, a new one while one may be dialed, or else the primary at once. A
// listing takes it, never to wait behind a transfer (L15). A dial that
// fails is a warning: the listing runs on the primary, and an error would
// open the log on each scan of a server that takes few connections.
func (p *connPool[T]) tryAcquire(ctx context.Context) (T, func()) {
	return p.acquireWait(ctx, false, DirWarn)
}

// acquireWait is acquire, waiting for an extra lent out only with waits. A
// dial that fails is logged as failed.
func (p *connPool[T]) acquireWait(ctx context.Context, waits bool, failed Dir) (T, func()) {
	if p == nil {
		var zero T
		return zero, func() {}
	}
	p.mu.Lock()
	for !p.closed {
		if c, ok := p.popIdle(); ok {
			if p.alive != nil && !p.alive(c) {
				// It ended while idle: the server dropped it, or the
				// keepalive found it dead.
				p.size--
				p.wakeLocked()
				p.mu.Unlock()
				p.closeFn(c)
				Log.Add(p.name, DirIn, "an extra connection is gone: dropped")
				p.mu.Lock()
				continue
			}
			p.mu.Unlock()
			return c, func() { p.release(c) }
		}
		if ctx.Err() != nil {
			break
		}
		if p.size < p.maxExtras && !time.Now().Before(p.next) {
			if c, ok := p.dialLocked(ctx, failed); ok {
				p.mu.Unlock()
				return c, func() { p.release(c) }
			}
			continue
		}
		if !waits || p.size == 0 {
			break
		}
		if p.freed == nil {
			p.freed = make(chan struct{})
		}
		freed := p.freed
		p.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
		}
		p.mu.Lock()
	}
	p.mu.Unlock()
	return p.primary, func() {}
}

// dialLocked dials a new extra, mu let go meanwhile. A dial that fails
// wakes the waiters, which may have waited for this one, and, unless ctx
// ended it, starts the wait before the next dial, or doubles it, and is
// logged as failed.
func (p *connPool[T]) dialLocked(ctx context.Context, failed Dir) (T, bool) {
	p.size++
	p.mu.Unlock()
	c, err := p.dial(ctx)
	p.mu.Lock()
	if err == nil {
		p.dialWait = 0
		return c, true
	}
	p.size--
	p.wakeLocked()
	// A dial alongside this one that failed first has set the wait.
	if ctx.Err() == nil && !time.Now().Before(p.next) {
		p.dialWait = min(max(2*p.dialWait, poolRedial), poolRedialMax)
		p.next = time.Now().Add(p.dialWait)
		Log.Add(p.name, failed, fmt.Sprintf("an extra connection: %v; no new one for %v", err, p.dialWait))
	}
	var zero T
	return zero, false
}

// wakeLocked wakes the acquires that wait, to look again; mu held.
func (p *connPool[T]) wakeLocked() {
	if p.freed != nil {
		close(p.freed)
		p.freed = nil
	}
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

// release returns conn c to the pool. If close has already run, the pool
// was shrunk below the extras it holds, or c's connection has ended,
// closes c inline instead: that is how a lowered limit takes effect as
// transfers finish, and it keeps conns acquired before close out of the
// abandoned idle list.
func (p *connPool[T]) release(c T) {
	p.mu.Lock()
	if p.closed || p.size > p.maxExtras || p.alive != nil && !p.alive(c) {
		p.size--
		p.wakeLocked()
		p.mu.Unlock()
		p.closeFn(c)
		return
	}
	p.idle = append(p.idle, c)
	p.wakeLocked()
	p.mu.Unlock()
}

// discard closes c, an extra the caller holds, for good, in place of its
// release: a connection closed under a call is of no use to the next one.
func (p *connPool[T]) discard(c T) {
	p.mu.Lock()
	p.size--
	p.wakeLocked()
	p.mu.Unlock()
	p.closeFn(c)
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
	p.wakeLocked()
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
	p.wakeLocked()
	p.mu.Unlock()
	for _, c := range idle {
		p.closeFn(c)
	}
}
