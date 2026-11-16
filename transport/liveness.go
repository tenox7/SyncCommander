package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// stallWatch is what a stall guard watches besides the progress counter
// (D5). A step that works without moving the bytes the user sees says so:
// it touches the watch while it works here (a block read to compare, a
// chunk spooled to disk), or says how long the server will be silent (a
// checksum of a large file, the prefix rsync hashes before an append). A
// guard inside another touches both.
type stallWatch struct {
	parent  *stallWatch
	touches atomic.Int64
	quiet   atomic.Int64 // unix nanos: silence is expected until then
}

type stallWatchKey struct{}

func watchFromContext(ctx context.Context) *stallWatch {
	w, _ := ctx.Value(stallWatchKey{}).(*stallWatch)
	return w
}

// Touch tells the stall guard over ctx that the step running under it is
// working. Without a guard it does nothing.
func Touch(ctx context.Context) {
	for w := watchFromContext(ctx); w != nil; w = w.parent {
		w.touches.Add(1)
	}
}

// ExpectSilence tells the stall guard over ctx that the next d may pass
// without progress: the server is busy with work it does not report. The
// guard then waits its idle time, as after any progress. The window is
// kept to its end, also once bytes move again: what the far side still
// hashes is not seen from here (a receiver hashing its prefix while the
// sender's first bytes wait in the socket).
func ExpectSilence(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	until := time.Now().Add(d).UnixNano()
	for w := watchFromContext(ctx); w != nil; w = w.parent {
		for {
			cur := w.quiet.Load()
			if cur >= until || w.quiet.CompareAndSwap(cur, until) {
				break
			}
		}
	}
}

// minHashRate is the slowest a side is taken to read and hash a file, in
// bytes per second: a slow disk, a small NAS, a network file system.
const minHashRate = 10 << 20

// HashTime is how long reading and hashing n bytes may take on a slow side.
func HashTime(n int64) time.Duration {
	return time.Duration(max(n, 0)/minHashRate) * time.Second
}

// Stalled reports whether ctx was ended by its stall guard, not by the user
// or the caller: a step can then say what happened, which a plain
// "context canceled" does not (the log drops that).
func Stalled(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrStall)
}

// ActiveReader touches the stall guard over ctx on every read: for data a
// step reads to check or to stage, which no progress counter shows.
func ActiveReader(ctx context.Context, r io.Reader) io.Reader {
	if watchFromContext(ctx) == nil {
		return r
	}
	return &activeReader{r: r, ctx: ctx}
}

type activeReader struct {
	r   io.Reader
	ctx context.Context
}

func (a *activeReader) Read(p []byte) (int, error) {
	n, err := a.r.Read(p)
	if n > 0 {
		Touch(a.ctx)
	}
	return n, err
}

func (a *activeReader) Unwrap() io.Reader { return a.r }

// activeConn touches the stall guard over ctx whenever bytes cross c.
func activeConn(ctx context.Context, c net.Conn) net.Conn {
	if watchFromContext(ctx) == nil {
		return c
	}
	return &activeConnT{Conn: c, ctx: ctx}
}

type activeConnT struct {
	net.Conn
	ctx context.Context
}

func (c *activeConnT) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		Touch(c.ctx)
	}
	return n, err
}

func (c *activeConnT) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		Touch(c.ctx)
	}
	return n, err
}

// activeReadCloser is ActiveReader for a stream the caller closes.
func activeReadCloser(ctx context.Context, rc io.ReadCloser) io.ReadCloser {
	if watchFromContext(ctx) == nil {
		return rc
	}
	return &activeReadCloserT{activeReader{r: rc, ctx: ctx}, rc}
}

type activeReadCloserT struct {
	activeReader
	c io.Closer
}

func (a *activeReadCloserT) Close() error { return a.c.Close() }
