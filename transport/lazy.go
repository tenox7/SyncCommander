package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sc/model"
)

// ErrUnsupported is returned for an optional capability the connected backend
// lacks (resume, direct local-path transfer, batch send).
var ErrUnsupported = errors.New("operation not supported by this backend")

// protoManagesLiveness names the protocols whose transport enforces its own
// idle deadline, so the scanner must not add a per-call stall timeout.
func protoManagesLiveness(proto string) bool {
	switch proto {
	case "webdav", "webdavs", "restic", "restics", "rclone":
		return true
	}
	return false
}

// connectPause is how long a backend tries no connect once its connects
// failed in a row for a whole retry budget (F7). A var for tests.
var connectPause = 30 * time.Second

// lazyBackend connects on first use and redials after a lost connection.
// dialMu serializes dial attempts so parallel callers share one, while mu
// only guards the pointer: Close never waits behind a dial.
type lazyBackend struct {
	factory  func(context.Context) (model.Backend, error)
	display  string
	proto    string
	mu       sync.Mutex
	inner    model.Backend
	closed   bool
	parallel int // last SetParallel, reapplied to every redialed inner; 0 = never set
	// Connects that failed, shared by every call (F7). A refused login is
	// kept and no connect is tried again: each would be one more failed
	// login. Other failures in a row space the next connect out on the
	// backoff one call's Retry uses, whatever call tries it; when they
	// outlast that budget, none is tried before pausedUntil and every call
	// fails at once, so a copy stops instead of trying each of its files.
	authErr     error
	through     bool // a call got through: the login is right
	failed      int
	lastErr     error
	nextConnect time.Time
	pausedUntil time.Time
	dialMu      sync.Mutex
}

// NewLazyBackend connects through factory on first use. The factory gets
// the ctx of the call that dials, which bounds the connect: a cancel stops
// a dial that hangs, as on an ssh -L forward whose far end is dead.
func NewLazyBackend(display string, factory func(context.Context) (model.Backend, error)) model.Backend {
	proto := "remote"
	if idx := strings.Index(display, "://"); idx > 0 {
		proto = display[:idx]
	}
	return &lazyBackend{factory: factory, display: display, proto: proto}
}

func (b *lazyBackend) ManagesLiveness() bool { return protoManagesLiveness(b.proto) }

func (b *lazyBackend) current() model.Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inner
}

// connCutter is a backend that can close its own connection, as SFTP does
// to stop a call the server does not answer (S11). One that has is of no
// more use.
type connCutter interface{ connCut() bool }

// connected returns the connection, nil when there is none yet, or why no
// call may reach the server now (connectGateLocked). A connection its
// backend closed is dropped here, so the call dials a new one at once.
func (b *lazyBackend) connected() (model.Backend, error) {
	b.mu.Lock()
	if err := b.connectGateLocked(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	inner := b.inner
	c, ok := inner.(connCutter)
	cut := ok && c.connCut()
	if cut {
		b.inner = nil
	}
	b.mu.Unlock()
	if cut {
		Log.Add(b.proto, DirErr, "the connection was closed to stop a call the server did not answer; reconnecting")
		inner.Close()
		return nil, nil
	}
	return inner, nil
}

func (b *lazyBackend) ensureConnected(ctx context.Context) (model.Backend, error) {
	if inner, err := b.connected(); inner != nil || err != nil {
		return inner, err
	}
	b.dialMu.Lock()
	defer b.dialMu.Unlock()
	// A call that waited for another's connect sees how it went.
	if inner, err := b.connected(); inner != nil || err != nil {
		return inner, err
	}
	// One connect per try: the Retry this call runs under owns the tries
	// (D4). It used to retry here too, 6 connects inside each of 6 tries.
	Log.Add(b.proto, DirOut, "connecting to "+b.display)
	inner, err := b.factory(ctx)
	if err != nil {
		if ctx.Err() == nil {
			err = b.connectFailed(connectFailure(err))
		}
		Log.Add(b.proto, DirErr, b.display+": "+err.Error())
		return nil, err
	}
	b.mu.Lock()
	b.failed = 0
	closed := b.closed
	if !closed {
		b.inner = inner
		// The factory sized the pool for the start value; +/- may have
		// moved it since.
		if b.parallel > 0 {
			SetParallel(inner, b.parallel)
		}
	}
	b.mu.Unlock()
	if closed {
		inner.Close()
		return nil, errClosed
	}
	Log.Add(b.proto, DirIn, "connected to "+b.display)
	return inner, nil
}

// markBrokenIf drops inner, the connection a call failed on, when err says
// it died, so the next call redials. Only while inner is still the current
// one: a call that ran on a connection another call has already replaced
// fails late, and the new connection is healthy (F8). Never a backend that
// makes its own connections again (redialsItself). Returns err unchanged.
func (b *lazyBackend) markBrokenIf(inner model.Backend, err error) error {
	if !isConnLost(err) || redialsItself(inner) {
		return err
	}
	b.mu.Lock()
	current := inner != nil && b.inner == inner
	if current {
		b.inner = nil
	}
	b.mu.Unlock()
	if current {
		Log.Add(b.proto, DirErr, "connection lost, reconnecting on next op: "+err.Error())
		inner.Close()
	}
	return err
}

// redialsItself reports whether inner makes its own connections again
// (Caps.RedialsItself): a lost one fails the call it met, and the retry
// goes to inner. Dropped and made again, an rclone, WebDAV or restic
// backend lost its caches, and its running recursive listing went on
// filling a cache no call read any more: a scan of a large bucket went on
// folder by folder (L19, W-X3). Capabilities ask nothing of the server.
func redialsItself(inner model.Backend) bool {
	if inner == nil {
		return false
	}
	caps, err := inner.Capabilities(context.Background())
	return err == nil && caps.RedialsItself
}

// connectGateLocked returns why no call may reach the server now, or nil: a
// closed backend; a refused login; a pause after connects failed in a row;
// the backoff after the last failure, which returns that failure for the
// call's Retry to try again after its own wait. It holds for a connected
// HTTP backend too, whose every call opens a connection. mu held.
func (b *lazyBackend) connectGateLocked() error {
	now := time.Now()
	switch {
	case b.closed:
		// A call that comes after Close dialed, closed what it got, and
		// was tried again: six connects for nothing (F23).
		return errClosed
	case b.authErr != nil:
		return b.authErr
	case b.failed == 0:
		return nil
	case now.Before(b.pausedUntil):
		return b.pausedErr()
	case now.Before(b.nextConnect):
		return b.lastErr
	}
	return nil
}

// connectFailed records a connect that failed and returns err, or, once
// failures in a row outlast the retry budget, the error that stops
// connecting for connectPause (F7). A refused login is kept for good.
func (b *lazyBackend) connectFailed(err error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if errors.Is(err, model.ErrAuth) {
		b.authErr = err
		return err
	}
	b.failed++
	b.lastErr = err
	now := time.Now()
	if b.failed > MaxRetries() {
		b.pausedUntil = now.Add(connectPause)
		return b.pausedErr()
	}
	b.nextConnect = now.Add(backoffDelay(b.failed - 1))
	return err
}

// pausedErr says why no connect is tried; mu held.
func (b *lazyBackend) pausedErr() error {
	return fmt.Errorf("%w: %d connects in a row failed; the next try is at %s, or open the location again; the last: %w",
		errNotConnecting, b.failed, b.pausedUntil.Format("15:04:05"), b.lastErr)
}

// noteResult tells the connect state what a call gave: one that got through
// ends a run of failed connects; one that could not open its own connection
// (an HTTP request to a server that is gone) is a failed connect; a login
// refused before any call got through is kept. A call that was canceled
// says nothing.
func (b *lazyBackend) noteResult(ctx context.Context, err error) error {
	switch {
	case err == nil:
		b.mu.Lock()
		b.failed, b.through = 0, true
		b.mu.Unlock()
	case ctx.Err() != nil:
	case errors.Is(err, model.ErrAuth):
		// Once a call got through, a refusal is about its path alone, as
		// on a WebDAV share whose folder asks for other credentials. It
		// is not tried again either way.
		b.mu.Lock()
		through := b.through
		b.mu.Unlock()
		if !through {
			return b.connectFailed(err)
		}
	case dialFailure(err):
		return b.connectFailed(model.WithKind(model.ErrUnreachable, err))
	}
	return err
}

// NotConnecting reports a backend that tries no connect now: its login was
// refused, or its connects failed in a row for a whole retry budget (F7).
// Opening the location again makes a new one, which tries: after a fixed
// password, an unlocked ssh agent, a server that is back.
func NotConnecting(b model.Backend) bool {
	l, ok := b.(*lazyBackend)
	if !ok {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.authErr != nil || time.Now().Before(l.pausedUntil)
}

// do runs op against the connected backend under Retry.
func (b *lazyBackend) do(ctx context.Context, what string, op func(model.Backend) error) error {
	return Retry(ctx, b.proto, what, func(context.Context) error {
		inner, err := b.ensureConnected(ctx)
		if err != nil {
			return err
		}
		return b.markBrokenIf(inner, b.noteResult(ctx, op(inner)))
	})
}

func doVal[T any](b *lazyBackend, ctx context.Context, what string, op func(model.Backend) (T, error)) (T, error) {
	var v T
	err := b.do(ctx, what, func(inner model.Backend) error {
		var e error
		v, e = op(inner)
		return e
	})
	return v, err
}

// connect returns the connected backend, connecting under Retry like any
// call.
func (b *lazyBackend) connect(ctx context.Context) (model.Backend, error) {
	return RetryVal(ctx, b.proto, "connect "+b.display, func(context.Context) (model.Backend, error) {
		return b.ensureConnected(ctx)
	})
}

// capability returns the connected backend as T, or ErrUnsupported: a
// caller that asked Capabilities first never gets it.
func capability[T any](b *lazyBackend, ctx context.Context) (T, error) {
	var zero T
	inner, err := b.connect(ctx)
	if err != nil {
		return zero, err
	}
	t, ok := inner.(T)
	if !ok {
		return zero, ErrUnsupported
	}
	return t, nil
}

// Capabilities says what the connected backend can do, connecting first:
// the wrapper has the methods of every optional interface, so its method
// set says nothing (D2). The same server stays behind it from one
// connection to the next, and so do its capabilities.
func (b *lazyBackend) Capabilities(ctx context.Context) (model.Caps, error) {
	inner, err := b.connect(ctx)
	if err != nil {
		return model.Caps{}, err
	}
	return inner.Capabilities(ctx)
}

// doAs is do for an optional capability: a backend without it fails fast
// instead of burning retries.
func doAs[T any](b *lazyBackend, ctx context.Context, what string, op func(T) error) error {
	if _, err := capability[T](b, ctx); err != nil {
		return err
	}
	return b.do(ctx, what, func(inner model.Backend) error {
		t, ok := inner.(T)
		if !ok {
			return ErrUnsupported
		}
		return op(t)
	})
}

// Location names the remote this backend opens, password masked. Two backends
// at one location are one object store: a copy between them reads the file it
// truncates.
func (b *lazyBackend) Location() string { return strings.TrimRight(b.display, "/") }

func (b *lazyBackend) BasePath() string {
	if inner := b.current(); inner != nil {
		return inner.BasePath()
	}
	return b.display
}

func (b *lazyBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return doVal(b, ctx, "list "+relDir, func(in model.Backend) ([]model.FileEntry, error) { return in.List(ctx, relDir) })
}

func (b *lazyBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	return doVal(b, ctx, "stat "+relPath, func(in model.Backend) (model.FileEntry, error) { return in.Stat(ctx, relPath) })
}

// Checksum passes algo on: a new connection is a new backend, which knew
// neither the algorithm nor what its server's probe found until the
// wrapper replayed both (F6). Now each call says which, and the new
// connection asks its server when it first needs to.
func (b *lazyBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	return doVal(b, ctx, "checksum "+relPath, func(in model.Backend) (string, error) { return in.Checksum(ctx, relPath, algo) })
}

func (b *lazyBackend) ChecksumAlgos(ctx context.Context) ([]string, error) {
	return doVal(b, ctx, "checksum algorithms", func(in model.Backend) ([]string, error) { return in.ChecksumAlgos(ctx) })
}

func (b *lazyBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	return b.do(ctx, "settimes "+relPath, func(in model.Backend) error { return in.SetTimes(ctx, relPath, mtime, atime, btime) })
}

func (b *lazyBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	return b.do(ctx, "mkdir "+relPath, func(in model.Backend) error { return in.Mkdir(ctx, relPath, mode) })
}

func (b *lazyBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	return b.do(ctx, "rename "+oldRelPath, func(in model.Backend) error { return in.Rename(ctx, oldRelPath, newRelPath) })
}

func (b *lazyBackend) Remove(ctx context.Context, relPath string) error {
	return b.do(ctx, "remove "+relPath, func(in model.Backend) error { return in.Remove(ctx, relPath) })
}

func (b *lazyBackend) RemoveAll(ctx context.Context, relPath string) error {
	return b.do(ctx, "removeall "+relPath, func(in model.Backend) error { return in.RemoveAll(ctx, relPath) })
}

func (b *lazyBackend) RemoveIn(ctx context.Context, dir string, items []model.RemoveItem) error {
	return doAs(b, ctx, fmt.Sprintf("remove %d in %s", len(items), dir), func(r model.BatchRemover) error { return r.RemoveIn(ctx, dir, items) })
}

func (b *lazyBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return doVal(b, ctx, "open "+relPath, func(in model.Backend) (io.ReadCloser, error) { return in.Open(ctx, relPath) })
}

// CopyFrom retries only while src is still untouched: typically a dead
// connection that markBrokenIf has just dropped, so the next attempt redials.
// Once bytes have been consumed the reader cannot be rewound and a second
// attempt would write a truncated file, so the failure is made permanent and
// the caller re-opens src and retries at a higher level instead.
func (b *lazyBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	counted := &countingReader{r: src}
	return b.do(ctx, "copy "+relPath, func(in model.Backend) error {
		err := in.CopyFrom(ctx, relPath, counted, p)
		if err != nil && counted.n.Load() > 0 {
			return &consumedError{err}
		}
		return err
	})
}

// consumedError marks a copy that failed after reading from its source; Retry
// treats it as permanent without logging it. The copy layer, which opens the
// source again for every try, takes the mark off (Unconsumed) and retries.
type consumedError struct{ err error }

func (e *consumedError) Error() string { return e.err.Error() }
func (e *consumedError) Unwrap() error { return e.err }

// Unconsumed takes off the mark a reconnecting backend puts on a CopyFrom
// that failed after reading from its source: that backend cannot retry
// with a stream it cannot rewind, so its Retry stops on the mark. A caller
// that opens the source again for every try takes the mark off, so its own
// Retry decides on the cause: an upload cut off by a dropped connection
// is tried again (F29).
func Unconsumed(err error) error {
	var c *consumedError
	if errors.As(err, &c) {
		return c.err
	}
	return err
}

// countingReader records whether anything was read, so CopyFrom can tell an
// untouched source stream from a partially consumed one.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// Unwrap lets IsPreCounted see the source under it: without it an rsync
// destination counted a download that had counted itself (F21).
func (c *countingReader) Unwrap() io.Reader { return c.r }

func (b *lazyBackend) Close() error {
	b.mu.Lock()
	inner := b.inner
	b.inner, b.closed = nil, true
	b.mu.Unlock()
	if inner == nil {
		return nil
	}
	return inner.Close()
}

// SetParallel resizes the connected backend's pool and remembers n for the
// next redial. It holds mu so a redial can't apply an older value after it.
func (b *lazyBackend) SetParallel(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.parallel = n
	if b.inner != nil {
		SetParallel(b.inner, n)
	}
}

func (b *lazyBackend) PrefetchChecksums(ctx context.Context, scope, algo string, recursive bool) (map[string]string, error) {
	var sums map[string]string
	err := doAs(b, ctx, "prefetch "+scope, func(p model.ChecksumPrefetcher) error {
		var e error
		sums, e = p.PrefetchChecksums(ctx, scope, algo, recursive)
		return e
	})
	if errors.Is(err, ErrUnsupported) {
		return nil, nil
	}
	return sums, err
}

func (b *lazyBackend) ChecksumFiles(ctx context.Context, rels []string, algo string) (map[string]string, error) {
	var sums map[string]string
	err := doAs(b, ctx, fmt.Sprintf("checksum %d files", len(rels)), func(c model.ChecksumBatcher) error {
		var e error
		sums, e = c.ChecksumFiles(ctx, rels, algo)
		return e
	})
	return sums, err
}

func (b *lazyBackend) PreloadRecursive(ctx context.Context, scope string) error {
	p, err := capability[model.RecursivePreloader](b, ctx)
	if errors.Is(err, ErrUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.PreloadRecursive(ctx, scope)
}

// AppendFrom is not retried here: the source has been consumed from offset,
// so the copy layer re-probes the destination size and resumes again itself.
func (b *lazyBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	inner, err := b.ensureConnected(ctx)
	if err != nil {
		return err
	}
	r, ok := inner.(model.Resumer)
	if !ok {
		return ErrUnsupported
	}
	return b.markBrokenIf(inner, b.noteResult(ctx, r.AppendFrom(ctx, relPath, src, p, offset)))
}

func (b *lazyBackend) PartSize(ctx context.Context, relPath string) (int64, error) {
	if _, err := capability[model.PartWriter](b, ctx); err != nil {
		return 0, err
	}
	return doVal(b, ctx, "stat part of "+relPath, func(in model.Backend) (int64, error) {
		p, ok := in.(model.PartWriter)
		if !ok {
			return 0, ErrUnsupported
		}
		return p.PartSize(ctx, relPath)
	})
}

func (b *lazyBackend) CommitPart(ctx context.Context, relPath string, mode os.FileMode) error {
	return doAs(b, ctx, "commit "+relPath, func(p model.PartWriter) error { return p.CommitPart(ctx, relPath, mode) })
}

func (b *lazyBackend) ChecksumRange(ctx context.Context, relPath, algo string, off, n int64) (string, error) {
	if _, err := capability[model.RegionChecksummer](b, ctx); err != nil {
		return "", err
	}
	return doVal(b, ctx, "checksum part of "+relPath, func(in model.Backend) (string, error) {
		r, ok := in.(model.RegionChecksummer)
		if !ok {
			return "", ErrUnsupported
		}
		return r.ChecksumRange(ctx, relPath, algo, off, n)
	})
}

func (b *lazyBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	if _, err := capability[model.SeekableOpener](b, ctx); err != nil {
		return nil, err
	}
	return doVal(b, ctx, "openat "+relPath, func(in model.Backend) (io.ReadCloser, error) {
		o, ok := in.(model.SeekableOpener)
		if !ok {
			return nil, ErrUnsupported
		}
		return o.OpenAt(ctx, relPath, offset)
	})
}

func (b *lazyBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, p model.PutParams) error {
	return doAs(b, ctx, "send "+relPath, func(s model.LocalSender) error { return s.SendLocalFile(ctx, srcPath, relPath, p) })
}

func (b *lazyBackend) SendLocalTree(ctx context.Context, srcRoot, relPath string, files []string, onFile func(name string)) error {
	return doAs(b, ctx, "batch send "+relPath, func(s model.BatchSender) error { return s.SendLocalTree(ctx, srcRoot, relPath, files, onFile) })
}

func (b *lazyBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	return doAs(b, ctx, "recv "+relPath, func(r model.LocalReceiver) error { return r.RecvToLocalFile(ctx, relPath, dstPath) })
}
