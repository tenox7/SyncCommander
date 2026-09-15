package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sc/model"
)

// isConnLost reports whether err looks like the transport underneath the
// backend has died, so the next attempt must redial. Matching is
// substring-based because the SSH/SFTP/net stack wraps these in
// heterogeneous ways. An op-level EOF counts too: no backend method returns
// io.EOF from a healthy connection (readers are not wrapped here), so it can
// only mean the peer hung up.
func isConnLost(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := err.Error()
	for _, sub := range []string{
		"use of closed network connection",
		"connection lost",
		"broken pipe",
		"connection reset",
		"ssh: connection",
		"ssh: handshake",
		"ssh: disconnect",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

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

// lazyBackend connects on first use and redials after a lost connection.
// dialMu serializes dial attempts so parallel callers share one, while mu
// only guards the pointer: Close never waits behind a dial.
type lazyBackend struct {
	factory func() (model.Backend, error)
	display string
	proto   string
	mu      sync.Mutex
	inner   model.Backend
	closed  bool
	dialMu  sync.Mutex
}

func NewLazyBackend(display string, factory func() (model.Backend, error)) model.Backend {
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

func (b *lazyBackend) ensureConnected(ctx context.Context) (model.Backend, error) {
	if inner := b.current(); inner != nil {
		return inner, nil
	}
	b.dialMu.Lock()
	defer b.dialMu.Unlock()
	if inner := b.current(); inner != nil {
		return inner, nil
	}
	Log.Add(b.proto, ">>>", "connecting to "+b.display)
	inner, err := RetryVal(ctx, b.proto, "connect "+b.display, b.factory)
	if err != nil {
		Log.Add(b.proto, "ERR", b.display+": "+err.Error())
		return nil, err
	}
	b.mu.Lock()
	closed := b.closed
	if !closed {
		b.inner = inner
	}
	b.mu.Unlock()
	if closed {
		CloseBackend(inner)
		return nil, net.ErrClosed
	}
	Log.Add(b.proto, "<<<", "connected to "+b.display)
	return inner, nil
}

// markBrokenIf drops the cached backend when err says the connection died, so
// the next call redials. Returns err unchanged.
func (b *lazyBackend) markBrokenIf(err error) error {
	if !isConnLost(err) {
		return err
	}
	b.mu.Lock()
	inner := b.inner
	b.inner = nil
	b.mu.Unlock()
	if inner != nil {
		Log.Add(b.proto, "ERR", "connection lost, reconnecting on next op: "+err.Error())
		CloseBackend(inner)
	}
	return err
}

// do runs op against the connected backend under Retry.
func (b *lazyBackend) do(ctx context.Context, what string, op func(model.Backend) error) error {
	return Retry(ctx, b.proto, what, func() error {
		inner, err := b.ensureConnected(ctx)
		if err != nil {
			return err
		}
		return b.markBrokenIf(op(inner))
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

// capability returns the connected backend as T, or ErrUnsupported.
func capability[T any](b *lazyBackend, ctx context.Context) (T, error) {
	var zero T
	inner, err := b.ensureConnected(ctx)
	if err != nil {
		return zero, err
	}
	t, ok := inner.(T)
	if !ok {
		return zero, ErrUnsupported
	}
	return t, nil
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

func (b *lazyBackend) BasePath() string {
	if inner := b.current(); inner != nil {
		return inner.BasePath()
	}
	return b.display
}

func (b *lazyBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return doVal(b, ctx, "list "+relDir, func(in model.Backend) ([]model.FileEntry, error) { return in.List(ctx, relDir) })
}

func (b *lazyBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	return doVal(b, ctx, "checksum "+relPath, func(in model.Backend) (string, error) { return in.Checksum(ctx, relPath) })
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

func (b *lazyBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return doVal(b, ctx, "open "+relPath, func(in model.Backend) (io.ReadCloser, error) { return in.Open(ctx, relPath) })
}

// CopyFrom retries only while src is still untouched: typically a dead
// connection that markBrokenIf has just dropped, so the next attempt redials.
// Once bytes have been consumed the reader cannot be rewound and a second
// attempt would write a truncated file, so the failure is made permanent and
// the caller re-opens src and retries at a higher level instead.
func (b *lazyBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	counted := &countingReader{r: src}
	return b.do(ctx, "copy "+relPath, func(in model.Backend) error {
		err := in.CopyFrom(ctx, relPath, counted, mode)
		if err != nil && counted.n.Load() > 0 {
			return &consumedError{err}
		}
		return err
	})
}

// consumedError marks a copy that failed after reading from its source; Retry
// treats it as permanent without logging it, the copy layer reports it.
type consumedError struct{ err error }

func (e *consumedError) Error() string { return e.err.Error() }
func (e *consumedError) Unwrap() error { return e.err }

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

func (b *lazyBackend) Close() error {
	b.mu.Lock()
	inner := b.inner
	b.inner, b.closed = nil, true
	b.mu.Unlock()
	if c, ok := inner.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (b *lazyBackend) ProbeChecksums() []string {
	p, err := capability[model.ChecksumProber](b, context.Background())
	if err != nil {
		return nil
	}
	return p.ProbeChecksums()
}

func (b *lazyBackend) SetChecksumAlgo(algo string) {
	if p, ok := b.current().(model.ChecksumProber); ok {
		p.SetChecksumAlgo(algo)
	}
}

func (b *lazyBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	err := doAs(b, ctx, "prefetch "+scope, func(p model.ChecksumPrefetcher) error { return p.PrefetchChecksums(ctx, scope, recursive) })
	if errors.Is(err, ErrUnsupported) {
		return nil
	}
	return err
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
func (b *lazyBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	r, err := capability[model.Resumer](b, ctx)
	if err != nil {
		return err
	}
	return b.markBrokenIf(r.AppendFrom(ctx, relPath, src, mode, offset))
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

func (b *lazyBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, mode os.FileMode) error {
	return doAs(b, ctx, "send "+relPath, func(s model.LocalSender) error { return s.SendLocalFile(ctx, srcPath, relPath, mode) })
}

func (b *lazyBackend) SendLocalTree(ctx context.Context, srcRoot, relPath string, onFile func(name string)) error {
	return doAs(b, ctx, "batch send "+relPath, func(s model.BatchSender) error { return s.SendLocalTree(ctx, srcRoot, relPath, onFile) })
}

func (b *lazyBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	return doAs(b, ctx, "recv "+relPath, func(r model.LocalReceiver) error { return r.RecvToLocalFile(ctx, relPath, dstPath) })
}
