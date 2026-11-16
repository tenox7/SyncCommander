package transport

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"sc/model"
)

type progressKey struct{}
type baseProgressKey struct{}
type fileSizeKey struct{}

func ContextWithProgress(ctx context.Context, counter *atomic.Int64) context.Context {
	if counter == nil {
		return ctx
	}
	return context.WithValue(ctx, progressKey{}, counter)
}

func progressFromContext(ctx context.Context) *atomic.Int64 {
	c, _ := ctx.Value(progressKey{}).(*atomic.Int64)
	return c
}

// ContextWithBaseProgress attaches a counter that tracks bytes which were
// pre-credited to the regular progress counter as "already at the destination"
// (resume offsets). Subtracting BaseBytes from Bytes yields the bytes actually
// transferred during this session — used for honest rate/ETA calculations.
func ContextWithBaseProgress(ctx context.Context, base *atomic.Int64) context.Context {
	if base == nil {
		return ctx
	}
	return context.WithValue(ctx, baseProgressKey{}, base)
}

func baseProgressFromContext(ctx context.Context) *atomic.Int64 {
	b, _ := ctx.Value(baseProgressKey{}).(*atomic.Int64)
	return b
}

// ContextWithFileSize attaches the size of the file currently being copied so
// backends can size their per-file CappedAdders correctly.
func ContextWithFileSize(ctx context.Context, size int64) context.Context {
	return context.WithValue(ctx, fileSizeKey{}, size)
}

func fileSizeFromContext(ctx context.Context) (int64, bool) {
	s, ok := ctx.Value(fileSizeKey{}).(int64)
	return s, ok
}

type modTimeKey struct{}

func ContextWithModTime(ctx context.Context, mtime time.Time) context.Context {
	if mtime.IsZero() {
		return ctx
	}
	return context.WithValue(ctx, modTimeKey{}, mtime)
}

func modTimeFromContext(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(modTimeKey{}).(time.Time)
	return t, ok
}

type PreCounted interface {
	preCounted()
}

type preCountedReadCloser struct{ io.ReadCloser }

func (preCountedReadCloser) preCounted() {}

func WrapPreCounted(rc io.ReadCloser) io.ReadCloser {
	return preCountedReadCloser{ReadCloser: rc}
}

// IsPreCounted reports whether r, or a reader it wraps (via Unwrap), was
// marked pre-counted: the producer already credited its bytes to the progress
// counter, so the consumer must not count them again.
func IsPreCounted(r io.Reader) bool {
	for r != nil {
		if _, ok := r.(PreCounted); ok {
			return true
		}
		u, ok := r.(interface{ Unwrap() io.Reader })
		if !ok {
			return false
		}
		r = u.Unwrap()
	}
	return false
}

// ProgressOwner is implemented by backends whose CopyFrom counts its own bytes
// against the context counter. When dst implements this and OwnsCopyProgress
// returns true, callers MUST NOT wrap the source reader with their own counter.
type ProgressOwner interface {
	OwnsCopyProgress() bool
}

// CappedAdder pushes monotonically increasing deltas to target up to a total
// of cap bytes. Subsequent Add calls past the cap are silently ignored. Safe
// for concurrent use.
type CappedAdder struct {
	target *atomic.Int64
	cap    int64
	used   atomic.Int64
}

func NewCappedAdder(target *atomic.Int64, cap int64) *CappedAdder {
	return &CappedAdder{target: target, cap: cap}
}

func (c *CappedAdder) Add(delta int64) {
	if delta <= 0 || c == nil || c.target == nil {
		return
	}
	for {
		used := c.used.Load()
		rem := c.cap - used
		if rem <= 0 {
			return
		}
		add := delta
		if add > rem {
			add = rem
		}
		if c.used.CompareAndSwap(used, used+add) {
			c.target.Add(add)
			return
		}
	}
}

// Used returns the bytes already credited through this adder. Callers use this
// to roll back in-band progress on operation failure (counter.Add(-Used)) so
// that a retry path doesn't double-count.
func (c *CappedAdder) Used() int64 {
	if c == nil {
		return 0
	}
	return c.used.Load()
}

// tailDirSize polls dir for the size of the file rsync is receiving there
// and pushes its growth to adder until stop is closed. A receive in place
// (--append) grows the file under its own name from skip: the caller already
// credited the bytes before skip (a sparse placeholder), and anything there
// beyond skip at the start is credited to base too, so it shows in the
// totals without inflating the rate. Any other receive writes a temp file
// next to the name and renames it at the end (R12): until then the file
// under the name is the old version and counts for nothing.
func tailDirSize(stop <-chan struct{}, dir, basename string, inplace bool, adder, base *CappedAdder, skip int64) {
	if adder == nil {
		return
	}
	finalPath := filepath.Join(dir, basename)
	size := func() int64 {
		if !inplace {
			return rsyncTempSize(dir, basename)
		}
		if fi, err := os.Stat(finalPath); err == nil {
			return fi.Size()
		}
		return 0
	}
	last := skip
	if n := size(); inplace && n > last {
		adder.Add(n - last)
		if base != nil {
			base.Add(n - last)
		}
		last = n
	}
	update := func() {
		if delta := size() - last; delta > 0 {
			adder.Add(delta)
			last += delta
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			update()
			return
		case <-ticker.C:
			update()
		}
	}
}

// rsyncTempSize is the size of the largest temp file rsync may be receiving
// basename into in dir: gorsync's ".<basename>.<random>", or "temp-rsync-*"
// on Windows. The names are compared, not globbed, so a name with * ? [ in
// it still matches. sc's own part file is not rsync's.
func rsyncTempSize(dir, basename string) int64 {
	d, err := os.Open(dir)
	if err != nil {
		return 0
	}
	names, _ := d.Readdirnames(-1)
	d.Close()
	prefix := "." + basename + "."
	var n int64
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) && !strings.HasPrefix(name, "temp-rsync-") || model.IsPartName(name) {
			continue
		}
		if fi, err := os.Lstat(filepath.Join(dir, name)); err == nil && fi.Mode().IsRegular() && fi.Size() > n {
			n = fi.Size()
		}
	}
	return n
}

// CountingWriter wraps a writer and pushes byte deltas through a CappedAdder.
type CountingWriter struct {
	W     io.Writer
	Adder *CappedAdder
}

func (c *CountingWriter) Write(p []byte) (int, error) {
	n, err := c.W.Write(p)
	if n > 0 {
		c.Adder.Add(int64(n))
	}
	return n, err
}

// CountingReadWriter wraps a stream and reports bytes written through the
// writer side via a CappedAdder. Reads and Close are passed through unchanged.
type CountingReadWriter struct {
	RW    io.ReadWriteCloser
	Adder *CappedAdder
}

func (c *CountingReadWriter) Read(p []byte) (int, error) { return c.RW.Read(p) }
func (c *CountingReadWriter) Close() error               { return c.RW.Close() }
func (c *CountingReadWriter) Write(p []byte) (int, error) {
	n, err := c.RW.Write(p)
	if n > 0 {
		c.Adder.Add(int64(n))
	}
	return n, err
}
