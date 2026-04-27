package transport

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type progressKey struct{}
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

// ContextWithFileSize attaches the size of the file currently being copied so
// backends can size their per-file CappedAdders correctly.
func ContextWithFileSize(ctx context.Context, size int64) context.Context {
	return context.WithValue(ctx, fileSizeKey{}, size)
}

func fileSizeFromContext(ctx context.Context) int64 {
	s, _ := ctx.Value(fileSizeKey{}).(int64)
	return s
}

type PreCounted interface {
	preCounted()
}

type preCountedReadCloser struct{ io.ReadCloser }

func (preCountedReadCloser) preCounted() {}

func WrapPreCounted(rc io.ReadCloser) io.ReadCloser {
	return preCountedReadCloser{ReadCloser: rc}
}

func IsPreCounted(r io.Reader) bool {
	_, ok := r.(PreCounted)
	return ok
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

// tailDirSize polls dir for the largest in-progress file size matching the
// final basename, gorsync's renameio temp prefix (".<basename><random>"), or
// gorsync's Windows pattern ("temp-rsync-*"), and pushes incremental size
// deltas to adder. Stop the goroutine by closing stop.
func tailDirSize(stop <-chan struct{}, dir, basename string, adder *CappedAdder) {
	if adder == nil {
		return
	}
	finalPath := filepath.Join(dir, basename)
	patterns := []string{
		filepath.Join(dir, "."+basename+"*"),
		filepath.Join(dir, "temp-rsync-*"),
	}
	var last int64
	update := func() {
		var size int64
		if fi, err := os.Stat(finalPath); err == nil {
			size = fi.Size()
		}
		for _, p := range patterns {
			matches, _ := filepath.Glob(p)
			for _, m := range matches {
				if fi, err := os.Stat(m); err == nil && fi.Size() > size {
					size = fi.Size()
				}
			}
		}
		if delta := size - last; delta > 0 {
			adder.Add(delta)
			last = size
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

// CountingReadWriter wraps an io.ReadWriter and reports bytes written through
// the writer side via a CappedAdder. Reads are passed through unchanged.
type CountingReadWriter struct {
	RW    io.ReadWriter
	Adder *CappedAdder
}

func (c *CountingReadWriter) Read(p []byte) (int, error)  { return c.RW.Read(p) }
func (c *CountingReadWriter) Write(p []byte) (int, error) {
	n, err := c.RW.Write(p)
	if n > 0 {
		c.Adder.Add(int64(n))
	}
	return n, err
}
