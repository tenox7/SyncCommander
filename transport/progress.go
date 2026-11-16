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
type progressBudgetKey struct{}

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

// ContextWithProgressBudget attaches the most the progress counter may be
// credited for the file being read: the size the listing gave it. A source
// that downloads the file before it gives it (rsync) caps its count there
// and, once the download worked, credits the rest. It is progress only: a
// put is told the file's size and mtime in model.PutParams (D1).
func ContextWithProgressBudget(ctx context.Context, size int64) context.Context {
	return context.WithValue(ctx, progressBudgetKey{}, size)
}

func progressBudgetFromContext(ctx context.Context) (int64, bool) {
	s, ok := ctx.Value(progressBudgetKey{}).(int64)
	return s, ok
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
// under the name is the old version and counts for nothing, and the temp
// file is found by reading the folder (tempWatch). What the last look
// missed is the caller's to credit once the receive worked.
func tailDirSize(stop <-chan struct{}, dir, basename string, inplace bool, adder, base *CappedAdder, skip int64) {
	if adder == nil {
		return
	}
	finalPath := filepath.Join(dir, basename)
	last := skip
	if inplace {
		if n := fileSizeOf(finalPath); n > last {
			adder.Add(n - last)
			if base != nil {
				base.Add(n - last)
			}
			last = n
		}
	}
	temp := tempWatch{dir: dir, base: basename, start: time.Now()}
	update := func(now time.Time, search bool) {
		if !inplace {
			adder.Add(temp.grown(now, search))
			return
		}
		if delta := fileSizeOf(finalPath) - last; delta > 0 {
			adder.Add(delta)
			last += delta
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			// No folder read for a file that is done: a copy of many
			// small files into a large folder would read it once each.
			update(time.Now(), false)
			return
		case now := <-ticker.C:
			update(now, true)
		}
	}
}

func fileSizeOf(p string) int64 {
	if fi, err := os.Stat(p); err == nil {
		return fi.Size()
	}
	return 0
}

// tempWatch follows the temp file rsync receives base into, in dir:
// gorsync's ".<base>.<random>", or "temp-rsync-*" on Windows. The folder
// may hold a million names, so it is read at most once a second, and only
// while no temp file is followed or the one followed did not grow since
// the last read: a temp file left by a run that was killed never grows,
// the live one does. Before, every file read the whole folder ten times a
// second (F9).
type tempWatch struct {
	dir, base string
	start     time.Time // what was written before this is not counted
	path      string    // the temp file followed, "" while none
	size      int64     // its size at the last look
	searched  time.Time // the last folder read
	moved     bool      // it grew since then
}

// readDirNames reads a folder's names; a var so a test can count the reads.
var readDirNames = func(dir string) ([]string, error) {
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return d.Readdirnames(-1)
}

// grown returns how much the temp file grew since the last look; search
// lets it read the folder when it is time to.
func (w *tempWatch) grown(now time.Time, search bool) int64 {
	var delta int64
	if w.path != "" {
		if fi, err := os.Lstat(w.path); err == nil {
			delta = max(fi.Size()-w.size, 0)
			w.size = fi.Size()
			w.moved = w.moved || delta > 0
		} else {
			w.path = "" // renamed into place, or removed
		}
	}
	if search && (w.path == "" || !w.moved) && now.Sub(w.searched) >= time.Second {
		w.searched, w.moved = now, false
		if p, fi := w.newest(); p != "" && p != w.path {
			// Written since the receive began: all of it is this
			// receive's. Older, a leftover: only its growth counts.
			w.path, w.size = p, fi.Size()
			if fi.ModTime().After(w.start) {
				delta += fi.Size()
			}
		}
	}
	return delta
}

// newest returns the most recently written temp file rsync may be receiving
// base into. The names are compared, not globbed, so a name with * ? [ in
// it still matches. sc's own part file is not rsync's.
func (w *tempWatch) newest() (string, os.FileInfo) {
	names, _ := readDirNames(w.dir)
	var path string
	var newest os.FileInfo
	for _, name := range names {
		if !isRsyncTemp(name, w.base) || model.IsPartName(name) {
			continue
		}
		p := filepath.Join(w.dir, name)
		if fi, err := os.Lstat(p); err == nil && fi.Mode().IsRegular() && (newest == nil || fi.ModTime().After(newest.ModTime())) {
			path, newest = p, fi
		}
	}
	return path, newest
}

// isRsyncTemp reports whether name is a temp file rsync may receive base
// into: gorsync's ".<base>.<digits>", or "temp-rsync-*". Only digits may
// follow, so ".a.b.123", the temp file of "a.b", is not taken for one of "a".
func isRsyncTemp(name, base string) bool {
	rest, ok := strings.CutPrefix(name, "."+base+".")
	if !ok {
		return strings.HasPrefix(name, "temp-rsync-")
	}
	return rest != "" && strings.Trim(rest, "0123456789") == ""
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
// writer side via a CappedAdder, if any, and to the stall guard over ctx, if
// set. Reads and Close are passed through unchanged.
type CountingReadWriter struct {
	RW    io.ReadWriteCloser
	Adder *CappedAdder
	ctx   context.Context
}

func (c *CountingReadWriter) Read(p []byte) (int, error) { return c.RW.Read(p) }
func (c *CountingReadWriter) Close() error               { return c.RW.Close() }
func (c *CountingReadWriter) Write(p []byte) (int, error) {
	n, err := c.RW.Write(p)
	if n > 0 {
		c.Adder.Add(int64(n))
		if c.ctx != nil {
			Touch(c.ctx)
		}
	}
	return n, err
}
