package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sync/atomic"

	"sc/model"
	"sc/transport"
)

// tryDirectTransfer attempts a path-to-path transfer (e.g. rsync directly
// between a local filesystem and a remote rsync daemon) when one side exposes
// a LocalFS path and the other supports a direct send or receive. This avoids
// any intermediate tmp file and lets rsync do its own delta-sync resume
// against whatever already exists at the destination. Backends implementing
// LocalSender or LocalReceiver credit progress themselves, so nothing is
// counted here.
func tryDirectTransfer(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry) bool {
	if lp, ok := src.(model.LocalFS); ok {
		if r, ok := dst.(model.LocalSender); ok {
			return r.SendLocalFile(ctx, lp.LocalPath(relPath), relPath, srcEntry.Mode) == nil
		}
	}
	if lp, ok := dst.(model.LocalFS); ok {
		if r, ok := src.(model.LocalReceiver); ok {
			return r.RecvToLocalFile(ctx, relPath, lp.LocalPath(relPath)) == nil
		}
	}
	return false
}

// tryResumeCopy appends only the missing tail of srcEntry onto an existing
// partial dst file. Returns false when resume is not applicable, not
// supported by either backend, or any step fails, so the caller falls back to
// a full copy.
//
// On success the Bytes counter ends up offset+(src.Size-offset) higher; the
// offset portion is also tracked in BaseBytes so it does not inflate the
// transfer-rate calculation. On failure the progress credited along the way
// is rolled back.
func tryResumeCopy(ctx context.Context, src, dst model.Backend, relPath string, srcEntry, dstEntry *model.FileEntry, bytes, baseBytes *atomic.Int64, verify func(context.Context) error) bool {
	if dstEntry == nil || dstEntry.IsDir || dstEntry.Size <= 0 || dstEntry.Size >= srcEntry.Size {
		return false
	}
	if _, ok := dst.(model.Resumer); !ok {
		return false
	}
	// The scan-time size goes stale as soon as anything else writes to dst,
	// and appending at the wrong offset corrupts the file. Re-read it live; a
	// dst that has since grown past srcEntry.Size (or vanished) falls back to
	// a full copy.
	offset := peekDstSize(ctx, dst, relPath)
	if offset <= 0 || offset >= srcEntry.Size {
		return false
	}
	return resumeAttempt(ctx, src, dst, relPath, srcEntry, offset, bytes, baseBytes, verify) == nil
}

// errResumeMismatch reports that a resumed file did not match the source after
// the append. Callers treat it like ErrUnsupported: fall back to a full
// overwrite copy rather than retrying the append.
var errResumeMismatch = errors.New("resumed file does not match source")

// resumeVerifier returns the post-append check for a resumed copy, or nil when
// verification is off. Resume trusts whatever prefix already sits at the
// destination (same size, different bytes produces a wrong file that otherwise
// reports success), so compare both sides once the append lands.
func resumeVerifier(enabled bool, scanner *model.Scanner, src, dst model.Backend, relPath string, size int64) func(context.Context) error {
	if !enabled {
		return nil
	}
	return func(ctx context.Context) error {
		if got := peekDstSize(ctx, dst, relPath); got != size {
			return fmt.Errorf("%w: %s (%d bytes at destination, source has %d)", errResumeMismatch, relPath, got, size)
		}
		// Without a shared algorithm the two sides would hash differently and
		// every comparison would read as a mismatch; the size check above is
		// all the verification available.
		if !scanner.NegotiateChecksum() {
			transport.Log.Add("copy", "ERR", "verify "+relPath+": no checksum algorithm shared by both sides, resumed content unverified")
			return nil
		}
		srcSum, serr := src.Checksum(ctx, relPath)
		dstSum, derr := dst.Checksum(ctx, relPath)
		if serr != nil || derr != nil || srcSum == "" || dstSum == "" {
			transport.Log.Add("copy", "ERR", "verify "+relPath+": checksum unavailable, resumed content unverified")
			return nil
		}
		if srcSum != dstSum {
			return fmt.Errorf("%w: %s (src %s, dst %s)", errResumeMismatch, relPath, srcSum, dstSum)
		}
		return nil
	}
}

// peekDstSize returns the current size of relPath on dst, or 0 if it cannot
// be determined. Used between retry attempts to find how many bytes of a
// partial upload survived so the next attempt can resume rather than restart.
func peekDstSize(ctx context.Context, dst model.Backend, relPath string) int64 {
	entries, err := dst.List(ctx, model.DirOf(relPath))
	if err != nil {
		return 0
	}
	name := path.Base(relPath)
	for i := range entries {
		if entries[i].Name == name && !entries[i].IsDir {
			return entries[i].Size
		}
	}
	return 0
}

// resumeAttempt appends bytes from offset onward, then runs verify (if any)
// against the finished file. Returns transport.ErrUnsupported if dst cannot
// append, errResumeMismatch if the result does not match the source; on any
// error the progress credited during the attempt is rolled back.
func resumeAttempt(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry, offset int64, bytes, baseBytes *atomic.Int64, verify func(context.Context) error) error {
	resumer, ok := dst.(model.Resumer)
	if !ok {
		return transport.ErrUnsupported
	}
	bytes.Add(offset)
	baseBytes.Add(offset)
	var added atomic.Int64
	added.Store(offset)
	opener := &trackedRangeOpener{Backend: src, RelPath: relPath, FileSize: srcEntry.Size, Ctx: ctx, Target: bytes, Added: &added}
	rollback := func() {
		bytes.Add(-added.Load())
		baseBytes.Add(-offset)
	}
	if err := resumer.AppendFrom(ctx, relPath, opener, srcEntry.Mode, offset); err != nil {
		rollback()
		return err
	}
	if verify == nil {
		return nil
	}
	if err := verify(ctx); err != nil {
		transport.Log.Add("copy", "ERR", err.Error()+", recopying in full")
		rollback()
		return err
	}
	return nil
}

// fullCopyAttempt opens src from byte 0 and writes the whole file via
// CopyFrom. On failure any progress credited during the attempt is rolled back.
func fullCopyAttempt(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry, counter *atomic.Int64) error {
	reader, err := src.Open(ctx, relPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	defer transport.CancelCloser(ctx, reader)()
	dstOwnsProgress := false
	if owner, ok := dst.(transport.ProgressOwner); ok && owner.OwnsCopyProgress() {
		dstOwnsProgress = true
	}
	var added atomic.Int64
	var srcReader io.Reader = reader
	if !transport.IsPreCounted(reader) && !dstOwnsProgress {
		srcReader = &trackedReader{r: srcReader, target: counter, added: &added}
	}
	srcReader = &cancelReader{r: srcReader, ctx: ctx}
	if err := dst.CopyFrom(ctx, relPath, srcReader, srcEntry.Mode); err != nil {
		counter.Add(-added.Load())
		return err
	}
	return nil
}

// trackedRangeOpener implements model.RangeOpener for resume flows. OpenAt
// wraps the returned reader so each tail byte read increments Target/Added.
// Open (used by rsync-style backends that own their own progress accounting)
// is left un-instrumented; those backends drive progress via the context
// counter during the rsync push.
type trackedRangeOpener struct {
	Backend  model.Backend
	RelPath  string
	FileSize int64
	Ctx      context.Context
	Target   *atomic.Int64
	Added    *atomic.Int64
}

func (o *trackedRangeOpener) Size() int64 { return o.FileSize }

func (o *trackedRangeOpener) LocalPath() string {
	if lp, ok := o.Backend.(model.LocalFS); ok {
		return lp.LocalPath(o.RelPath)
	}
	return ""
}

func (o *trackedRangeOpener) Open(ctx context.Context) (io.ReadCloser, error) {
	rd, err := o.Backend.Open(ctx, o.RelPath)
	if err != nil {
		return nil, err
	}
	return &cancelReadCloser{rc: rd, ctx: o.Ctx}, nil
}

func (o *trackedRangeOpener) OpenAt(ctx context.Context, offset int64) (io.ReadCloser, error) {
	var rd io.ReadCloser
	if seeker, ok := o.Backend.(model.SeekableOpener); ok {
		r, err := seeker.OpenAt(ctx, o.RelPath, offset)
		if err != nil && !errors.Is(err, transport.ErrUnsupported) {
			return nil, err
		}
		rd = r
	}
	if rd == nil {
		r, err := o.Backend.Open(ctx, o.RelPath)
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			if _, err := io.CopyN(io.Discard, r, offset); err != nil {
				r.Close()
				return nil, err
			}
		}
		rd = r
	}
	if transport.IsPreCounted(rd) {
		return &cancelReadCloser{rc: rd, ctx: o.Ctx}, nil
	}
	return &cancelReadCloser{rc: &trackedReadCloser{rc: rd, target: o.Target, added: o.Added}, ctx: o.Ctx}, nil
}

// The wrappers below expose Unwrap so transport.IsPreCounted can see through
// them to a pre-counted source.

type trackedReadCloser struct {
	rc     io.ReadCloser
	target *atomic.Int64
	added  *atomic.Int64
}

func (t *trackedReadCloser) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		t.target.Add(int64(n))
		t.added.Add(int64(n))
	}
	return n, err
}

func (t *trackedReadCloser) Close() error      { return t.rc.Close() }
func (t *trackedReadCloser) Unwrap() io.Reader { return t.rc }

type cancelReadCloser struct {
	rc  io.ReadCloser
	ctx context.Context
}

func (c *cancelReadCloser) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.rc.Read(p)
}

func (c *cancelReadCloser) Close() error      { return c.rc.Close() }
func (c *cancelReadCloser) Unwrap() io.Reader { return c.rc }

type trackedReader struct {
	r      io.Reader
	target *atomic.Int64
	added  *atomic.Int64
}

func (t *trackedReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.target.Add(int64(n))
		t.added.Add(int64(n))
	}
	return n, err
}

func (t *trackedReader) Unwrap() io.Reader { return t.r }

type cancelReader struct {
	r   io.Reader
	ctx context.Context
}

func (c *cancelReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func (c *cancelReader) Unwrap() io.Reader { return c.r }
