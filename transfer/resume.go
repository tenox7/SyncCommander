package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
//
// The local side must then hold as many bytes as the listing gave: the file
// sent, or the file received (F14). The transfer itself is checked by rsync.
// When it does not, the plain copy that follows fails on it too.
func tryDirectTransfer(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry) bool {
	if lp, ok := src.(model.LocalFS); ok {
		if r, ok := dst.(model.LocalSender); ok {
			local := lp.LocalPath(relPath)
			return r.SendLocalFile(ctx, local, relPath, srcEntry.Mode) == nil && localSizeIs(relPath, local, srcEntry.Size)
		}
	}
	if lp, ok := dst.(model.LocalFS); ok {
		if r, ok := src.(model.LocalReceiver); ok {
			local := lp.LocalPath(relPath)
			return r.RecvToLocalFile(ctx, relPath, local) == nil && localSizeIs(relPath, local, srcEntry.Size)
		}
	}
	return false
}

// localSizeIs reports whether the local file holds want bytes, logging why
// not. A copy of another size is not of the file the user saw, so it must
// not get the source's times and count as done (F14).
func localSizeIs(relPath, local string, want int64) bool {
	if err := sizeOfLocal(local, want); err != nil {
		transport.Log.Add("copy", transport.DirWarn, "COPY "+relPath+": "+err.Error()+", recopying in full")
		return false
	}
	return true
}

func sizeOfLocal(local string, want int64) error {
	fi, err := os.Stat(local)
	if err == nil && fi.Size() != want {
		err = transport.SizeMismatch(fi.Size(), want)
	}
	return err
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
func tryResumeCopy(ctx context.Context, src, dst model.Backend, relPath string, srcEntry, dstEntry *model.FileEntry, bytes, baseBytes *atomic.Int64, verify func(context.Context, int64) error) bool {
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
	return resumeAttempt(ctx, src, dst, relPath, relPath, srcEntry.Mode, srcEntry, offset, bytes, baseBytes, verify) == nil
}

// errResumeMismatch reports that a resumed file did not match the source after
// the append. Callers treat it like ErrUnsupported: fall back to a full
// overwrite copy rather than retrying the append.
var errResumeMismatch = errors.New("resumed file does not match source")

// resumeVerifier returns the post-append check for a resumed copy, or nil when
// verification is off. Resume trusts whatever prefix already sits at the
// destination (same size, different bytes produces a wrong file that otherwise
// reports success), so compare both sides once the append lands: by checksum
// when the sides share an algorithm, else by reading the prefix back from
// both. offset is where the append started.
func resumeVerifier(enabled bool, scanner *model.Scanner, src, dst model.Backend, relPath string, size int64) func(ctx context.Context, offset int64) error {
	if !enabled {
		return nil
	}
	return func(ctx context.Context, offset int64) error {
		if got := peekDstSize(ctx, dst, relPath); got != size {
			return fmt.Errorf("%w: %s (%d bytes at destination, source has %d)", errResumeMismatch, relPath, got, size)
		}
		why := "no checksum algorithm shared by both sides"
		if scanner.NegotiateChecksum() {
			srcSum, serr := src.Checksum(ctx, relPath)
			dstSum, derr := dst.Checksum(ctx, relPath)
			if serr == nil && derr == nil && srcSum != "" && dstSum != "" {
				if srcSum != dstSum {
					return fmt.Errorf("%w: %s (src %s, dst %s)", errResumeMismatch, relPath, srcSum, dstSum)
				}
				return nil
			}
			why = "checksum unavailable"
		}
		transport.Log.Add("copy", transport.DirOut, "verify "+relPath+": "+why+", comparing the resumed prefix byte for byte")
		return comparePrefix(ctx, src, dst, relPath, offset)
	}
}

// comparePrefix reads the first n bytes of relPath from both sides and reports
// errResumeMismatch when they differ. Only the prefix needs checking: the
// tail was just streamed from src. Fresh counters keep the read-back out of
// the copy's progress.
func comparePrefix(ctx context.Context, src, dst model.Backend, relPath string, n int64) error {
	ctx = transport.ContextWithProgress(ctx, new(atomic.Int64))
	ctx = transport.ContextWithBaseProgress(ctx, new(atomic.Int64))
	sr, err := src.Open(ctx, relPath)
	if err != nil {
		return err
	}
	defer sr.Close()
	dr, err := dst.Open(ctx, relPath)
	if err != nil {
		return err
	}
	defer dr.Close()
	same, err := sameBytes(sr, dr, n)
	if err != nil {
		return err
	}
	if !same {
		return fmt.Errorf("%w: %s (resumed prefix differs)", errResumeMismatch, relPath)
	}
	return nil
}

// sameBytes compares the next n bytes of a and b; a short read is an error.
func sameBytes(a, b io.Reader, n int64) (bool, error) {
	ba, bb := make([]byte, 256*1024), make([]byte, 256*1024)
	for n > 0 {
		want := min(n, int64(len(ba)))
		if _, err := io.ReadFull(a, ba[:want]); err != nil {
			return false, err
		}
		if _, err := io.ReadFull(b, bb[:want]); err != nil {
			return false, err
		}
		if !bytes.Equal(ba[:want], bb[:want]) {
			return false, nil
		}
		n -= want
	}
	return true, nil
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

// resumeAttempt appends relPath's bytes from offset onward to dstPath on dst
// (relPath itself, or its part), then runs verify (if any) against the
// finished file. Returns transport.ErrUnsupported if dst cannot append,
// errResumeMismatch if the result does not match the source; on any error
// the progress credited during the attempt is rolled back.
func resumeAttempt(ctx context.Context, src, dst model.Backend, relPath, dstPath string, mode os.FileMode, srcEntry *model.FileEntry, offset int64, bytes, baseBytes *atomic.Int64, verify func(context.Context, int64) error) error {
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
	if err := resumer.AppendFrom(ctx, dstPath, opener, mode, offset); err != nil {
		rollback()
		return err
	}
	if err := opener.checkSize(offset); err != nil {
		rollback()
		return err
	}
	if verify == nil {
		return nil
	}
	if err := verify(ctx, offset); err != nil {
		transport.Log.Add("copy", transport.DirWarn, err.Error()+", recopying in full")
		rollback()
		return err
	}
	return nil
}

// fullCopyAttempt opens relPath on src from byte 0 and writes the whole file
// to dstPath on dst (relPath itself, or its part) via CopyFrom. On failure
// any progress credited during the attempt is rolled back. A copy succeeds
// only when CopyFrom took as many bytes as the listing gave, no more and no
// fewer, and the source closed without an error (F14).
func fullCopyAttempt(ctx context.Context, src, dst model.Backend, relPath, dstPath string, mode os.FileMode, srcEntry *model.FileEntry, counter *atomic.Int64) error {
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
	counted := &countingReader{r: srcReader}
	srcReader = &cancelReader{r: counted, ctx: ctx}
	// This try opened the source itself, so a failure after the first byte
	// is the copy layer's to retry, with the source opened again (F29).
	err = transport.Unconsumed(dst.CopyFrom(ctx, dstPath, srcReader, mode))
	if err == nil {
		// A destination that takes the listed number of bytes and stops
		// (rclone's Put is told the size) leaves the rest of a source that
		// grew unread: one more read counts it.
		var probe [1]byte
		if _, perr := srcReader.Read(probe[:]); perr != nil && perr != io.EOF {
			err = fmt.Errorf("the source: %w", perr)
		}
	}
	if err == nil {
		if cerr := reader.Close(); cerr != nil {
			err = fmt.Errorf("the source: %w", cerr)
		}
	}
	if got := counted.n.Load(); err == nil && got != srcEntry.Size {
		err = transport.SizeMismatch(got, srcEntry.Size)
	}
	if err != nil {
		counter.Add(-added.Load())
		return err
	}
	return nil
}

// countingReader counts the bytes read through it. CopyFrom may read on
// another goroutine (an ssh session's stdin copier).
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func (c *countingReader) Unwrap() io.Reader { return c.r }

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

	// What the destination read, for checkSize.
	tailRead  atomic.Bool
	tail      atomic.Int64 // bytes read through OpenAt
	wholeRead atomic.Bool
	whole     atomic.Int64 // bytes read through Open
}

// checkSize compares what the destination read with the listed size: a tail
// through OpenAt, the whole file through Open (rsync spools it), or else the
// local file an rsync backend sent by its path (F14).
func (o *trackedRangeOpener) checkSize(offset int64) error {
	switch {
	case o.tailRead.Load():
		if got := offset + o.tail.Load(); got != o.FileSize {
			return transport.SizeMismatch(got, o.FileSize)
		}
	case o.wholeRead.Load():
		if got := o.whole.Load(); got != o.FileSize {
			return transport.SizeMismatch(got, o.FileSize)
		}
	case o.LocalPath() != "":
		return sizeOfLocal(o.LocalPath(), o.FileSize)
	default:
		return transport.SizeMismatch(offset, o.FileSize) // nothing was read
	}
	return nil
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
	o.wholeRead.Store(true)
	return &cancelReadCloser{rc: &countingReadCloser{rc: rd, n: &o.whole}, ctx: o.Ctx}, nil
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
	o.tailRead.Store(true)
	rd = &countingReadCloser{rc: rd, n: &o.tail}
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

type countingReadCloser struct {
	rc io.ReadCloser
	n  *atomic.Int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func (c *countingReadCloser) Close() error      { return c.rc.Close() }
func (c *countingReadCloser) Unwrap() io.Reader { return c.rc }

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
