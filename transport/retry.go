package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sc/model"
)

// CancelCloser starts a watcher that closes c when ctx is canceled. The
// returned stop func cancels the watcher; call it on normal completion
// (e.g. via defer) so the goroutine doesn't leak after the close. It may
// run more than once: a reader's Close calls it, and a reader can be
// closed twice. Closing
// the destination handle is what actually unblocks an in-flight Write on
// SFTP/SSH backends; ctx cancellation alone does not. c may be nil.
func CancelCloser(ctx context.Context, c io.Closer) func() {
	if c == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-done:
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// WithStallGuard runs op against a child of parent. If neither counter nor
// the steps' own signs of work (Touch, ExpectSilence) advance for idle, the
// child ctx is canceled with ErrStall as its cause, unblocking whatever op
// is waiting on. Parent cancellation propagates normally. If the stall
// fires while parent is still healthy, op's err is normalized to ErrStall
// so Retry treats it as retryable (rather than as a parent-ctx cancel which
// would abort retries). idle <= 0 disables the guard.
func WithStallGuard(parent context.Context, counter *atomic.Int64, idle time.Duration, op func(context.Context) error) error {
	if counter == nil || idle <= 0 {
		return op(parent)
	}
	attemptCtx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	w := &stallWatch{parent: watchFromContext(parent)}
	attemptCtx = context.WithValue(attemptCtx, stallWatchKey{}, w)

	var stalled atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		last, lastTouch := counter.Load(), w.touches.Load()
		lastChange := time.Now()
		tickInterval := max(idle/4, time.Second)
		t := time.NewTicker(tickInterval)
		defer t.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case now := <-t.C:
				cur, touch := counter.Load(), w.touches.Load()
				if cur != last || touch != lastTouch {
					last, lastTouch, lastChange = cur, touch, now
					continue
				}
				from := lastChange
				if quiet := time.Unix(0, w.quiet.Load()); quiet.After(from) {
					from = quiet
				}
				if now.Sub(from) >= idle {
					stalled.Store(true)
					cancel(ErrStall)
					return
				}
			}
		}
	}()

	err := op(attemptCtx)
	cancel(nil)
	<-done

	if stalled.Load() && parent.Err() == nil {
		return ErrStall
	}
	return err
}

var maxRetries atomic.Int32

func init() { maxRetries.Store(5) }

func SetMaxRetries(n int) { maxRetries.Store(int32(max(n, 0))) }

func MaxRetries() int { return int(maxRetries.Load()) }

// stallTimeout is the per-file idle window. If progress bytes do not advance
// for this long, the current attempt is canceled and (if invoked under
// Retry) retried. 0 disables stall detection.
var stallTimeout atomic.Int64

func init() { stallTimeout.Store(int64(60 * time.Second)) }

func SetStallTimeout(d time.Duration) { stallTimeout.Store(int64(d)) }

func StallTimeout() time.Duration { return time.Duration(stallTimeout.Load()) }

// ErrStall is returned by WithStallGuard when no progress was observed for
// the idle window. Retry treats it as a retryable failure (not permanent,
// not a parent-ctx cancel).
var ErrStall = errors.New("transfer stalled")

type fatalCancelKey struct{}

// ContextWithFatalCancel attaches a cancel func that backends can invoke via
// TriggerFatalAbort to abort the entire enclosing operation (not just the
// current retryable attempt). Used for errors that are pointless to retry
// or continue past, e.g. an rsync daemon module marked read only.
func ContextWithFatalCancel(ctx context.Context, cancel context.CancelFunc) context.Context {
	if cancel == nil {
		return ctx
	}
	return context.WithValue(ctx, fatalCancelKey{}, cancel)
}

func TriggerFatalAbort(ctx context.Context) {
	if c, ok := ctx.Value(fatalCancelKey{}).(context.CancelFunc); ok && c != nil {
		c()
	}
}

func pluralRetries(n int) string {
	if n == 1 {
		return "retry"
	}
	return "retries"
}

// ErrSizeMismatch fails a copy that moved another number of bytes than the
// listing gave: a source that ended early without an error, or one that
// changed since the scan. Another try reads the same file again, so it is
// final.
var ErrSizeMismatch = errors.New("size does not match the listing")

// ErrSpecialFile refuses to read or write a symbolic link, a named pipe, a
// socket or a device as if it were a file: a link leads out of the tree, and
// opening a pipe waits for a writer that may never come (L2, L9).
var ErrSpecialFile = errors.New("sc does not read or write through links, pipes or devices")

// ErrBadName refuses a name the file system cannot hold as it is: on
// Windows one that lands on another file or on none (model.WindowsName,
// L25). Another try has the same name, so it is final.
var ErrBadName = errors.New("a name this file system cannot hold")

// ErrAppendOffset is an AppendFrom refusing a file that does not end where
// the append starts: missing or shorter (writing on would leave a gap of
// zeros), or longer (L14, S8). The caller looks at the file again.
var ErrAppendOffset = errors.New("the file does not end where the append starts")

// appendOffsetErr is ErrAppendOffset for relPath holding size bytes, or
// missing when size < 0.
func appendOffsetErr(proto, relPath string, size, offset int64) error {
	if size < 0 {
		return fmt.Errorf("%s: %s is not there to append to at %d: %w", proto, relPath, offset, ErrAppendOffset)
	}
	return fmt.Errorf("%s: %s holds %d bytes, the append starts at %d: %w", proto, relPath, size, offset, ErrAppendOffset)
}

// SizeMismatch is the error for a copy of got bytes when the listing gave want.
func SizeMismatch(got, want int64) error {
	return fmt.Errorf("%w: %d bytes, the source listed %d (it ended early or changed since the scan)", ErrSizeMismatch, got, want)
}

// isPermanentError reports an error another try cannot fix. The kinds are
// read by type (D3): a refused login, a full disk, a backend that stopped
// connecting (F7). The words below are what servers send as text, kept
// for the backends that do not mark their errors yet.
func isPermanentError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrPermission) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrExist) || errors.Is(err, ErrUnsupported) || errors.Is(err, ErrIncompleteListing) || errors.Is(err, ErrSizeMismatch) || errors.Is(err, ErrSpecialFile) || errors.Is(err, ErrBadName) {
		return true
	}
	if GivesUp(err) {
		return true
	}
	// An HTTP answer is read by its code. The words below are read in what
	// was said, never in the path put with it, as an fs.PathError puts it:
	// a file or folder named "no such file" made any error final (W-M1).
	var se *statusError
	if errors.As(err, &se) {
		return se.final()
	}
	var pe *fs.PathError
	if errors.As(err, &pe) && pe.Err != nil {
		return isPermanentError(pe.Err)
	}
	msg := strings.ToLower(err.Error())
	for _, sub := range []string{"permission denied", "operation not permitted", "does not exist", "no such file", "not supported", "unknown module"} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

func backoffDelay(attempt int) time.Duration {
	return min(time.Duration(500<<attempt)*time.Millisecond, 10*time.Second)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type retryOwnerKey struct{}

// Retry runs op up to MaxRetries+1 times with exponential backoff. Returns
// immediately if ctx is canceled. Each retry is logged to the global Log;
// a missing capability, and a mode the destination would not take after
// the work was done (model.ErrModeNotSet), are reported to the caller
// without a FAIL line and without another try.
//
// The first Retry on a call path owns the tries (D4): op gets a ctx that
// says so, and a Retry under it, as a backend call inside a copy that is
// tried as a whole, runs its op once and leaves the next try to the owner.
// Nested tries used to multiply: 6 copy tries of 6 calls of 6 connects
// each (F7).
func Retry(ctx context.Context, proto, what string, op func(ctx context.Context) error) error {
	if ctx.Value(retryOwnerKey{}) != nil {
		return op(ctx)
	}
	ctx = context.WithValue(ctx, retryOwnerKey{}, true)
	max := MaxRetries()
	var err error
	for attempt := 0; attempt <= max; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = op(ctx)
		if err == nil {
			if attempt > 0 {
				Log.Add(proto, DirRec, fmt.Sprintf("%s: recovered after %d %s", what, attempt, pluralRetries(attempt)))
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var consumed *consumedError
		if errors.Is(err, ErrUnsupported) || errors.Is(err, model.ErrModeNotSet) || errors.As(err, &consumed) {
			return err
		}
		if isPermanentError(err) {
			Log.Add(proto, DirFail, fmt.Sprintf("%s: %v", what, err))
			return err
		}
		if attempt == max {
			break
		}
		d := backoffDelay(attempt)
		Log.Add(proto, DirRetry, fmt.Sprintf("%s: %v (attempt %d/%d in %v)", what, err, attempt+2, max+1, d))
		if serr := sleepCtx(ctx, d); serr != nil {
			return serr
		}
	}
	Log.Add(proto, DirFail, fmt.Sprintf("%s: gave up after %d attempts: %v", what, max+1, err))
	return err
}

func RetryVal[T any](ctx context.Context, proto, what string, op func(ctx context.Context) (T, error)) (T, error) {
	var v T
	err := Retry(ctx, proto, what, func(ctx context.Context) error {
		var e error
		v, e = op(ctx)
		return e
	})
	return v, err
}
