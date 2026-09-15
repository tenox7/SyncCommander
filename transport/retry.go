package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync/atomic"
	"time"
)

// CancelCloser starts a watcher that closes c when ctx is canceled. The
// returned stop func cancels the watcher; call it on normal completion
// (e.g. via defer) so the goroutine doesn't leak after the close. Closing
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
	return func() { close(done) }
}

// WithStallGuard runs op against a child of parent. If counter does not
// advance for idle, the child ctx is canceled, unblocking whatever op is
// waiting on. Parent cancellation propagates normally. If the stall fires
// while parent is still healthy, op's err is normalized to ErrStall so
// Retry treats it as retryable (rather than as a parent-ctx cancel which
// would abort retries). idle <= 0 disables the guard.
func WithStallGuard(parent context.Context, counter *atomic.Int64, idle time.Duration, op func(context.Context) error) error {
	if counter == nil || idle <= 0 {
		return op(parent)
	}
	attemptCtx, cancel := context.WithCancel(parent)
	defer cancel()

	var stalled atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		last := counter.Load()
		lastChange := time.Now()
		tickInterval := max(idle/4, time.Second)
		t := time.NewTicker(tickInterval)
		defer t.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case now := <-t.C:
				cur := counter.Load()
				if cur != last {
					last = cur
					lastChange = now
					continue
				}
				if now.Sub(lastChange) >= idle {
					stalled.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	err := op(attemptCtx)
	cancel()
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

func isPermanentError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrPermission) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, ErrUnsupported) {
		return true
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

// Retry runs op up to MaxRetries+1 times with exponential backoff. Returns
// immediately if ctx is canceled. Each retry is logged to the global Log;
// a missing capability is reported to the caller without a FAIL line.
func Retry(ctx context.Context, proto, what string, op func() error) error {
	max := MaxRetries()
	var err error
	for attempt := 0; attempt <= max; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = op()
		if err == nil {
			if attempt > 0 {
				Log.Add(proto, "REC", fmt.Sprintf("%s: recovered after %d %s", what, attempt, pluralRetries(attempt)))
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var consumed *consumedError
		if errors.Is(err, ErrUnsupported) || errors.As(err, &consumed) {
			return err
		}
		if isPermanentError(err) {
			Log.Add(proto, "FAIL", fmt.Sprintf("%s: %v", what, err))
			return err
		}
		if attempt == max {
			break
		}
		d := backoffDelay(attempt)
		Log.Add(proto, "RETRY", fmt.Sprintf("%s: %v (attempt %d/%d in %v)", what, err, attempt+2, max+1, d))
		if serr := sleepCtx(ctx, d); serr != nil {
			return serr
		}
	}
	Log.Add(proto, "FAIL", fmt.Sprintf("%s: gave up after %d attempts: %v", what, max+1, err))
	return err
}

func RetryVal[T any](ctx context.Context, proto, what string, op func() (T, error)) (T, error) {
	var v T
	err := Retry(ctx, proto, what, func() error {
		var e error
		v, e = op()
		return e
	})
	return v, err
}
