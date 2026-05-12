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

// cancelCloser starts a watcher that closes c when ctx is canceled. The
// returned stop func cancels the watcher; call it on normal completion
// (e.g. via defer) so the goroutine doesn't leak after the close. Closing
// the destination handle is what actually unblocks an in-flight Write on
// SFTP/SSH backends — ctx cancellation alone does not.
func cancelCloser(ctx context.Context, c io.Closer) func() {
	if ctx == nil || c == nil {
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
// advance for idle, the child ctx is canceled — unblocking whatever op is
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
		tickInterval := idle / 4
		if tickInterval < time.Second {
			tickInterval = time.Second
		}
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

var maxRetries int32 = 5

func SetMaxRetries(n int) {
	if n < 0 {
		n = 0
	}
	atomic.StoreInt32(&maxRetries, int32(n))
}

func MaxRetries() int {
	return int(atomic.LoadInt32(&maxRetries))
}

// stallTimeoutSec is the per-file idle window in seconds. If progress
// bytes do not advance for this long, the current attempt is canceled
// and (if invoked under Retry) retried. 0 disables stall detection.
var stallTimeoutSec int64 = 60

func SetStallTimeout(d time.Duration) {
	atomic.StoreInt64(&stallTimeoutSec, int64(d/time.Second))
}

func StallTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&stallTimeoutSec)) * time.Second
}

// ErrStall is returned by WithStallGuard when no progress was observed for
// the idle window. Retry treats it as a retryable failure (not permanent,
// not a parent-ctx cancel).
var ErrStall = errors.New("transfer stalled")

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
	if errors.Is(err, fs.ErrPermission) || errors.Is(err, fs.ErrNotExist) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "permission denied") || strings.Contains(msg, "operation not permitted") {
		return true
	}
	if strings.Contains(msg, "does not exist") || strings.Contains(msg, "no such file") {
		return true
	}
	return strings.Contains(msg, "not supported")
}

func backoffDelay(attempt int) time.Duration {
	d := time.Duration(500<<attempt) * time.Millisecond
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	return d
}

func ctxDone(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Err() != nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if ctx == nil {
		time.Sleep(d)
		return nil
	}
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
// immediately if ctx is canceled. Each retry is logged to the global Log.
func Retry(ctx context.Context, proto, what string, op func() error) error {
	max := MaxRetries()
	var err error
	for attempt := 0; attempt <= max; attempt++ {
		if ctxDone(ctx) {
			return ctx.Err()
		}
		err = op()
		if err == nil {
			if attempt > 0 {
				Log.Add(proto, "REC", fmt.Sprintf("%s: recovered after %d %s", what, attempt, pluralRetries(attempt)))
			}
			return nil
		}
		if ctxDone(ctx) {
			return ctx.Err()
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
	if err != nil {
		Log.Add(proto, "FAIL", fmt.Sprintf("%s: gave up after %d attempts: %v", what, max+1, err))
	}
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
