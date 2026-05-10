package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync/atomic"
	"time"
)

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

func pluralRetries(n int) string {
	if n == 1 {
		return "retry"
	}
	return "retries"
}

func isPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrPermission) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "permission denied") || strings.Contains(msg, "operation not permitted")
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
		if isPermissionError(err) {
			Log.Add(proto, "FAIL", fmt.Sprintf("%s: %v (permission error, not retrying)", what, err))
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
