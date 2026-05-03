package transport

import (
	"context"
	"fmt"
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
			return nil
		}
		if ctxDone(ctx) {
			return ctx.Err()
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
