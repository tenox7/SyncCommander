package transport

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// L15: a listing takes an extra too. A failed dial of one is an error for
// a transfer, which asked for that many connections: the server takes
// fewer. A listing runs on the primary without it, so for a listing it is
// a warning, which opens no log.
func TestConnPoolFailedDialLevel(t *testing.T) {
	for _, c := range []struct {
		name, level string
		acquire     func(*connPool[*int64], context.Context) (*int64, func())
	}{
		{"pool-transfer", "ERR", (*connPool[*int64]).acquire},
		{"pool-listing", "WARN", (*connPool[*int64]).tryAcquire},
	} {
		d := &testDialer{}
		d.refuse.Store(true)
		primary := new(int64)
		p := newConnPool(c.name, primary, 1, d.dial, func(*int64) {})
		mark := len(Log.Lines())
		got, release := c.acquire(p, context.Background())
		release()
		if got != primary {
			t.Errorf("%s: an acquire the server turned away got an extra", c.name)
		}
		want := " " + c.name + " " + c.level + " an extra connection: 421"
		if !slices.ContainsFunc(Log.Lines()[mark:], func(l string) bool { return strings.Contains(l, want) }) {
			t.Errorf("%s: no %q in the log: %q", c.name, want, Log.Lines()[mark:])
		}
	}
}
