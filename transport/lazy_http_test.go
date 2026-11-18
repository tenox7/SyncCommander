package transport

import (
	"context"
	"sync"
	"testing"

	"sc/model"
)

// redialingStub is a connStub whose backend makes its own connections
// again, as an HTTP client does (Caps.RedialsItself).
type redialingStub struct{ *connStub }

func (s redialingStub) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := s.connStub.Capabilities(ctx)
	caps.RedialsItself = true
	return caps, err
}

// L19, W-X3: an rclone, WebDAV or restic backend has no connection of its
// own to lose: each call is a request. One reset dropped the whole backend
// for a new one, which lost its caches, while its running recursive
// listing went on filling a cache no call read: a scan went on folder by
// folder. Now a backend that says it redials itself is kept: the call that
// met the reset fails, its retry goes to the same backend, and nothing is
// closed. One that does not say so is dialed again, as an SFTP backend
// whose connection is gone. The backend decides, not the name of its
// protocol.
func TestLostConnectionKeepsABackendThatRedialsItself(t *testing.T) {
	SetMaxRetries(1)
	defer SetMaxRetries(5)
	for _, c := range []struct {
		what    string
		url     string
		redials bool // the stub says RedialsItself
	}{
		{"says it redials itself", "sftp://host/x", true},
		{"does not say so", "rclone://remote:bucket", false},
	} {
		t.Run(c.what, func(t *testing.T) {
			var mu sync.Mutex
			var conns []*connStub
			b := NewLazyBackend(c.url, func(context.Context) (model.Backend, error) {
				mu.Lock()
				defer mu.Unlock()
				s := &connStub{}
				if len(conns) == 0 {
					reset := connReset("read")
					s.listErr.Store(&reset)
				}
				conns = append(conns, s)
				if c.redials {
					return redialingStub{s}, nil
				}
				return s, nil
			})
			if _, err := b.List(context.Background(), ""); err != nil {
				t.Fatalf("a listing whose first try met a reset: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			want := 2
			if c.redials {
				want = 1
			}
			if len(conns) != want {
				t.Errorf("%d backends made; want %d", len(conns), want)
			}
			if c.redials && conns[0].closed.Load() {
				t.Error("the backend was closed after one lost connection")
			}
		})
	}
}
