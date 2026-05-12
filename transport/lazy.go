package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"sc/model"
)

// isConnLost reports whether err looks like the underlying network/SSH
// connection has been torn down — the kind of error that survives across
// retry attempts unless we redial. Matching is substring-based because
// the SSH/SFTP/net stack wraps these in heterogeneous ways. Bare io.EOF
// is excluded since it's the normal end-of-stream signal from readers.
func isConnLost(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := err.Error()
	for _, sub := range []string{
		"use of closed network connection",
		"connection lost",
		"broken pipe",
		"connection reset",
		"ssh: connection",
		"ssh: handshake",
		"ssh: disconnect",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

// ErrResumeUnsupported is returned by lazyBackend.AppendFrom/OpenAt when the
// underlying backend does not implement the corresponding optional interface.
var ErrResumeUnsupported = errors.New("resume not supported by this backend")

type lazyBackend struct {
	factory func() (model.Backend, error)
	inner   model.Backend
	mu      sync.Mutex
	display string
	proto   string
}

func NewLazyBackend(display string, factory func() (model.Backend, error)) model.Backend {
	proto := "remote"
	if idx := strings.Index(display, "://"); idx > 0 {
		proto = display[:idx]
	}
	return &lazyBackend{factory: factory, display: display, proto: proto}
}

func (b *lazyBackend) ensureConnected() (model.Backend, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inner != nil {
		return b.inner, nil
	}
	Log.Add(b.proto, ">>>", "connecting to "+b.display)
	var inner model.Backend
	err := Retry(context.Background(), b.proto, "connect "+b.display, func() error {
		var ferr error
		inner, ferr = b.factory()
		return ferr
	})
	if err != nil {
		Log.Add(b.proto, "ERR", b.display+": "+err.Error())
		return nil, err
	}
	b.inner = inner
	Log.Add(b.proto, "<<<", "connected to "+b.display)
	return inner, nil
}

// markBrokenIf inspects err and, if it indicates the connection has died,
// drops the cached backend so the next ensureConnected() redials. Safe to
// call from any goroutine. Returns err unchanged.
func (b *lazyBackend) markBrokenIf(err error) error {
	if !isConnLost(err) {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inner != nil {
		Log.Add(b.proto, "ERR", "connection lost — will reconnect on next op: "+err.Error())
		if c, ok := b.inner.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		b.inner = nil
	}
	return err
}

func (b *lazyBackend) BasePath() string {
	b.mu.Lock()
	inner := b.inner
	b.mu.Unlock()
	if inner != nil {
		return inner.BasePath()
	}
	return b.display
}

func (b *lazyBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return RetryVal(ctx, b.proto, "list "+relDir, func() ([]model.FileEntry, error) {
		inner, err := b.ensureConnected()
		if err != nil {
			return nil, err
		}
		entries, err := inner.List(ctx, relDir)
		return entries, b.markBrokenIf(err)
	})
}

func (b *lazyBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	return RetryVal(ctx, b.proto, "checksum "+relPath, func() (string, error) {
		inner, err := b.ensureConnected()
		if err != nil {
			return "", err
		}
		s, err := inner.Checksum(ctx, relPath)
		return s, b.markBrokenIf(err)
	})
}

func (b *lazyBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	return Retry(ctx, b.proto, "settimes "+relPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		return b.markBrokenIf(inner.SetTimes(ctx, relPath, mtime, atime, btime))
	})
}

func (b *lazyBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	inner, err := b.ensureConnected()
	if err != nil {
		return err
	}
	return b.markBrokenIf(inner.CopyFrom(ctx, relPath, src, mode))
}

func (b *lazyBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	return Retry(ctx, b.proto, "mkdir "+relPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		return b.markBrokenIf(inner.Mkdir(ctx, relPath, mode))
	})
}

func (b *lazyBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	return Retry(ctx, b.proto, "rename "+oldRelPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		return b.markBrokenIf(inner.Rename(ctx, oldRelPath, newRelPath))
	})
}

func (b *lazyBackend) Remove(ctx context.Context, relPath string) error {
	return Retry(ctx, b.proto, "remove "+relPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		return b.markBrokenIf(inner.Remove(ctx, relPath))
	})
}

func (b *lazyBackend) RemoveAll(ctx context.Context, relPath string) error {
	return Retry(ctx, b.proto, "removeall "+relPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		return b.markBrokenIf(inner.RemoveAll(ctx, relPath))
	})
}

func (b *lazyBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return RetryVal(ctx, b.proto, "open "+relPath, func() (io.ReadCloser, error) {
		inner, err := b.ensureConnected()
		if err != nil {
			return nil, err
		}
		rc, err := inner.Open(ctx, relPath)
		return rc, b.markBrokenIf(err)
	})
}

func (b *lazyBackend) Close() error {
	b.mu.Lock()
	inner := b.inner
	b.mu.Unlock()
	if inner == nil {
		return nil
	}
	if c, ok := inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (b *lazyBackend) ProbeChecksums() []string {
	inner, err := b.ensureConnected()
	if err != nil {
		return nil
	}
	if p, ok := inner.(model.ChecksumProber); ok {
		return p.ProbeChecksums()
	}
	return nil
}

func (b *lazyBackend) SetChecksumAlgo(algo string) {
	b.mu.Lock()
	inner := b.inner
	b.mu.Unlock()
	if inner == nil {
		return
	}
	if p, ok := inner.(model.ChecksumProber); ok {
		p.SetChecksumAlgo(algo)
	}
}

func (b *lazyBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	inner, err := b.ensureConnected()
	if err != nil {
		return err
	}
	p, ok := inner.(model.ChecksumPrefetcher)
	if !ok {
		return nil
	}
	return Retry(ctx, b.proto, "prefetch "+scope, func() error {
		return p.PrefetchChecksums(ctx, scope, recursive)
	})
}

func (b *lazyBackend) AppendFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode, offset int64) error {
	inner, err := b.ensureConnected()
	if err != nil {
		return err
	}
	r, ok := inner.(model.Resumer)
	if !ok {
		return ErrResumeUnsupported
	}
	return b.markBrokenIf(r.AppendFrom(ctx, relPath, src, mode, offset))
}

func (b *lazyBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	inner, err := b.ensureConnected()
	if err != nil {
		return nil, err
	}
	if _, ok := inner.(model.SeekableOpener); !ok {
		return nil, ErrResumeUnsupported
	}
	return RetryVal(ctx, b.proto, "openat "+relPath, func() (io.ReadCloser, error) {
		inner, err := b.ensureConnected()
		if err != nil {
			return nil, err
		}
		o, ok := inner.(model.SeekableOpener)
		if !ok {
			return nil, ErrResumeUnsupported
		}
		rc, err := o.OpenAt(ctx, relPath, offset)
		return rc, b.markBrokenIf(err)
	})
}

func (b *lazyBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, mode os.FileMode) error {
	inner, err := b.ensureConnected()
	if err != nil {
		return err
	}
	if _, ok := inner.(model.LocalSender); !ok {
		return ErrResumeUnsupported
	}
	return Retry(ctx, b.proto, "send "+relPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		s, ok := inner.(model.LocalSender)
		if !ok {
			return ErrResumeUnsupported
		}
		return b.markBrokenIf(s.SendLocalFile(ctx, srcPath, relPath, mode))
	})
}

func (b *lazyBackend) PreloadRecursive(ctx context.Context, scope string) error {
	inner, err := b.ensureConnected()
	if err != nil {
		return err
	}
	p, ok := inner.(model.RecursivePreloader)
	if !ok {
		return nil
	}
	return p.PreloadRecursive(ctx, scope)
}

func (b *lazyBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	inner, err := b.ensureConnected()
	if err != nil {
		return err
	}
	if _, ok := inner.(model.LocalReceiver); !ok {
		return ErrResumeUnsupported
	}
	return Retry(ctx, b.proto, "recv "+relPath, func() error {
		inner, err := b.ensureConnected()
		if err != nil {
			return err
		}
		r, ok := inner.(model.LocalReceiver)
		if !ok {
			return ErrResumeUnsupported
		}
		return b.markBrokenIf(r.RecvToLocalFile(ctx, relPath, dstPath))
	})
}
