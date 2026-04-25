package transport

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"sc/model"
)

type lazyBackend struct {
	factory func() (model.Backend, error)
	inner   model.Backend
	once    sync.Once
	err     error
	display string
}

// NewLazyBackend returns a Backend that defers connection until first use.
func NewLazyBackend(display string, factory func() (model.Backend, error)) model.Backend {
	return &lazyBackend{factory: factory, display: display}
}

func (b *lazyBackend) connect() {
	b.once.Do(func() {
		Log.Add("conn", ">>>", "connecting to "+b.display)
		inner, err := b.factory()
		if err != nil {
			b.err = err
			Log.Add("conn", "ERR", b.display+": "+err.Error())
			return
		}
		b.inner = inner
		Log.Add("conn", "<<<", "connected to "+b.display)
	})
}

func (b *lazyBackend) BasePath() string {
	if b.inner != nil {
		return b.inner.BasePath()
	}
	return b.display
}

func (b *lazyBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	b.connect()
	if b.err != nil {
		return nil, b.err
	}
	return b.inner.List(ctx, relDir)
}

func (b *lazyBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	b.connect()
	if b.err != nil {
		return "", b.err
	}
	return b.inner.Checksum(ctx, relPath)
}

func (b *lazyBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	b.connect()
	if b.err != nil {
		return b.err
	}
	return b.inner.SetTimes(ctx, relPath, mtime, atime, btime)
}

func (b *lazyBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	b.connect()
	if b.err != nil {
		return b.err
	}
	return b.inner.CopyFrom(ctx, relPath, src, mode)
}

func (b *lazyBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	b.connect()
	if b.err != nil {
		return b.err
	}
	return b.inner.Rename(ctx, oldRelPath, newRelPath)
}

func (b *lazyBackend) Remove(ctx context.Context, relPath string) error {
	b.connect()
	if b.err != nil {
		return b.err
	}
	return b.inner.Remove(ctx, relPath)
}

func (b *lazyBackend) RemoveAll(ctx context.Context, relPath string) error {
	b.connect()
	if b.err != nil {
		return b.err
	}
	return b.inner.RemoveAll(ctx, relPath)
}

func (b *lazyBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	b.connect()
	if b.err != nil {
		return nil, b.err
	}
	return b.inner.Open(ctx, relPath)
}

func (b *lazyBackend) Close() error {
	if b.inner == nil {
		return nil
	}
	if c, ok := b.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (b *lazyBackend) ProbeChecksums() []string {
	b.connect()
	if b.err != nil {
		return nil
	}
	if p, ok := b.inner.(model.ChecksumProber); ok {
		return p.ProbeChecksums()
	}
	return nil
}

func (b *lazyBackend) SetChecksumAlgo(algo string) {
	if b.inner == nil {
		return
	}
	if p, ok := b.inner.(model.ChecksumProber); ok {
		p.SetChecksumAlgo(algo)
	}
}

func (b *lazyBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	b.connect()
	if b.err != nil {
		return b.err
	}
	if p, ok := b.inner.(model.ChecksumPrefetcher); ok {
		return p.PrefetchChecksums(ctx, scope, recursive)
	}
	return nil
}
