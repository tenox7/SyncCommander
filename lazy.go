package main

import (
	"context"
	"io"
	"os"
	"sync"
	"time"
)

type lazyBackend struct {
	factory func() (Backend, error)
	inner   Backend
	once    sync.Once
	err     error
	display string
}

func (b *lazyBackend) connect() {
	b.once.Do(func() {
		remoteLog.Add("conn", ">>>", "connecting to "+b.display)
		b.inner, b.err = b.factory()
		if b.err != nil {
			remoteLog.Add("conn", "ERR", b.display+": "+b.err.Error())
			return
		}
		remoteLog.Add("conn", "<<<", "connected to "+b.display)
	})
}

func (b *lazyBackend) BasePath() string {
	if b.inner != nil {
		return b.inner.BasePath()
	}
	return b.display
}

func (b *lazyBackend) List(ctx context.Context, relDir string) ([]FileEntry, error) {
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

func (b *lazyBackend) probeChecksums() []string {
	b.connect()
	if b.err != nil {
		return nil
	}
	if p, ok := b.inner.(checksumProber); ok {
		return p.probeChecksums()
	}
	return nil
}

func (b *lazyBackend) setChecksumAlgo(algo string) {
	if b.inner == nil {
		return
	}
	if p, ok := b.inner.(checksumProber); ok {
		p.setChecksumAlgo(algo)
	}
}
