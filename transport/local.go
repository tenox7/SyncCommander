package transport

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/mmcloughlin/md4"
	"github.com/zeebo/xxh3"

	"sc/model"
)

type LocalBackend struct {
	base      string
	cksumAlgo atomic.Value // string; set by negotiation while checksum workers may already run
}

func NewLocalBackend(base string) *LocalBackend {
	return &LocalBackend{base: base}
}

func (b *LocalBackend) BasePath() string { return b.base }

// LocalPath maps a slash-separated relPath onto the host filesystem.
func (b *LocalBackend) LocalPath(relPath string) string {
	return filepath.Join(b.base, filepath.FromSlash(relPath))
}

func (b *LocalBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	entries, err := os.ReadDir(b.LocalPath(relDir))
	if err != nil {
		return nil, err
	}
	result := make([]model.FileEntry, 0, len(entries))
	for _, d := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		info, err := d.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		entry := model.FileEntry{
			RelPath: path.Join(relDir, d.Name()),
			Name:    d.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   d.IsDir(),
			Mode:    info.Mode(),
		}
		fillTimes(&entry, info)
		result = append(result, entry)
	}
	return result, nil
}

func (b *LocalBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	f, err := os.Open(b.LocalPath(relPath))
	if err != nil {
		return "", err
	}
	defer f.Close()

	var h hash.Hash
	switch b.cksumAlgo.Load() {
	case "xxh3":
		h = xxh3.New()
	case "sha1":
		h = sha1.New()
	case "md5":
		h = md5.New()
	case "md4":
		h = md4.New()
	default:
		h = sha256.New()
	}
	buf := make([]byte, 256*1024)
	for {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		n, err := f.Read(buf)
		if n > 0 {
			TakeIn(n)
			h.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (b *LocalBackend) ProbeChecksums() []string {
	return []string{"xxh3", "sha256", "sha1", "md5", "md4"}
}

func (b *LocalBackend) SetChecksumAlgo(algo string) { b.cksumAlgo.Store(algo) }

func (b *LocalBackend) SetTimes(_ context.Context, relPath string, mtime, atime, btime time.Time) error {
	if atime.IsZero() {
		atime = mtime
	}
	return setTimes(b.LocalPath(relPath), mtime, atime, btime)
}

func (b *LocalBackend) CopyFrom(_ context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	return b.write(relPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode, 0, src)
}

func (b *LocalBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	return b.write(relPath, os.O_CREATE|os.O_WRONLY, mode, offset, rd)
}

// write streams src into relPath from offset, cutting the file there first.
// Close is checked: network filesystems report write errors there.
func (b *LocalBackend) write(relPath string, flags int, mode os.FileMode, offset int64, src io.Reader) error {
	p := b.LocalPath(relPath)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	if fi, err := os.Stat(p); err == nil && fi.IsDir() {
		return fmt.Errorf("local: refuse to overwrite existing directory %s", p)
	}
	f, err := os.OpenFile(p, flags, mode)
	if err != nil {
		return err
	}
	if offset > 0 {
		if err = f.Truncate(offset); err == nil {
			_, err = f.Seek(offset, io.SeekStart)
		}
	}
	if err == nil {
		_, err = io.Copy(LimitWriter(f), src)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (b *LocalBackend) Open(_ context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(nil, relPath, 0)
}

func (b *LocalBackend) OpenAt(_ context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	f, err := os.Open(b.LocalPath(relPath))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return LimitReadCloser(f), nil
}

func (b *LocalBackend) Mkdir(_ context.Context, relPath string, mode os.FileMode) error {
	if mode == 0 {
		mode = 0755
	}
	return os.MkdirAll(b.LocalPath(relPath), mode.Perm())
}

func (b *LocalBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	return os.Rename(b.LocalPath(oldRelPath), b.LocalPath(newRelPath))
}

func (b *LocalBackend) Remove(_ context.Context, relPath string) error {
	return os.Remove(b.LocalPath(relPath))
}

func (b *LocalBackend) RemoveAll(_ context.Context, relPath string) error {
	full := b.LocalPath(relPath)
	if err := os.RemoveAll(full); err != nil {
		return err
	}
	if _, err := os.Stat(full); err == nil {
		return fmt.Errorf("local: %s still exists after RemoveAll", full)
	}
	return nil
}
