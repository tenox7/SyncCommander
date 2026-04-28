package transport

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mmcloughlin/md4"

	"sc/model"
)

type LocalBackend struct {
	base      string
	cksumAlgo string
}

func NewLocalBackend(base string) *LocalBackend {
	return &LocalBackend{base: base}
}

func (b *LocalBackend) BasePath() string {
	return b.base
}

func (b *LocalBackend) LocalPath(relPath string) string {
	return filepath.Join(b.base, relPath)
}

func (b *LocalBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := filepath.Join(b.base, relDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := make([]model.FileEntry, 0, len(entries))
	for _, d := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		info, err := d.Info()
		if err != nil {
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		rel := filepath.Join(relDir, d.Name())
		entry := model.FileEntry{
			RelPath: rel,
			Name:    d.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   d.IsDir(),
			Mode:    info.Mode(),
		}
		fillTimes(&entry, filepath.Join(dir, d.Name()))
		result = append(result, entry)
	}
	return result, nil
}

func (b *LocalBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	f, err := os.Open(filepath.Join(b.base, relPath))
	if err != nil {
		return "", err
	}
	defer f.Close()

	var h hash.Hash
	switch b.cksumAlgo {
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
	return []string{"sha256", "sha1", "md5", "md4"}
}

func (b *LocalBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
}

func (b *LocalBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	return setTimes(filepath.Join(b.base, relPath), mtime, atime, btime)
}

func (b *LocalBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	path := filepath.Join(b.base, relPath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}

func (b *LocalBackend) AppendFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode, offset int64) error {
	path := filepath.Join(b.base, relPath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	_, err = io.Copy(f, src)
	return err
}

func (b *LocalBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(b.base, relPath))
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (b *LocalBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	return os.Rename(filepath.Join(b.base, oldRelPath), filepath.Join(b.base, newRelPath))
}

func (b *LocalBackend) Remove(ctx context.Context, relPath string) error {
	return os.Remove(filepath.Join(b.base, relPath))
}

func (b *LocalBackend) RemoveAll(ctx context.Context, relPath string) error {
	return os.RemoveAll(filepath.Join(b.base, relPath))
}

func (b *LocalBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(b.base, relPath))
}
