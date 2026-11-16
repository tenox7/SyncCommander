package transport

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
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

// LocalPath maps a slash-separated relPath onto the host filesystem. The path
// is cleaned as if absolute first, so ".." segments can never climb above
// base.
func (b *LocalBackend) LocalPath(relPath string) string {
	if filepath.Separator != '/' {
		relPath = strings.ReplaceAll(relPath, string(filepath.Separator), "/")
	}
	return filepath.Join(b.base, filepath.FromSlash(path.Clean("/"+relPath)))
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
		if errors.Is(err, fs.ErrNotExist) {
			continue // removed since ReadDir
		}
		if err != nil {
			// Skipping it would make a short listing look complete: the
			// scan would show the entry as missing and mirror delete it on
			// the other side. Fail the folder instead.
			return nil, err
		}
		// A link, pipe or device is listed with its type, so the tree shows
		// it and a copy does not take its name for free; nothing reads it.
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
	f, err := openRegular(b.LocalPath(relPath))
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
			if err := TakeIn(ctx, n); err != nil {
				return "", err
			}
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

func (b *LocalBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	return b.write(ctx, relPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode, 0, src)
}

func (b *LocalBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	return b.write(ctx, relPath, os.O_CREATE|os.O_WRONLY, mode, offset, rd)
}

// write streams src into relPath from offset, cutting the file there first.
// Close is checked: network filesystems report write errors there.
func (b *LocalBackend) write(ctx context.Context, relPath string, flags int, mode os.FileMode, offset int64, src io.Reader) error {
	p := b.LocalPath(relPath)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	if fi, err := os.Lstat(p); err == nil && fi.IsDir() {
		return fmt.Errorf("local: refuse to overwrite existing directory %s", p)
	} else if err == nil && !fi.Mode().IsRegular() {
		return specialFileErr(p, fi.Mode())
	}
	f, err := os.OpenFile(p, flags|oNoFollow, mode)
	if err != nil {
		return err
	}
	if offset > 0 {
		if err = f.Truncate(offset); err == nil {
			_, err = f.Seek(offset, io.SeekStart)
		}
	}
	if err == nil {
		_, err = io.Copy(LimitWriter(ctx, f), src)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (b *LocalBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(ctx, relPath, 0)
}

func (b *LocalBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	f, err := openRegular(b.LocalPath(relPath))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return LimitReadCloser(ctx, f), nil
}

func (b *LocalBackend) Mkdir(_ context.Context, relPath string, mode os.FileMode) error {
	if mode == 0 {
		mode = 0755
	}
	p := b.LocalPath(relPath)
	if fi, err := os.Lstat(p); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return specialFileErr(p, fi.Mode()) // files written in it would land in the link's target
	}
	return os.MkdirAll(p, mode.Perm())
}

// openRegular opens a local file for reading, and nothing else: a named pipe
// would block the open until a writer comes, and a link would be read
// through, out of the tree (L2, L9).
func openRegular(p string) (*os.File, error) {
	fi, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, specialFileErr(p, fi.Mode())
	}
	return os.Open(p)
}

func specialFileErr(p string, m os.FileMode) error {
	return fmt.Errorf("local: %s is a %s: %w", p, model.KindName(m), ErrSpecialFile)
}

// Rename refuses an existing target, which os.Rename would replace. A target
// that is the same file, as in a case-only rename on a case-insensitive disk,
// is not in the way.
func (b *LocalBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	oldPath, newPath := b.LocalPath(oldRelPath), b.LocalPath(newRelPath)
	if ni, err := os.Lstat(newPath); err == nil {
		if oi, err := os.Lstat(oldPath); err != nil || !os.SameFile(oi, ni) {
			return renameTargetTaken("local", newRelPath)
		}
	}
	return os.Rename(oldPath, newPath)
}

func (b *LocalBackend) Remove(_ context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("local: refusing to remove the base directory")
	}
	return os.Remove(b.LocalPath(relPath))
}

func (b *LocalBackend) RemoveAll(_ context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("local: refusing to remove the base directory")
	}
	full := b.LocalPath(relPath)
	if err := os.RemoveAll(full); err != nil {
		return err
	}
	if _, err := os.Stat(full); err == nil {
		return fmt.Errorf("local: %s still exists after RemoveAll", full)
	}
	return nil
}
