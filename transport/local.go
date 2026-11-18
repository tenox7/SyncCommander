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
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mmcloughlin/md4"
	"github.com/zeebo/xxh3"

	"sc/model"
)

type LocalBackend struct {
	base      string
	namesOnce sync.Once
	names     model.NameRule // how base's file system matches names, see nameRule
}

func NewLocalBackend(base string) *LocalBackend {
	return &LocalBackend{base: base}
}

func (b *LocalBackend) BasePath() string { return b.base }

// nameRule is how the file system under base matches names, found by one
// probe that writes nothing; the rule of a file system does not change.
func (b *LocalBackend) nameRule() model.NameRule {
	b.namesOnce.Do(func() {
		b.names = probeNames(b.base)
		b.names.Windows = windowsNames
	})
	return b.names
}

// windowsNames is whether local names follow the rules of Windows
// (model.WindowsName): a variable, so their tests run on every system.
var windowsNames = runtime.GOOS == "windows"

// LocalPath maps a slash-separated relPath onto the host filesystem. The path
// is cleaned by cleanRel first, so ".." segments can never climb above base,
// and isBaseRel knows the paths that land on base itself. It is "" for a
// relPath this system cannot hold (path): no file is that path.
func (b *LocalBackend) LocalPath(relPath string) string {
	p, err := b.path(relPath)
	if err != nil {
		return ""
	}
	return p
}

// path is LocalPath, with ErrBadName for a relPath this system cannot hold
// as it is: on Windows one with a name model.WindowsName refuses. Mapped,
// such a name landed on another file or on none, and a copy into it said
// OK (L25): a\b, a name from another system, was the file b in a folder a.
// Windows takes \ for a separator, and it is refused there with the rest;
// the replace below keeps a ..\ inside base all the same.
func (b *LocalBackend) path(relPath string) (string, error) {
	if windowsNames {
		for _, name := range strings.Split(cleanRel(relPath), "/") {
			if why := model.WindowsName(name); why != "" {
				return "", fmt.Errorf("local: %#q in %#q: %w: %s", name, relPath, ErrBadName, why)
			}
		}
	}
	if filepath.Separator != '/' {
		relPath = strings.ReplaceAll(relPath, string(filepath.Separator), "/")
	}
	return filepath.Join(b.base, filepath.FromSlash(cleanRel(relPath))), nil
}

func (b *LocalBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir, err := b.path(relDir)
	if err != nil {
		return nil, err
	}
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
		result = append(result, localEntry(path.Join(relDir, d.Name()), info))
	}
	return result, nil
}

// Stat reports relPath as the disk holds it now, a link as a link.
func (b *LocalBackend) Stat(_ context.Context, relPath string) (model.FileEntry, error) {
	p, err := b.path(relPath)
	if err != nil {
		return model.FileEntry{}, err
	}
	info, err := os.Lstat(p)
	if err != nil {
		return model.FileEntry{}, err
	}
	return localEntry(relPath, info), nil
}

// localEntry is the row of relPath, whose lstat is info.
func localEntry(relPath string, info fs.FileInfo) model.FileEntry {
	e := model.FileEntry{
		RelPath: relPath,
		Name:    path.Base(relPath),
		Size:    info.Size(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
		Mode:    info.Mode(),
	}
	if relPath == "" {
		e.Name = ""
	}
	fillTimes(&e, info)
	return e
}

func (b *LocalBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	return b.checksum(ctx, relPath, algo, 0, -1)
}

// ChecksumRange sums n bytes of relPath from off (model.RegionChecksummer).
func (b *LocalBackend) ChecksumRange(ctx context.Context, relPath, algo string, off, n int64) (string, error) {
	return b.checksum(ctx, relPath, algo, off, n)
}

// localAlgos are the algorithms sc computes itself, from a file's bytes.
var localAlgos = []string{"xxh3", "sha256", "sha1", "md5", "md4"}

// newHash returns a hash of algo, one of localAlgos.
func newHash(algo string) (hash.Hash, error) {
	switch algo {
	case "xxh3":
		return xxh3.New(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha1":
		return sha1.New(), nil
	case "md5":
		return md5.New(), nil
	case "md4":
		return md4.New(), nil
	}
	return nil, fmt.Errorf("no %q checksum", algo)
}

// checksum sums relPath from off with algo: n bytes, or to the end when
// n < 0. A file that ends before off+n is an error.
func (b *LocalBackend) checksum(ctx context.Context, relPath, algo string, off, n int64) (string, error) {
	h, err := newHash(algo)
	if err != nil {
		return "", err
	}
	p, err := b.path(relPath)
	if err != nil {
		return "", err
	}
	f, err := openRegular(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return "", err
		}
	}

	buf := make([]byte, 256*1024)
	for n != 0 {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		want := len(buf)
		if n > 0 && n < int64(want) {
			want = int(n)
		}
		got, err := f.Read(buf[:want])
		if got > 0 {
			if err := TakeIn(ctx, got); err != nil {
				return "", err
			}
			h.Write(buf[:got])
			if n > 0 {
				n -= int64(got)
			}
		}
		if err == io.EOF {
			if n > 0 {
				return "", io.ErrUnexpectedEOF
			}
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (b *LocalBackend) ChecksumAlgos(context.Context) ([]string, error) { return localAlgos, nil }

// Close does nothing: a local folder holds no connection.
func (b *LocalBackend) Close() error { return nil }

func (b *LocalBackend) SetTimes(_ context.Context, relPath string, mtime, atime, btime time.Time) error {
	if atime.IsZero() {
		atime = mtime
	}
	p, err := b.path(relPath)
	if err != nil {
		return err
	}
	return setTimes(p, mtime, atime, btime)
}

func (b *LocalBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	f, err := b.create(relPath, p.Mode)
	if err != nil {
		return err
	}
	return fill(ctx, f, src)
}

// AppendFrom writes the source from offset on at the end of relPath, which
// must hold exactly offset bytes (see openEnd).
func (b *LocalBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, _ model.PutParams, offset int64) error {
	f, err := b.openEnd(relPath, offset)
	if err != nil {
		return err
	}
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		f.Close()
		return err
	}
	defer rd.Close()
	return fill(ctx, f, rd)
}

// create opens relPath for a new body, making its folder. It never writes
// through a link, a pipe or a device (L2, L9).
func (b *LocalBackend) create(relPath string, mode os.FileMode) (*os.File, error) {
	p, err := b.path(relPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(p); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("local: refuse to overwrite existing directory %s", p)
	} else if err == nil && !fi.Mode().IsRegular() {
		return nil, specialFileErr(p, fi.Mode())
	}
	return os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|oNoFollow, mode)
}

// openEnd opens relPath to write on after its offset bytes. It never creates
// the file and refuses one of another size (L14, S8): writing past the end
// of a shorter file leaves a gap of zeros, and a longer one holds something
// else.
func (b *LocalBackend) openEnd(relPath string, offset int64) (*os.File, error) {
	p, err := b.path(relPath)
	if err != nil {
		return nil, err
	}
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, appendOffsetErr("local", relPath, -1, offset)
	case err != nil:
		return nil, err
	case fi.IsDir():
		return nil, fmt.Errorf("local: %s is a folder", p)
	case !fi.Mode().IsRegular():
		return nil, specialFileErr(p, fi.Mode())
	}
	f, err := os.OpenFile(p, os.O_WRONLY|oNoFollow, 0)
	if err != nil {
		return nil, err
	}
	if fi, err = f.Stat(); err == nil && fi.Size() != offset {
		err = appendOffsetErr("local", relPath, fi.Size(), offset)
	}
	if err == nil {
		_, err = f.Seek(offset, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// fill copies src into f, syncs f to disk and closes it. Without the sync, a
// crash soon after the rename could leave the file's name, size and time
// with bytes that never reached the disk, and the next compare would call
// it equal (L14). Close is checked: network file systems report write
// errors there.
func fill(ctx context.Context, f *os.File, src io.Reader) error {
	_, err := io.Copy(LimitWriter(ctx, f), src)
	if err == nil {
		err = syncFile(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// syncFile hands f's bytes to the disk (fsync), except on a file system
// that cannot (some network and FUSE file systems): nothing more to do.
func syncFile(f *os.File) error {
	err := fsync(f)
	if errors.Is(err, errors.ErrUnsupported) || errors.Is(err, syscall.EINVAL) {
		return nil
	}
	return err
}

// PartSize stats relPath's part (model.PartWriter).
func (b *LocalBackend) PartSize(_ context.Context, relPath string) (int64, error) {
	if _, err := b.path(relPath); err != nil {
		return 0, err // a part of a name the copy can never commit
	}
	p, err := b.path(model.PartPath(relPath))
	if err != nil {
		return 0, err
	}
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return 0, nil
	case err != nil:
		return 0, err
	case !fi.Mode().IsRegular():
		return 0, fmt.Errorf("local: %s is a %s: %w", p, model.KindName(fi.Mode()), model.ErrPartInTheWay)
	}
	return fi.Size(), nil
}

// CommitPart gives relPath's part its mode and renames it over relPath
// (model.PartWriter); fill synced its bytes. The rename replaces a file or
// a link in one step, and the mode is what creating the file with it would
// give, as before parts. A folder at relPath is never replaced.
func (b *LocalBackend) CommitPart(_ context.Context, relPath string, mode os.FileMode) error {
	final, err := b.path(relPath)
	if err != nil {
		return err
	}
	part, err := b.path(model.PartPath(relPath))
	if err != nil {
		return err
	}
	if fi, err := os.Lstat(final); err == nil && fi.IsDir() {
		return fmt.Errorf("local: %s is a folder: %w", final, fs.ErrExist)
	}
	// A mode the disk will not take (FAT, some network shares) does not
	// keep the part from its name (S20).
	cerr := chmodFile(part, mode.Perm()&^umask)
	if err := os.Rename(part, final); err != nil {
		return err
	}
	if cerr != nil {
		return fmt.Errorf("local: %s: %w: %w", relPath, model.ErrModeNotSet, cerr)
	}
	return nil
}

// chmodFile is os.Chmod. A test makes it fail, as a disk with no modes.
var chmodFile = os.Chmod

func (b *LocalBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(ctx, relPath, 0)
}

func (b *LocalBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	p, err := b.path(relPath)
	if err != nil {
		return nil, err
	}
	f, err := openRegular(p)
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
	p, err := b.path(relPath)
	if err != nil {
		return err
	}
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
	if err := renameBase("local", oldRelPath, newRelPath); err != nil {
		return err
	}
	oldPath, err := b.path(oldRelPath)
	if err != nil {
		return err
	}
	newPath, err := b.path(newRelPath)
	if err != nil {
		return err
	}
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
	p, err := b.path(relPath)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func (b *LocalBackend) RemoveAll(_ context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("local: refusing to remove the base directory")
	}
	full, err := b.path(relPath)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(full); err != nil {
		return err
	}
	if _, err := os.Stat(full); err == nil {
		return fmt.Errorf("local: %s still exists after RemoveAll", full)
	}
	return nil
}
