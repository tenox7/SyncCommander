package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

// sftpFastOpts returns the throughput tuning applied to every sftp.Client
// the backend opens. The pkg/sftp defaults are pessimistic: maxPacket=32KB
// and writes are synchronous (each Write blocks for STATUS), so per-stream
// throughput collapses to roughly maxPacket/RTT. UseConcurrentWrites lets
// File.ReadFrom (which io.Copy invokes) pipeline up to
// maxConcurrentRequests packets at once. The packet size stays at the 32KB
// default: MaxPacketChecked rejects anything larger and fails the connect,
// and MaxPacketUnchecked silently truncates downloads, because the
// concurrent reader treats a short READ reply as EOF while pkg/sftp servers
// (rclone, sftpgo) clamp replies to 32KB. 64 in-flight 32KB packets already
// fill the 2MB SSH channel window, so a bigger packet buys no throughput.
func sftpFastOpts() []sftp.ClientOption {
	return []sftp.ClientOption{
		sftp.UseConcurrentWrites(true),
		sftp.UseConcurrentReads(true),
	}
}

type sftpConn struct {
	sftp *sftp.Client
	ssh  *ssh.Client
}

func (c *sftpConn) close() {
	if c.sftp != nil {
		c.sftp.Close()
	}
	if c.ssh != nil {
		c.ssh.Close()
	}
}

type SFTPBackend struct {
	sshShell
	sftp *sftp.Client
	pool *connPool[*sftpConn]
}

func NewSFTPBackend(rawURL string, insecure bool, parallel int) (*SFTPBackend, error) {
	return dialSFTPBackend(context.Background(), rawURL, insecure, parallel)
}

// dialSFTPBackend is NewSFTPBackend with ctx bounding the connect.
func dialSFTPBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*SFTPBackend, error) {
	conn, err := dialSSH(ctx, rawURL, insecure)
	if err != nil {
		return nil, err
	}
	b, err := newSFTPBackend(conn, rawURL, insecure, parallel)
	if err != nil {
		conn.client.Close()
	}
	return b, err
}

// newSFTPBackend builds an SFTPBackend over an already-dialed sshConn; on
// error the caller closes conn.client. rawURL lets the pool dial extra
// connections for parallel transfers (parallel-1 of them).
func newSFTPBackend(conn *sshConn, rawURL string, insecure bool, parallel int) (*SFTPBackend, error) {
	client, err := sftp.NewClient(conn.client, sftpFastOpts()...)
	if err != nil {
		return nil, fmt.Errorf("sftp: %w", err)
	}
	b := &SFTPBackend{sshShell: sshShell{client: conn.client, proto: "sftp", listCache: newListCache()}, sftp: client}
	if b.base, err = expandHome(conn.basePath, client.Getwd); err != nil {
		client.Close()
		return nil, fmt.Errorf("sftp: %w", err)
	}
	b.display = sshDisplayURL(conn, b.base)
	dial := func(ctx context.Context) (*sftpConn, error) {
		c, err := dialSSH(ctx, rawURL, insecure)
		if err != nil {
			return nil, err
		}
		sc, err := sftp.NewClient(c.client, sftpFastOpts()...)
		if err != nil {
			c.client.Close()
			return nil, fmt.Errorf("sftp: %w", err)
		}
		Log.Add("sftp", DirIn, "extra connection dialed")
		return &sftpConn{sftp: sc, ssh: c.client}, nil
	}
	b.pool = newConnPool(&sftpConn{sftp: client, ssh: conn.client}, parallel-1, dial, func(c *sftpConn) { c.close() })
	return b, nil
}

// SetParallel keeps n-1 extra connections, the primary being the n-th.
func (b *SFTPBackend) SetParallel(n int) { b.pool.resize(n - 1) }

func (b *SFTPBackend) Close() error {
	b.pool.close()
	b.sftp.Close()
	return b.client.Close()
}

func (b *SFTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.readDir)
}

// PreloadRecursive lists the whole scope with one find over the shell. On an
// SFTP-only server that fails and List goes back to per-dir ReadDir.
func (b *SFTPBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.findRecursive)
	return nil
}

func (b *SFTPBackend) readDir(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := b.abs(relDir)
	Log.Add("sftp", DirOut, "READDIR "+dir)
	entries, err := b.sftp.ReadDir(dir)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
		return nil, err
	}
	Log.Add("sftp", DirIn, fmt.Sprintf("%d entries", len(entries)))
	result := make([]model.FileEntry, 0, len(entries))
	for _, info := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Links, pipes and devices are listed with their type (L2, L9).
		result = append(result, sftpEntry(path.Join(relDir, info.Name()), info))
	}
	return result, nil
}

// Stat lstats relPath on the server: one request, no listing.
func (b *SFTPBackend) Stat(_ context.Context, relPath string) (model.FileEntry, error) {
	info, err := b.sftp.Lstat(b.abs(relPath))
	if err != nil {
		return model.FileEntry{}, fmt.Errorf("sftp: stat %s: %w", relPath, err)
	}
	return sftpEntry(relPath, info), nil
}

// sftpEntry is the row of relPath, whose attributes are info.
func sftpEntry(relPath string, info os.FileInfo) model.FileEntry {
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
	if stat, ok := info.Sys().(*sftp.FileStat); ok && stat != nil {
		e.ATime = time.Unix(int64(stat.Atime), 0)
	}
	return e
}

func (b *SFTPBackend) SetTimes(_ context.Context, relPath string, mtime, atime, _ time.Time) error {
	if atime.IsZero() {
		atime = mtime
	}
	err := b.sftp.Chtimes(b.abs(relPath), atime, mtime)
	b.listCache.invalidate(parentDir(relPath))
	if err != nil {
		Log.Add("sftp", DirErr, "CHTIMES "+relPath+": "+err.Error())
	}
	return err
}

// put writes relPath on a pooled connection and then applies mode: a new
// body when offset < 0, else the rest of the file after its offset bytes.
// An append never creates the file and refuses one of another size (L14,
// S8): writing past the end of a shorter file leaves a gap of zeros. The
// source opens once the file is ready. The data is synced when the server
// can, and Close is checked: pkg/sftp reports the final flush status there,
// so ignoring it would report a short file as copied.
func (b *SFTPBackend) put(ctx context.Context, relPath string, offset int64, mode os.FileMode, open func() (io.ReadCloser, error)) error {
	fullPath := b.abs(relPath)
	flags := os.O_WRONLY
	if offset < 0 {
		if err := b.sftp.MkdirAll(path.Dir(fullPath)); err != nil {
			return err
		}
		flags |= os.O_CREATE | os.O_TRUNC
	}
	conn, release := b.pool.acquire(ctx)
	defer release()
	f, err := conn.sftp.OpenFile(fullPath, flags)
	if offset >= 0 && errors.Is(err, fs.ErrNotExist) {
		return appendOffsetErr("sftp", relPath, -1, offset)
	}
	if err != nil {
		return err
	}
	stop := CancelCloser(ctx, f)
	if offset >= 0 {
		var fi os.FileInfo
		if fi, err = f.Stat(); err == nil && fi.Size() != offset {
			err = appendOffsetErr("sftp", relPath, fi.Size(), offset)
		}
		if err == nil {
			_, err = f.Seek(offset, io.SeekStart)
		}
	}
	var src io.ReadCloser
	if err == nil {
		src, err = open()
	}
	if err == nil {
		_, err = io.Copy(f, src)
		src.Close()
	}
	if err == nil {
		err = syncSFTP(f)
	}
	stop()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = conn.sftp.Chmod(fullPath, mode)
	}
	return err
}

// syncSFTP asks the server to sync f to disk (fsync@openssh.com, L14). A
// server without the extension has no way to, so that is no error.
func syncSFTP(f *sftp.File) error {
	err := f.Sync()
	var se *sftp.StatusError
	if errors.As(err, &se) && se.FxCode() == sftp.ErrSSHFxOpUnsupported {
		return nil
	}
	return err
}

func (b *SFTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	Log.Add("sftp", DirOut, "STOR "+relPath)
	err := b.put(ctx, relPath, -1, p.Mode, func() (io.ReadCloser, error) { return io.NopCloser(src), nil })
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
	}
	return err
}

// AppendFrom writes the source from offset on at the end of relPath, which
// must hold exactly offset bytes (see put).
func (b *SFTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	Log.Add("sftp", DirOut, fmt.Sprintf("APPEND %s @%d", relPath, offset))
	err := b.put(ctx, relPath, offset, p.Mode, func() (io.ReadCloser, error) { return src.OpenAt(ctx, offset) })
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
	}
	return err
}

// PartSize stats relPath's part (model.PartWriter).
func (b *SFTPBackend) PartSize(_ context.Context, relPath string) (int64, error) {
	p := b.abs(model.PartPath(relPath))
	fi, err := b.sftp.Lstat(p)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return 0, nil
	case err != nil:
		return 0, err
	case !fi.Mode().IsRegular():
		return 0, fmt.Errorf("sftp: %s is a %s: %w", p, model.KindName(fi.Mode()), model.ErrPartInTheWay)
	}
	return fi.Size(), nil
}

// CommitPart renames relPath's part over relPath (model.PartWriter); put
// synced it when the server could. posix-rename@openssh.com is rename(2),
// which replaces a file or a link in one step, never a folder. Plain
// SSH_FXP_RENAME refuses a name in use on OpenSSH, so without the extension
// the file there goes first: a moment with no file at that name.
func (b *SFTPBackend) CommitPart(_ context.Context, relPath string, mode os.FileMode) error {
	Log.Add("sftp", DirOut, "COMMIT "+relPath)
	err := b.commitPart(model.PartPath(relPath), relPath, mode)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, "COMMIT "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) commitPart(partRel, relPath string, mode os.FileMode) error {
	part, final := b.abs(partRel), b.abs(relPath)
	if model.PartMode(mode) != mode.Perm() {
		if err := b.sftp.Chmod(part, mode); err != nil {
			return err
		}
	}
	folder := func() error {
		if fi, err := b.sftp.Lstat(final); err == nil && fi.IsDir() {
			return fmt.Errorf("sftp: %s is a folder: %w", final, fs.ErrExist)
		}
		return nil
	}
	if _, ok := b.sftp.HasExtension("posix-rename@openssh.com"); ok {
		err := b.sftp.PosixRename(part, final)
		if err != nil {
			if ferr := folder(); ferr != nil {
				return ferr
			}
		}
		return err
	}
	if err := folder(); err != nil {
		return err
	}
	if err := b.sftp.Remove(final); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return b.sftp.Rename(part, final)
}

func (b *SFTPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.openAt(ctx, relPath, 0)
}

func (b *SFTPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	return b.openAt(ctx, relPath, offset)
}

func (b *SFTPBackend) openAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	conn, release := b.pool.acquire(ctx)
	rc, err := conn.sftp.Open(b.abs(relPath))
	if err == nil && offset > 0 {
		if _, err = rc.Seek(offset, io.SeekStart); err != nil {
			rc.Close()
		}
	}
	if err != nil {
		release()
		Log.Add("sftp", DirErr, "OPEN "+relPath+": "+err.Error())
		return nil, err
	}
	return &sftpPooledReader{ReadCloser: rc, release: release}, nil
}

func (b *SFTPBackend) Mkdir(_ context.Context, relPath string, mode os.FileMode) error {
	fullPath := b.abs(relPath)
	Log.Add("sftp", DirOut, "MKDIR "+relPath)
	err := b.sftp.MkdirAll(fullPath)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
		return err
	}
	if mode != 0 {
		_ = b.sftp.Chmod(fullPath, mode.Perm())
	}
	return nil
}

// Rename refuses an existing target. It then prefers
// posix-rename@openssh.com, which is rename(2); plain SSH_FXP_RENAME needs
// hard links on OpenSSH. A case-only rename skips the check: on a
// case-insensitive server the target is the file itself.
func (b *SFTPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	if !strings.EqualFold(oldRelPath, newRelPath) {
		if _, err := b.sftp.Lstat(b.abs(newRelPath)); err == nil {
			return renameTargetTaken("sftp", newRelPath)
		}
	}
	rename := b.sftp.Rename
	if _, ok := b.sftp.HasExtension("posix-rename@openssh.com"); ok {
		rename = b.sftp.PosixRename
	}
	err := rename(b.abs(oldRelPath), b.abs(newRelPath))
	b.listCache.forgetRenamed(oldRelPath, newRelPath)
	if err != nil {
		Log.Add("sftp", DirErr, "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) Remove(_ context.Context, relPath string) error {
	err := b.sftp.Remove(b.abs(relPath))
	b.listCache.invalidate(parentDir(relPath))
	if err != nil {
		Log.Add("sftp", DirErr, "REMOVE "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) RemoveAll(_ context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return fmt.Errorf("sftp: refusing to remove the base directory")
	}
	err := b.removeAll(b.abs(relPath))
	b.listCache.forgetRemoved(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) removeAll(fullPath string) error {
	info, err := b.sftp.Stat(fullPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return b.sftp.Remove(fullPath)
	}
	entries, err := b.sftp.ReadDir(fullPath)
	if err != nil {
		return err
	}
	for _, e := range entries {
		child := path.Join(fullPath, e.Name())
		if e.IsDir() {
			err = b.removeAll(child)
		} else {
			err = b.sftp.Remove(child)
		}
		if err != nil {
			return err
		}
	}
	return b.sftp.RemoveDirectory(fullPath)
}

// sftpPooledReader wraps an sftp.File so the underlying connection is
// released back to the pool when the caller closes the reader.
type sftpPooledReader struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (r *sftpPooledReader) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.release)
	return err
}
