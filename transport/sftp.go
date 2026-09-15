package transport

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
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
// maxConcurrentRequests packets at once, and MaxPacketChecked(256KB) cuts
// the per-block overhead in half again.
func sftpFastOpts() []sftp.ClientOption {
	return []sftp.ClientOption{
		sftp.UseConcurrentWrites(true),
		sftp.UseConcurrentReads(true),
		sftp.MaxPacketChecked(256 << 10),
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
	conn, err := dialSSH(rawURL, insecure)
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
		return nil, fmt.Errorf("sftp: %v", err)
	}
	b := &SFTPBackend{sshShell: sshShell{client: conn.client, proto: "sftp", listCache: newListCache()}, sftp: client}
	b.base = expandHome(conn.basePath, client.Getwd)
	b.display = sshDisplayURL(conn, b.base)
	dial := func() (*sftpConn, error) {
		c, err := dialSSH(rawURL, insecure)
		if err != nil {
			return nil, err
		}
		sc, err := sftp.NewClient(c.client, sftpFastOpts()...)
		if err != nil {
			c.client.Close()
			return nil, fmt.Errorf("sftp: %v", err)
		}
		Log.Add("sftp", DirIn, "extra connection dialed")
		return &sftpConn{sftp: sc, ssh: c.client}, nil
	}
	b.pool = newConnPool(&sftpConn{sftp: client, ssh: conn.client}, parallel-1, dial, func(c *sftpConn) { c.close() })
	return b, nil
}

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
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		entry := model.FileEntry{
			RelPath: path.Join(relDir, info.Name()),
			Name:    info.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
			Mode:    info.Mode(),
		}
		if stat, ok := info.Sys().(*sftp.FileStat); ok && stat != nil {
			entry.ATime = time.Unix(int64(stat.Atime), 0)
		}
		result = append(result, entry)
	}
	return result, nil
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

// write streams src into fullPath from offset on a pooled connection, then
// applies mode. Close is checked: pkg/sftp reports the final flush status
// there, so ignoring it would report a short file as copied.
func (b *SFTPBackend) write(ctx context.Context, fullPath string, flags int, offset int64, src io.Reader, mode os.FileMode) error {
	if err := b.sftp.MkdirAll(path.Dir(fullPath)); err != nil {
		return err
	}
	conn, release := b.pool.acquire()
	defer release()
	f, err := conn.sftp.OpenFile(fullPath, flags)
	if err != nil {
		return err
	}
	stop := CancelCloser(ctx, f)
	if offset > 0 {
		if err = f.Truncate(offset); err == nil {
			_, err = f.Seek(offset, io.SeekStart)
		}
	}
	if err == nil {
		_, err = io.Copy(f, src)
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

func (b *SFTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	Log.Add("sftp", DirOut, "STOR "+relPath)
	err := b.write(ctx, b.abs(relPath), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0, src, mode)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
	}
	return err
}

// AppendFrom resumes at offset; the destination is truncated there first so a
// file that shrank underneath us does not end up with a zero-filled hole.
func (b *SFTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	Log.Add("sftp", DirOut, fmt.Sprintf("APPEND %s @%d", relPath, offset))
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
		return err
	}
	defer rd.Close()
	err = b.write(ctx, b.abs(relPath), os.O_WRONLY|os.O_CREATE, offset, rd, mode)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
	}
	return err
}

func (b *SFTPBackend) Open(_ context.Context, relPath string) (io.ReadCloser, error) {
	return b.openAt(relPath, 0)
}

func (b *SFTPBackend) OpenAt(_ context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	return b.openAt(relPath, offset)
}

func (b *SFTPBackend) openAt(relPath string, offset int64) (io.ReadCloser, error) {
	conn, release := b.pool.acquire()
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

func (b *SFTPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	err := b.sftp.Rename(b.abs(oldRelPath), b.abs(newRelPath))
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
