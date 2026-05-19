package transport

import (
	"context"
	"fmt"
	"io"
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
	base      string
	rawURL    string
	client    *sftp.Client
	sshClient *ssh.Client
	display   string
	cksumAlgo string
	cksumCmds map[string]string
	pool      *sshPool[*sftpConn]
}

func NewSFTPBackend(rawURL string, parallel int) (*SFTPBackend, error) {
	conn, err := dialSSH(rawURL)
	if err != nil {
		return nil, err
	}
	b, err := newSFTPBackendFromConn(conn, rawURL, parallel)
	if err != nil {
		conn.client.Close()
	}
	return b, err
}

// newSFTPBackendFromConn builds an SFTPBackend over an already-dialed sshConn.
// rawURL is retained so the pool can dial fresh SSH connections for parallel
// transfers. parallel sizes the pool's extras (= parallel-1, so total
// concurrent slots = parallel). On error the caller is responsible for
// closing conn.client.
func newSFTPBackendFromConn(conn *sshConn, rawURL string, parallel int) (*SFTPBackend, error) {
	sftpClient, err := sftp.NewClient(conn.client, sftpFastOpts()...)
	if err != nil {
		return nil, fmt.Errorf("sftp: %v", err)
	}

	remotePath := conn.basePath
	switch {
	case remotePath == "" || remotePath == "/" || remotePath == "/~":
		if wd, err := sftpClient.Getwd(); err == nil {
			remotePath = wd
		} else {
			remotePath = "/"
		}
	case strings.HasPrefix(remotePath, "/~/"):
		if wd, err := sftpClient.Getwd(); err == nil {
			remotePath = path.Join(wd, remotePath[3:])
		}
	}

	primary := &sftpConn{sftp: sftpClient, ssh: conn.client}
	maxExtras := parallel - 1
	dial := func() (*sftpConn, error) {
		c, err := dialSSH(rawURL)
		if err != nil {
			return nil, err
		}
		sc, err := sftp.NewClient(c.client, sftpFastOpts()...)
		if err != nil {
			c.client.Close()
			return nil, fmt.Errorf("sftp: %v", err)
		}
		Log.Add("sftp", "<<<", "extra connection dialed")
		return &sftpConn{sftp: sc, ssh: c.client}, nil
	}
	pool := newSSHPool(primary, maxExtras, dial, func(c *sftpConn) { c.close() })

	return &SFTPBackend{
		base:      remotePath,
		rawURL:    rawURL,
		client:    sftpClient,
		sshClient: conn.client,
		display:   sshDisplayURL(conn, remotePath),
		pool:      pool,
	}, nil
}

func (b *SFTPBackend) BasePath() string { return b.display }

func (b *SFTPBackend) Close() error {
	b.pool.close()
	b.client.Close()
	return b.sshClient.Close()
}

func (b *SFTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := path.Join(b.base, relDir)
	Log.Add("sftp", ">>>", "READDIR "+dir)
	entries, err := b.client.ReadDir(dir)
	if err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return nil, err
	}
	Log.Add("sftp", "<<<", fmt.Sprintf("%d entries", len(entries)))
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

func (b *SFTPBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "" {
		return "", fmt.Errorf("no checksum algorithm configured")
	}
	cmd := fmt.Sprintf("%s %s", b.cksumCmds[b.cksumAlgo], shellQuote(path.Join(b.base, relPath)))
	out, err := runSSHCmdCtx(ctx, b.sshClient, "sftp", cmd)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum output")
	}
	return strings.TrimPrefix(fields[0], "\\"), nil
}

func (b *SFTPBackend) ProbeChecksums() []string {
	if b.cksumCmds == nil {
		run := func(cmd string) (string, error) { return runSSHCmd(b.sshClient, "sftp", cmd) }
		var algos []string
		algos, b.cksumCmds = probeSSHChecksums(run)
		return algos
	}
	var algos []string
	seen := make(map[string]bool)
	for _, p := range cksumProbes {
		if _, ok := b.cksumCmds[p.algo]; ok && !seen[p.algo] {
			algos = append(algos, p.algo)
			seen[p.algo] = true
		}
	}
	return algos
}

func (b *SFTPBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
}

func (b *SFTPBackend) SetTimes(_ context.Context, relPath string, mtime, atime, _ time.Time) error {
	err := b.client.Chtimes(path.Join(b.base, relPath), atime, mtime)
	if err != nil {
		Log.Add("sftp", "ERR", "CHTIMES "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	Log.Add("sftp", ">>>", "STOR "+relPath)
	fullPath := path.Join(b.base, relPath)
	if err := b.client.MkdirAll(path.Dir(fullPath)); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	conn, release := b.pool.acquire()
	defer release()
	f, err := conn.sftp.Create(fullPath)
	if err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	defer f.Close()
	defer cancelCloser(ctx, f)()
	if _, err := io.Copy(f, src); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	if err := conn.sftp.Chmod(fullPath, mode); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	return nil
}

func (b *SFTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	Log.Add("sftp", ">>>", fmt.Sprintf("APPEND %s @%d", relPath, offset))
	fullPath := path.Join(b.base, relPath)
	if err := b.client.MkdirAll(path.Dir(fullPath)); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	defer rd.Close()
	conn, release := b.pool.acquire()
	defer release()
	f, err := conn.sftp.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE)
	if err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	defer f.Close()
	defer cancelCloser(ctx, f)()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	if _, err := io.Copy(f, rd); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	if err := conn.sftp.Chmod(fullPath, mode); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	return nil
}

func (b *SFTPBackend) OpenAt(_ context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	conn, release := b.pool.acquire()
	rc, err := conn.sftp.Open(path.Join(b.base, relPath))
	if err != nil {
		release()
		Log.Add("sftp", "ERR", "OPEN "+relPath+": "+err.Error())
		return nil, err
	}
	if _, err := rc.Seek(offset, io.SeekStart); err != nil {
		rc.Close()
		release()
		Log.Add("sftp", "ERR", "SEEK "+relPath+": "+err.Error())
		return nil, err
	}
	return &sftpPooledReader{ReadCloser: rc, release: release}, nil
}

func (b *SFTPBackend) Mkdir(_ context.Context, relPath string, mode os.FileMode) error {
	fullPath := path.Join(b.base, relPath)
	Log.Add("sftp", ">>>", "MKDIR "+relPath)
	if err := b.client.MkdirAll(fullPath); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	if mode != 0 {
		_ = b.client.Chmod(fullPath, mode.Perm())
	}
	return nil
}

func (b *SFTPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	err := b.client.Rename(path.Join(b.base, oldRelPath), path.Join(b.base, newRelPath))
	if err != nil {
		Log.Add("sftp", "ERR", "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) Remove(_ context.Context, relPath string) error {
	err := b.client.Remove(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("sftp", "ERR", "REMOVE "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) RemoveAll(_ context.Context, relPath string) error {
	err := b.removeAll(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("sftp", "ERR", "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) removeAll(fullPath string) error {
	info, err := b.client.Stat(fullPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return b.client.Remove(fullPath)
	}
	entries, err := b.client.ReadDir(fullPath)
	if err != nil {
		return err
	}
	for _, e := range entries {
		child := path.Join(fullPath, e.Name())
		if e.IsDir() {
			if err := b.removeAll(child); err != nil {
				return err
			}
			continue
		}
		if err := b.client.Remove(child); err != nil {
			return err
		}
	}
	return b.client.RemoveDirectory(fullPath)
}

func (b *SFTPBackend) Open(_ context.Context, relPath string) (io.ReadCloser, error) {
	conn, release := b.pool.acquire()
	rc, err := conn.sftp.Open(path.Join(b.base, relPath))
	if err != nil {
		release()
		Log.Add("sftp", "ERR", "OPEN "+relPath+": "+err.Error())
		return nil, err
	}
	return &sftpPooledReader{ReadCloser: rc, release: release}, nil
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
