package transport

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

// SCPBackend is the shell-only ssh backend: listings come from find, bodies
// from cat. It is the fallback when the server has no SFTP subsystem.
type SCPBackend struct {
	sshShell
	pool *connPool[*ssh.Client]
}

// newSCPBackend builds an SCPBackend over an already-dialed sshConn. rawURL
// lets the pool dial fresh connections for parallel transfers (parallel-1 of
// them).
func newSCPBackend(conn *sshConn, rawURL string, insecure bool, parallel int) *SCPBackend {
	b := &SCPBackend{sshShell: sshShell{client: conn.client, proto: "scp", listCache: newListCache()}}
	b.base = expandHome(conn.basePath, b.home)
	b.display = sshDisplayURL(conn, b.base)
	dial := func() (*ssh.Client, error) {
		c, err := dialSSH(rawURL, insecure)
		if err != nil {
			return nil, err
		}
		Log.Add("scp", DirIn, "extra connection dialed")
		return c.client, nil
	}
	b.pool = newConnPool(conn.client, parallel-1, dial, func(c *ssh.Client) { c.Close() })
	return b
}

func (b *SCPBackend) Close() error {
	b.pool.close()
	return b.client.Close()
}

func (b *SCPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.findList)
}

func (b *SCPBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.findRecursive)
	return nil
}

func (b *SCPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	fullPath := b.abs(relPath)
	if _, err := b.run(ctx, "mkdir -p "+shellQuote(path.Dir(fullPath))); err != nil {
		return err
	}
	client, release := b.pool.acquire()
	defer release()
	err := b.exec(ctx, client, fmt.Sprintf("cat > %s && chmod %04o %s", shellQuote(fullPath), mode.Perm(), shellQuote(fullPath)), src, nil)
	b.listCache.invalidateAncestors(relPath)
	return err
}

func (b *SCPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	fullPath := b.abs(relPath)
	if _, err := b.run(ctx, "mkdir -p "+shellQuote(path.Dir(fullPath))); err != nil {
		return err
	}
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	client, release := b.pool.acquire()
	defer release()
	cmd := fmt.Sprintf("%s && cat >> %s && chmod %04o %s", truncateCmd(fullPath, offset), shellQuote(fullPath), mode.Perm(), shellQuote(fullPath))
	err = b.exec(ctx, client, cmd, rd, nil)
	b.listCache.invalidateAncestors(relPath)
	return err
}

func (b *SCPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	client, release := b.pool.acquire()
	return b.stream(ctx, client, "cat "+shellQuote(b.abs(relPath)), release)
}

func (b *SCPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	client, release := b.pool.acquire()
	return b.stream(ctx, client, b.tailCmd(relPath, offset), release)
}
