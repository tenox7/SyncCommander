package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

// SCPBackend is the shell-only ssh backend: listings come from find, bodies
// from cat. It is the fallback when the server has no SFTP subsystem.
type SCPBackend struct {
	sshShell
	pool *connPool[*ssh.Client]
}

// newSCPBackend builds an SCPBackend over an already-dialed sshConn; on
// error the caller closes conn.client. rawURL lets the pool dial fresh
// connections for parallel transfers (parallel-1 of them).
func newSCPBackend(conn *sshConn, rawURL string, insecure bool, parallel int) (*SCPBackend, error) {
	b := &SCPBackend{sshShell: sshShell{client: conn.client, proto: "scp", listCache: newListCache()}}
	base, err := expandHome(conn.basePath, b.home)
	if err != nil {
		return nil, fmt.Errorf("scp: %w", err)
	}
	b.base = base
	b.display = sshDisplayURL(conn, b.base)
	dial := func(ctx context.Context) (*ssh.Client, error) {
		c, err := dialSSH(ctx, rawURL, insecure)
		if err != nil {
			return nil, err
		}
		Log.Add("scp", DirIn, "extra connection dialed")
		return c.client, nil
	}
	b.pool = newConnPool(conn.client, parallel-1, dial, func(c *ssh.Client) { c.Close() })
	return b, nil
}

// SetParallel keeps n-1 extra connections, the primary being the n-th.
func (b *SCPBackend) SetParallel(n int) { b.pool.resize(n - 1) }

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

// Exit codes of the shell checks below, so the error says what went wrong
// rather than "exited with status 1".
const (
	exitNotAtOffset = 75 // the file to append to does not hold offset bytes
	exitFolder      = 76 // a folder is where the part goes
	exitNoPart      = 77 // the part to commit is gone
)

// exitCode is the status cmd ended with, or -1 when it did not end with one.
func exitCode(err error) int {
	var ee *ssh.ExitError
	if errors.As(err, &ee) {
		return ee.ExitStatus()
	}
	return -1
}

func (b *SCPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	fullPath := b.abs(relPath)
	if _, err := b.run(ctx, "mkdir -p "+shellQuote(path.Dir(fullPath))); err != nil {
		return err
	}
	client, release := b.pool.acquire(ctx)
	defer release()
	err := b.exec(ctx, client, fmt.Sprintf("cat > %s && chmod %04o %s", shellQuote(fullPath), p.Mode.Perm(), shellQuote(fullPath)), src, nil)
	b.listCache.invalidateAncestors(relPath)
	return err
}

// AppendFrom writes the source from offset on at the end of relPath. The
// file must be there and hold exactly offset bytes, checked on the server
// before anything is written (L14, S8): a dd seek used to extend a missing
// or shorter file with zeros.
func (b *SCPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	fp := shellQuote(b.abs(relPath))
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	client, release := b.pool.acquire(ctx)
	defer release()
	cmd := fmt.Sprintf("n=$(wc -c 2>/dev/null < %[1]s) && [ ! -L %[1]s ] && [ $n -eq %[2]d ] || exit %[3]d; cat >> %[1]s && chmod %04[4]o %[1]s",
		fp, offset, exitNotAtOffset, p.Mode.Perm())
	err = b.exec(ctx, client, cmd, rd, nil)
	b.listCache.invalidateAncestors(relPath)
	if exitCode(err) == exitNotAtOffset {
		err = fmt.Errorf("scp: %s is not a file of %d bytes to append to: %w", relPath, offset, ErrAppendOffset)
	}
	return err
}

// PartSize looks at relPath's part on the server (model.PartWriter).
func (b *SCPBackend) PartSize(ctx context.Context, relPath string) (int64, error) {
	p := shellQuote(b.abs(model.PartPath(relPath)))
	out, err := b.run(ctx, fmt.Sprintf("if [ -L %[1]s ]; then echo link; elif [ -f %[1]s ]; then wc -c < %[1]s; elif [ -e %[1]s ]; then echo other; else echo 0; fi", p))
	if err != nil {
		return 0, err
	}
	switch out = strings.TrimSpace(out); out {
	case "link", "other":
		return 0, fmt.Errorf("scp: %s is a %s, not a file: %w", model.PartPath(relPath), map[string]string{"link": "link", "other": "folder or special file"}[out], model.ErrPartInTheWay)
	}
	n, err := strconv.ParseInt(out, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("scp: size of %s: %q", model.PartPath(relPath), out)
	}
	return n, nil
}

// CommitPart moves relPath's part over relPath in one shell command
// (model.PartWriter). dd syncs the part where dd knows conv=fsync (GNU,
// busybox; BSD's refuses it and the part is moved unsynced). mv is
// rename(2), which replaces a file or a link in one step, but mv onto a
// link to a folder would move the part into that folder: the link goes
// first. A folder is never replaced.
func (b *SCPBackend) CommitPart(ctx context.Context, relPath string, mode os.FileMode) error {
	_, err := b.run(ctx, commitCmd(b.abs(model.PartPath(relPath)), b.abs(relPath), mode))
	b.listCache.invalidateAncestors(relPath)
	switch exitCode(err) {
	case exitFolder:
		err = fmt.Errorf("scp: %s is a folder: %w", relPath, fs.ErrExist)
	case exitNoPart:
		err = fmt.Errorf("scp: the part of %s is gone", relPath)
	}
	return err
}

func commitCmd(part, final string, mode os.FileMode) string {
	chmod := ""
	if model.PartMode(mode) != mode.Perm() {
		chmod = fmt.Sprintf("chmod %04o %s || exit 1; ", mode.Perm(), shellQuote(part))
	}
	return fmt.Sprintf("[ -f %[1]s ] && [ ! -L %[1]s ] || exit %[3]d; dd if=/dev/null of=%[1]s conv=notrunc,fsync 2>/dev/null; %[5]s"+
		"if [ -L %[2]s ]; then rm -f %[2]s || exit 1; elif [ -d %[2]s ]; then exit %[4]d; fi; mv -f %[1]s %[2]s",
		shellQuote(part), shellQuote(final), exitNoPart, exitFolder, chmod)
}

func (b *SCPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	client, release := b.pool.acquire(ctx)
	return b.stream(ctx, client, "cat "+shellQuote(b.abs(relPath)), release)
}

func (b *SCPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	client, release := b.pool.acquire(ctx)
	return b.stream(ctx, client, b.tailCmd(relPath, offset), release)
}
