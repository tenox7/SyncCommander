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
func newSCPBackend(ctx context.Context, conn *sshConn, rawURL string, insecure bool, parallel int) (*SCPBackend, error) {
	b := &SCPBackend{sshShell: sshShell{client: conn.client, gone: conn.gone, proto: "scp", listCache: newListCache()}}
	if err := b.checkShell(ctx); err != nil {
		return nil, fmt.Errorf("scp: %w", err)
	}
	base, err := expandHome(conn.basePath, func() (string, error) { return b.home(ctx) })
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
	b.pool = newConnPool("scp", conn.client, parallel-1, dial, func(c *ssh.Client) { c.Close() })
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

// takeListCache has b go on with c, the list cache of the backend of the
// connection before b's, and with the recursive listing it cut off (S18).
func (b *SCPBackend) takeListCache(c *listCache) {
	b.listCache = c
	c.resume(b.proto, b.findRecursive)
}

// Exit codes of the shell checks below, so the error says what went wrong
// rather than "exited with status 1".
const (
	exitNotAtOffset = 75 // the file to append to does not hold offset bytes
	exitFolder      = 76 // a folder is where the part goes
	exitNoPart      = 77 // the part to commit is gone
	exitModeNotSet  = 79 // the bytes or the name are in place, but chmod failed
)

// modeNotSet is err, the end of a write or a commit, as ErrModeNotSet when
// only its chmod failed (S20).
func modeNotSet(proto, relPath string, mode os.FileMode, err error) error {
	if exitCode(err) != exitModeNotSet {
		return err
	}
	return fmt.Errorf("%s: chmod %04o %s: %w: %w", proto, mode.Perm(), relPath, model.ErrModeNotSet, err)
}

// exitCode is the status cmd ended with, or -1 when it did not end with one.
func exitCode(err error) int {
	var ee *ssh.ExitError
	if errors.As(err, &ee) {
		return ee.ExitStatus()
	}
	return -1
}

// CopyFrom makes relPath's folder and writes the file in one command: the
// folder was made by a command of its own, one more round trip for each
// file, 14 hours for a million small files at 50 ms (S19).
func (b *SCPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	fp := shellQuote(b.abs(relPath))
	client, release := b.pool.acquire(ctx)
	defer release()
	// The bytes are in place once cat ends: a mode the server will not
	// take is no reason to send them again (S20).
	err := b.exec(ctx, client, fmt.Sprintf("{ mkdir -p %s && cat > %s; } || exit; chmod %04o %s || exit %d",
		shellQuote(path.Dir(b.abs(relPath))), fp, p.Mode.Perm(), fp, exitModeNotSet), src, nil)
	b.listCache.invalidateAncestors(relPath)
	return modeNotSet("scp", relPath, p.Mode, err)
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
	cmd := fmt.Sprintf("n=$(wc -c 2>/dev/null < %[1]s) && [ ! -L %[1]s ] && [ $n -eq %[2]d ] || exit %[3]d; cat >> %[1]s || exit; chmod %04[4]o %[1]s || exit %[5]d",
		fp, offset, exitNotAtOffset, p.Mode.Perm(), exitModeNotSet)
	err = b.exec(ctx, client, cmd, rd, nil)
	b.listCache.invalidateAncestors(relPath)
	if exitCode(err) == exitNotAtOffset {
		err = fmt.Errorf("scp: %s is not a file of %d bytes to append to: %w", relPath, offset, ErrAppendOffset)
	}
	return modeNotSet("scp", relPath, p.Mode, err)
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
	return modeNotSet("scp", relPath, mode, err)
}

func commitCmd(part, final string, mode os.FileMode) string {
	// A mode the server will not take does not keep the part from its
	// name (S20): the rename goes on, and the exit status says so.
	chmod := "m=0; "
	if model.PartMode(mode) != mode.Perm() {
		chmod = fmt.Sprintf("m=0; chmod %04o %s || m=%d; ", mode.Perm(), shellQuote(part), exitModeNotSet)
	}
	return fmt.Sprintf("[ -f %[1]s ] && [ ! -L %[1]s ] || exit %[3]d; dd if=/dev/null of=%[1]s conv=notrunc,fsync 2>/dev/null; %[5]s"+
		"if [ -L %[2]s ]; then rm -f %[2]s || exit 1; elif [ -d %[2]s ]; then exit %[4]d; fi; mv -f %[1]s %[2]s || exit; exit $m",
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
