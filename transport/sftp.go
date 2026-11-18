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
	"sync/atomic"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

// sftpFastOpts returns the throughput tuning applied to every sftp.Client
// the backend opens. The pkg/sftp defaults are pessimistic: maxPacket=32KB
// and writes are synchronous (each Write blocks for STATUS), so per-stream
// throughput collapses to roughly maxPacket/RTT. UseConcurrentWrites lets
// a file's writes pipeline up to maxConcurrentRequests packets at once;
// put asks for that (writeAll), as pkg/sftp's ReadFrom does it only for a
// reader that tells its size. The packet size stays at the 32KB
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

// sftpConn is one ssh connection and the sftp session on it.
type sftpConn struct {
	sftp *sftp.Client
	ssh  *ssh.Client
	cut  atomic.Bool // watch closed it, to stop a call the server did not answer
}

// close ends c. The ssh connection goes first: pkg/sftp's Close waits until
// the server ends the session, which a server that hangs never does (S11).
func (c *sftpConn) close() error {
	var err error
	if c.ssh != nil {
		err = c.ssh.Close()
	}
	if c.sftp != nil {
		c.sftp.Close()
	}
	if c.cut.Load() {
		return nil
	}
	return err
}

// sftpGrace is how long a call has, after its ctx ends, to end by itself
// before watch closes its connection.
const sftpGrace = time.Second

// watch closes c when ctx ends and the call on c has not ended a moment
// later (S11). pkg/sftp takes no ctx: a call waits for the server's answer
// as long as that takes, forever on a server whose file system hangs while
// sshd stays up, and a file's Close waits behind the transfer it would stop.
// Closing the ssh connection ends every call on c at once. The moment keeps
// c when the call ends by itself, as one whose source stopped does. The call
// ends with stop, which reports whether c was closed.
func (c *sftpConn) watch(ctx context.Context) (stop func() bool) {
	ended, decided := make(chan struct{}), make(chan struct{})
	var cut bool
	after := context.AfterFunc(ctx, func() {
		defer close(decided)
		t := time.NewTimer(sftpGrace)
		defer t.Stop()
		select {
		case <-ended:
		case <-t.C:
			cut = true
			c.cut.Store(true)
			c.ssh.Close()
		}
	})
	return func() bool {
		close(ended)
		if after() {
			return false
		}
		<-decided
		return cut
	}
}

// stopped is the error of a call whose connection watch closed.
func stopped(ctx context.Context) error {
	return fmt.Errorf("%w: the server did not answer, so its connection was closed", ctx.Err())
}

// newSFTPConn starts an sftp session on client, which it closes when ctx
// ends first (see watch).
func newSFTPConn(ctx context.Context, client *ssh.Client) (*sftpConn, error) {
	c := &sftpConn{ssh: client}
	stop := c.watch(ctx)
	sc, err := sftp.NewClient(client, sftpFastOpts()...)
	if stop() {
		if err == nil {
			sc.Close()
		}
		return nil, fmt.Errorf("sftp: %w", stopped(ctx))
	}
	if err != nil {
		return nil, fmt.Errorf("sftp: %w", err)
	}
	c.sftp = sc
	return c, nil
}

type SFTPBackend struct {
	sshShell
	main  *sftpConn // calls on files and folders; transfers too without extras
	pool  *connPool[*sftpConn]
	shell atomic.Int32 // whether the login shell runs commands: shellUnknown until asked
}

// NewSFTPBackend connects to rawURL; a cancel of ctx stops the connect.
func NewSFTPBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*SFTPBackend, error) {
	conn, err := dialSSH(ctx, rawURL, insecure)
	if err != nil {
		return nil, err
	}
	b, err := newSFTPBackend(ctx, conn, rawURL, insecure, parallel)
	if err != nil {
		conn.client.Close()
	}
	return b, err
}

// newSFTPBackend builds an SFTPBackend over an already-dialed sshConn; on
// error the caller closes conn.client. rawURL lets the pool dial extra
// connections for parallel transfers (parallel-1 of them). A ctx that ends
// first stops the start.
func newSFTPBackend(ctx context.Context, conn *sshConn, rawURL string, insecure bool, parallel int) (*SFTPBackend, error) {
	main, err := newSFTPConn(ctx, conn.client)
	if err != nil {
		return nil, err
	}
	b := &SFTPBackend{sshShell: sshShell{client: conn.client, gone: conn.gone, proto: "sftp", listCache: newListCache()}, main: main}
	if err := b.onMain(ctx, func(c *sftp.Client) (err error) {
		b.base, err = expandHome(conn.basePath, c.Getwd)
		return err
	}); err != nil {
		main.close()
		return nil, fmt.Errorf("sftp: %w", err)
	}
	b.display = sshDisplayURL(conn, b.base)
	dial := func(ctx context.Context) (*sftpConn, error) {
		c, err := dialSSH(ctx, rawURL, insecure)
		if err != nil {
			return nil, err
		}
		sc, err := newSFTPConn(ctx, c.client)
		if err != nil {
			c.client.Close()
			return nil, err
		}
		Log.Add("sftp", DirIn, "extra connection dialed")
		return sc, nil
	}
	b.pool = newConnPool("sftp", main, parallel-1, dial, func(c *sftpConn) { c.close() })
	return b, nil
}

// SetParallel keeps n-1 extra connections, the primary being the n-th.
func (b *SFTPBackend) SetParallel(n int) { b.pool.resize(n - 1) }

func (b *SFTPBackend) Close() error {
	b.pool.close()
	return b.main.close()
}

// connCut reports that watch closed the main connection: the backend is of
// no more use, and the reconnecting wrapper opens a new one.
func (b *SFTPBackend) connCut() bool { return b.main.cut.Load() }

// onMain runs op on the main connection, which watch closes when ctx ends
// first. An op that ended anyway keeps its result; either way the backend
// is of no more use then (connCut).
func (b *SFTPBackend) onMain(ctx context.Context, op func(*sftp.Client) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := b.main.watch(ctx)
	err := op(b.main.sftp)
	if stop() && err != nil {
		return stopped(ctx)
	}
	return err
}

// onPooled runs op on a pooled connection, which watch closes when ctx ends
// first.
func (b *SFTPBackend) onPooled(ctx context.Context, op func(*sftp.Client) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	conn, release := b.pool.acquire(ctx)
	if err := ctx.Err(); err != nil {
		release()
		return err
	}
	stop := conn.watch(ctx)
	err := op(conn.sftp)
	cut := stop()
	b.giveBack(conn, release, cut)
	if cut && err != nil {
		return stopped(ctx)
	}
	return err
}

// giveBack returns conn to the pool once its call ended. An extra
// connection that watch closed leaves the pool: the next call gets a new
// one.
func (b *SFTPBackend) giveBack(conn *sftpConn, release func(), cut bool) {
	if cut && conn != b.main {
		b.pool.discard(conn)
		return
	}
	release()
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

// takeListCache has b go on with c, the list cache of the backend of the
// connection before b's, and with the recursive listing it cut off (S18).
func (b *SFTPBackend) takeListCache(c *listCache) {
	b.listCache = c
	c.resume(b.proto, b.findRecursive)
}

func (b *SFTPBackend) readDir(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := b.abs(relDir)
	Log.Add("sftp", DirOut, "READDIR "+dir)
	var entries []os.FileInfo
	err := b.onMain(ctx, func(c *sftp.Client) (err error) {
		entries, err = c.ReadDir(dir)
		return err
	})
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
func (b *SFTPBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	var info os.FileInfo
	err := b.onMain(ctx, func(c *sftp.Client) (err error) {
		info, err = c.Lstat(b.abs(relPath))
		return err
	})
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
		TimeRes: model.TimeSecond, // SFTP v3 sends whole seconds
		IsDir:   info.IsDir(),
		Mode:    info.Mode(),
	}
	if relPath == "" {
		e.Name = ""
	}
	if stat, ok := info.Sys().(*sftp.FileStat); ok && stat != nil {
		e.ATime = model.NanoOf(time.Unix(int64(stat.Atime), 0))
	}
	return e
}

func (b *SFTPBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, _ time.Time) error {
	if atime.IsZero() {
		atime = mtime
	}
	err := b.onMain(ctx, func(c *sftp.Client) error { return c.Chtimes(b.abs(relPath), atime, mtime) })
	b.listCache.invalidate(parentDir(relPath))
	if err != nil {
		Log.Add("sftp", DirErr, "CHTIMES "+relPath+": "+err.Error())
	}
	return err
}

// put writes relPath on a pooled connection and then applies mode: a new
// body when offset < 0, else the rest of the file after its offset bytes.
// size is the whole file's, 0 or less when not known. A failed write is
// cut back to the first byte that may be missing (S21, writeAll). A mode
// the server will not take is ErrModeNotSet, the bytes in place.
// An append never creates the file and refuses one of another size (L14,
// S8): writing past the end of a shorter file leaves a gap of zeros. The
// source opens once the file is ready. The data is synced when the server
// can, and Close is checked: pkg/sftp reports the final flush status there,
// so ignoring it would report a short file as copied. A cancel closes the
// connection when the server does not answer (onPooled): closing the file
// would wait behind the transfer.
func (b *SFTPBackend) put(ctx context.Context, relPath string, offset, size int64, mode os.FileMode, open func() (io.ReadCloser, error)) error {
	fullPath := b.abs(relPath)
	return b.onPooled(ctx, func(c *sftp.Client) error {
		flags := os.O_WRONLY
		if offset < 0 {
			if err := c.MkdirAll(path.Dir(fullPath)); err != nil {
				return err
			}
			flags |= os.O_CREATE | os.O_TRUNC
		}
		f, err := c.OpenFile(fullPath, flags)
		if offset >= 0 && errors.Is(err, fs.ErrNotExist) {
			return appendOffsetErr("sftp", relPath, -1, offset)
		}
		if err != nil {
			return err
		}
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
		cut := int64(-1)
		if err == nil {
			cut, err = writeAll(f, src, size-max(offset, 0))
			src.Close()
		}
		if err == nil {
			err = syncSFTP(f)
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if cut >= 0 && !isConnLost(err) {
			// After the close: a server that runs a file's writes side
			// by side, as pkg/sftp's, finishes them before it closes the
			// file, not before a SETSTAT, and a write still in flight
			// would land past the cut.
			if terr := c.Truncate(fullPath, cut); terr != nil {
				err = fmt.Errorf("%w; cutting it at %d: %v", err, cut, terr)
			}
		}
		if err == nil {
			// The bytes are in place: a mode the server will not take
			// is no reason to send them again (S20).
			if cerr := c.Chmod(fullPath, mode); cerr != nil {
				err = fmt.Errorf("sftp: chmod %04o %s: %w: %w", mode.Perm(), relPath, model.ErrModeNotSet, cerr)
			}
		}
		return err
	})
}

// sftpPacket is the size of one WRITE: pkg/sftp's default packet, which
// sc keeps (sftpFastOpts).
const sftpPacket = 32 << 10

// writeAll writes src to f from f's offset on. pkg/sftp keeps many WRITEs
// in flight only for a reader that tells its size, and the copy's readers
// tell none: an upload sent one packet and waited for its answer, a round
// trip for each 32 KiB (S21). writeAll asks for as many as pkg/sftp would
// for a reader of remain bytes, what src is said to hold (0 or less when
// not known: one at a time). With many in flight, a failed write can have
// later ones land past it, which leaves zeros below the file's size.
// pkg/sftp leaves f's offset at the first byte that may be missing, all
// before it written, and writeAll returns that offset as where to cut f;
// -1 when nothing can lie past it.
func writeAll(f *sftp.File, src io.Reader, remain int64) (cut int64, err error) {
	start, _ := f.Seek(0, io.SeekCurrent)
	var read int64
	if remain > sftpPacket {
		// pkg/sftp caps it at its own limit, 64 by default.
		read, err = f.ReadFromWithConcurrency(src, int(min(remain/sftpPacket+1, 64)))
	} else {
		read, err = f.ReadFrom(src)
	}
	if err == nil {
		return -1, nil
	}
	if at, _ := f.Seek(0, io.SeekCurrent); at < start+read {
		return at, err
	}
	return -1, err
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
	err := b.put(ctx, relPath, -1, p.Size, p.Mode, func() (io.ReadCloser, error) { return io.NopCloser(src), nil })
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
	err := b.put(ctx, relPath, offset, p.Size, p.Mode, func() (io.ReadCloser, error) { return src.OpenAt(ctx, offset) })
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
	}
	return err
}

// PartSize stats relPath's part (model.PartWriter).
func (b *SFTPBackend) PartSize(ctx context.Context, relPath string) (int64, error) {
	p := b.abs(model.PartPath(relPath))
	var fi os.FileInfo
	err := b.onMain(ctx, func(c *sftp.Client) (err error) {
		fi, err = c.Lstat(p)
		return err
	})
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
func (b *SFTPBackend) CommitPart(ctx context.Context, relPath string, mode os.FileMode) error {
	Log.Add("sftp", DirOut, "COMMIT "+relPath)
	err := b.onMain(ctx, func(c *sftp.Client) error { return b.commitPart(c, model.PartPath(relPath), relPath, mode) })
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, "COMMIT "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) commitPart(c *sftp.Client, partRel, relPath string, mode os.FileMode) error {
	part, final := b.abs(partRel), b.abs(relPath)
	// A mode the server will not take does not keep the part from its name
	// (S20).
	var modeErr error
	if model.PartMode(mode) != mode.Perm() {
		if err := c.Chmod(part, mode); err != nil {
			modeErr = fmt.Errorf("sftp: chmod %04o %s: %w: %w", mode.Perm(), relPath, model.ErrModeNotSet, err)
		}
	}
	if err := renamePart(c, part, final); err != nil {
		return err
	}
	return modeErr
}

// renamePart renames part over final: a file or a link there is replaced,
// a folder is not.
func renamePart(c *sftp.Client, part, final string) error {
	folder := func() error {
		if fi, err := c.Lstat(final); err == nil && fi.IsDir() {
			return fmt.Errorf("sftp: %s is a folder: %w", final, fs.ErrExist)
		}
		return nil
	}
	if _, ok := c.HasExtension("posix-rename@openssh.com"); ok {
		err := c.PosixRename(part, final)
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
	if err := c.Remove(final); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return c.Rename(part, final)
}

func (b *SFTPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.openAt(ctx, relPath, 0)
}

func (b *SFTPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	return b.openAt(ctx, relPath, offset)
}

// openAt opens relPath on a pooled connection, which watch closes when ctx
// ends before the reader is closed and a read waits for the server (S11).
func (b *SFTPBackend) openAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, release := b.pool.acquire(ctx)
	stop := conn.watch(ctx)
	rc, err := conn.sftp.Open(b.abs(relPath))
	if err == nil && offset > 0 {
		if _, err = rc.Seek(offset, io.SeekStart); err != nil {
			rc.Close()
		}
	}
	if err != nil {
		cut := stop()
		b.giveBack(conn, release, cut)
		if cut {
			err = stopped(ctx)
		}
		Log.Add("sftp", DirErr, "OPEN "+relPath+": "+err.Error())
		return nil, err
	}
	return &sftpReader{ReadCloser: rc, ctx: ctx, done: func() { b.giveBack(conn, release, stop()) }}, nil
}

func (b *SFTPBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	fullPath := b.abs(relPath)
	Log.Add("sftp", DirOut, "MKDIR "+relPath)
	err := b.onMain(ctx, func(c *sftp.Client) error {
		if err := c.MkdirAll(fullPath); err != nil {
			return err
		}
		if mode != 0 {
			// The folder is made: a mode the server will not take is
			// no failure (S20).
			if err := c.Chmod(fullPath, mode.Perm()); err != nil {
				Log.Add("sftp", DirWarn, fmt.Sprintf("MKDIR %s: chmod %04o: %v", relPath, mode.Perm(), err))
			}
		}
		return nil
	})
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, err.Error())
		return err
	}
	return nil
}

// Rename refuses an existing target. It then prefers
// posix-rename@openssh.com, which is rename(2); plain SSH_FXP_RENAME needs
// hard links on OpenSSH. A case-only rename skips the check: on a
// case-insensitive server the target is the file itself.
func (b *SFTPBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	if err := renameBase("sftp", oldRelPath, newRelPath); err != nil {
		return err
	}
	taken := false
	err := b.onMain(ctx, func(c *sftp.Client) error {
		if !strings.EqualFold(oldRelPath, newRelPath) {
			if _, err := c.Lstat(b.abs(newRelPath)); err == nil {
				taken = true
				return renameTargetTaken("sftp", newRelPath)
			}
		}
		rename := c.Rename
		if _, ok := c.HasExtension("posix-rename@openssh.com"); ok {
			rename = c.PosixRename
		}
		return rename(b.abs(oldRelPath), b.abs(newRelPath))
	})
	if taken {
		return err
	}
	b.listCache.forgetRenamed(oldRelPath, newRelPath)
	if err != nil {
		Log.Add("sftp", DirErr, "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) Remove(ctx context.Context, relPath string) error {
	err := b.onMain(ctx, func(c *sftp.Client) error { return c.Remove(b.abs(relPath)) })
	b.listCache.invalidate(parentDir(relPath))
	if err != nil {
		Log.Add("sftp", DirErr, "REMOVE "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) RemoveAll(ctx context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return fmt.Errorf("sftp: refusing to remove the base directory")
	}
	// One rm -rf on the shell removes a folder with one round trip, where
	// the requests go one for each entry (S19). A server whose shell does
	// not run it gets the requests.
	err := errNoShell
	if b.shellRuns(ctx) {
		err = b.rmRF(ctx, relPath)
	}
	if errors.Is(err, errNoShell) {
		err = b.onMain(ctx, func(c *sftp.Client) error { return removeAll(c, b.abs(relPath)) })
	}
	b.listCache.forgetRemoved(relPath)
	if err != nil {
		Log.Add("sftp", DirErr, "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

// shellRuns says whether the login shell runs the commands it is sent,
// asking it once for the connection: an SFTP-only server runs the sftp
// server for every command, and that waits on an input held open for
// requests that never come. A check that was stopped is asked again.
func (b *SFTPBackend) shellRuns(ctx context.Context) bool {
	switch b.shell.Load() {
	case shellYes:
		return true
	case shellNo:
		return false
	}
	err := b.checkShell(ctx)
	switch {
	case ctx.Err() != nil:
		return false
	case err != nil:
		b.shell.Store(shellNo)
		Log.Add("sftp", DirWarn, "folders are removed by SFTP requests: "+err.Error())
		return false
	}
	b.shell.Store(shellYes)
	return true
}

// What SFTPBackend.shellRuns found.
const (
	shellUnknown int32 = iota
	shellYes
	shellNo
)

func removeAll(c *sftp.Client, fullPath string) error {
	// Lstat: a link is removed as a link. Stat followed it, and a folder
	// that became a link after the scan had the folder it points to
	// emptied, outside the base too, as os.RemoveAll and rm -rf never do.
	info, err := c.Lstat(fullPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return c.Remove(fullPath)
	}
	entries, err := c.ReadDir(fullPath)
	if err != nil {
		return err
	}
	// Each listed name is joined onto fullPath, so each must name a
	// child. The library drops "." and "..", but of a name with a slash it
	// keeps the last part, which may be "..": then nothing in fullPath is
	// removed (L7).
	for _, e := range entries {
		if !model.IsFileName(e.Name()) {
			return fmt.Errorf("sftp: %s lists %q, which is no file name: nothing in it is removed", fullPath, e.Name())
		}
	}
	for _, e := range entries {
		child := path.Join(fullPath, e.Name())
		if e.IsDir() {
			err = removeAll(c, child)
		} else {
			err = c.Remove(child)
		}
		if err != nil {
			return err
		}
	}
	return c.RemoveDirectory(fullPath)
}

// sftpReader reads a file on a pooled connection, which openAt's watch
// closes when ctx ends during a read the server does not answer. Close
// gives the connection back.
//
// It reads ahead of its caller (S17). pkg/sftp sends a read of more than
// one packet (32 KiB) as many requests at once, and a read of one packet
// as one request; callers read 32 KiB at a time, so a download waited a
// round trip for each 32 KiB: 756 KiB/s over a 40 ms link, against 21
// MiB/s for an upload. The reads ahead start at one packet, as before,
// and grow while they come back quickly (nextReadAhead).
type sftpReader struct {
	io.ReadCloser
	ctx  context.Context
	done func()
	once sync.Once
	buf  []byte // read ahead and not yet given: buf[off:]
	off  int
	size int   // the next read ahead
	err  error // what ended the reads ahead, given once buf is empty
}

// Read gives what was read ahead, and reads ahead again once all of it is
// given. A read that failed after ctx ended gives ctx's error: a closed
// connection reads as lost, and the backend the caller writes to would be
// dropped for it (markBrokenIf).
func (r *sftpReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.off == len(r.buf) && r.err == nil {
		r.fill()
	}
	if r.off == len(r.buf) {
		return 0, r.err
	}
	n := copy(p, r.buf[r.off:])
	r.off += n
	return n, nil
}

// fill reads ahead once: size bytes, or fewer where the file ends.
func (r *sftpReader) fill() {
	if r.size == 0 {
		r.size = sftpReadAheadMin
	}
	if cap(r.buf) < r.size {
		r.buf = make([]byte, r.size)
	}
	start := time.Now()
	n, err := r.ReadCloser.Read(r.buf[:r.size])
	r.buf, r.off = r.buf[:n], 0
	if err != nil {
		if err != io.EOF && r.ctx.Err() != nil {
			err = fmt.Errorf("sftp: read: %w", r.ctx.Err())
		}
		r.err = err
		return
	}
	r.size = nextReadAhead(r.size, n, time.Since(start))
}

const (
	sftpReadAheadMin = 32 << 10 // one packet: a slow link is no slower than before
	sftpReadAheadMax = 4 << 20
)

// nextReadAhead is the read ahead after one of size bytes that gave n in
// took: twice as much after a full one that came back within a quarter of
// a second, up to 4 MiB; half as much after one that took more than a
// second, so a slow link still shows progress well within the stall
// guard's minute; else the same.
func nextReadAhead(size, n int, took time.Duration) int {
	switch {
	case n == size && took < time.Second/4:
		return min(2*size, sftpReadAheadMax)
	case took > time.Second:
		return max(size/2, sftpReadAheadMin)
	}
	return size
}

func (r *sftpReader) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.done)
	return err
}
