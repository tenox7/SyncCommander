package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

type RsyncSSHBackend struct {
	sshShell
	pool *connPool[*ssh.Client]
}

// NewRsyncSSHBackend dials rawURL; a cancel of ctx stops the connect. Up to
// parallel-1 extra connections are dialed lazily so parallel transfers do
// not share one TCP stream.
func NewRsyncSSHBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*RsyncSSHBackend, error) {
	conn, err := dialSSH(ctx, rawURL, insecure)
	if err != nil {
		return nil, err
	}
	b := &RsyncSSHBackend{sshShell: sshShell{client: conn.client, gone: conn.gone, proto: "rsync+ssh", listCache: newListCache()}}
	if err := b.checkShell(ctx); err != nil {
		conn.client.Close()
		return nil, fmt.Errorf("rsync+ssh: %w", err)
	}
	if b.base, err = expandHome(conn.basePath, func() (string, error) { return b.home(ctx) }); err != nil {
		conn.client.Close()
		return nil, fmt.Errorf("rsync+ssh: %w", err)
	}
	if err := b.findRsync(ctx); err != nil {
		conn.client.Close()
		return nil, fmt.Errorf("rsync+ssh: %w", err)
	}
	b.display = sshDisplayURL(conn, b.base)
	dial := func(ctx context.Context) (*ssh.Client, error) {
		c, err := dialSSH(ctx, rawURL, insecure)
		if err != nil {
			return nil, err
		}
		Log.Add(b.proto, DirIn, "extra connection dialed")
		return c.client, nil
	}
	b.pool = newConnPool(b.proto, conn.client, parallel-1, dial, func(c *ssh.Client) { c.Close() })
	b.pool.alive = sshClientAlive
	return b, nil
}

// findRsync asks the login shell for rsync. It had no context and no
// deadline: a PATH on a hung mount hung the connect, and a cancel did not
// stop it (R17).
func (b *RsyncSSHBackend) findRsync(ctx context.Context) error {
	ctx, cancel := shellCheckCtx(ctx)
	defer cancel()
	_, err := b.run(ctx, "command -v rsync")
	if ctx.Err() != nil {
		return fmt.Errorf("command -v rsync: %w", shellCheckErr(ctx, err))
	}
	if err != nil {
		return errors.New("remote rsync not found")
	}
	return nil
}

// SetParallel keeps n-1 extra connections, the primary being the n-th.
func (b *RsyncSSHBackend) SetParallel(n int) { b.pool.resize(n - 1) }

func (b *RsyncSSHBackend) Close() error {
	b.pool.close()
	return b.client.Close()
}

func (b *RsyncSSHBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.findList)
}

func (b *RsyncSSHBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

// takeListCache has b go on with c, the list cache of the backend of the
// connection before b's, and with the recursive listing it cut off (S18).
func (b *RsyncSSHBackend) takeListCache(c *listCache) {
	b.listCache = c
	c.resume(b.proto, b.runRecursiveList)
}

func (b *RsyncSSHBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
	// rsync quotes paths in its messages as absolute paths.
	l := newRsyncListing(scope, emit, complete, func(p string) (string, bool) { return relUnder(b.base, p) })
	client, err := newRsyncClient([]string{"-n", "-r"}, rsyncclient.WithFileListCallback(l.add), rsyncclient.WithStderr(l))
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "rsync-rlist-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	remote := b.abs(scope) + "/"
	result, err := b.runRsync(ctx, "RLIST", b.client, client, remote, []string{tmpDir + "/"}, nil, false)
	if err != nil {
		return err
	}
	if err := l.finish("RLIST "+remote, result); err != nil {
		Log.Add(b.proto, DirWarn, err.Error())
		return err
	}
	Log.Add(b.proto, DirIn, fmt.Sprintf("RLIST %d entries", len(result.FileList)))
	return nil
}

// runPooled is runRsync on a pooled connection, for data transfers: a run
// rsync would end with exit code 23 or 24 fails (transferErr), and so does
// any non-zero exit status of the remote rsync.
func (b *RsyncSSHBackend) runPooled(ctx context.Context, label string, client *rsyncclient.Client, remote string, local []string, wrap func(io.ReadWriteCloser) io.ReadWriteCloser) (*rsyncclient.Result, error) {
	conn, release := b.pool.acquire(ctx)
	defer release()
	return b.runRsync(ctx, label, conn, client, remote, local, wrap, true)
}

// remoteExitWait bounds the wait for the remote rsync to exit once the
// protocol has ended.
const remoteExitWait = 30 * time.Second

// runRsync starts the remote rsync server for remote over a fresh session on
// conn and drives client against it; wrap may decorate the stream to credit
// progress. A transfer also checks the error counts and the remote exit
// status; a listing judges the counts itself (rsyncListing.finish).
func (b *RsyncSSHBackend) runRsync(ctx context.Context, label string, conn *ssh.Client, client *rsyncclient.Client, remote string, local []string, wrap func(io.ReadWriteCloser) io.ReadWriteCloser, transfer bool) (*rsyncclient.Result, error) {
	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remote))
	Log.Add(b.proto, DirOut, label+" "+serverCmd)
	session, err := conn.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	defer CancelCloser(ctx, session)()
	stdin, err := session.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	session.Stderr = &stderr
	if err := session.Start(serverCmd); err != nil {
		Log.Add(b.proto, DirErr, err.Error())
		return nil, err
	}
	var rw io.ReadWriteCloser = &sessionRW{r: stdout, w: stdin}
	if wrap != nil {
		rw = wrap(rw)
	}
	result, err := client.Run(ctx, rw, local)
	if err == nil && transfer {
		err = transferErr(result.XferErrors, result.IOErrors)
		if err == nil {
			stdin.Close()
			err = waitRemote(ctx, session, &stderr)
		}
	}
	if err != nil {
		Log.Add(b.proto, DirErr, err.Error())
	}
	return result, err
}

// waitRemote waits for the remote rsync to exit and fails unless it exits
// with status 0: rsync counts errors there that need not reach the client.
// stderr is read only once the session is done with it.
func waitRemote(ctx context.Context, session *ssh.Session, stderr *bytes.Buffer) error {
	done := make(chan error, 1)
	go func() { done <- session.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			return nil
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			err = fmt.Errorf("%w: %s", err, msg)
		}
		return fmt.Errorf("rsync: the remote rsync: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(remoteExitWait):
		return errors.New("rsync: the remote rsync did not exit after the transfer")
	}
}

func (b *RsyncSSHBackend) buildServerCmd(args []string) string {
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellQuote(a)
	}
	return "rsync " + strings.Join(quoted, " ")
}

// sessionRW is the rsync wire over one SSH session; Close ends the server's
// stdin, the session itself is closed by runRsync.
type sessionRW struct {
	r io.Reader
	w io.WriteCloser
}

func (s *sessionRW) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *sessionRW) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *sessionRW) Close() error                { return s.w.Close() }

// Checksum takes md4 from rsync's file list, other algorithms from the
// shell's commands.
func (b *RsyncSSHBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	if algo == "md4" {
		return md4Of(ctx, relPath, b.fetchMD4)
	}
	return b.sshShell.Checksum(ctx, relPath, algo)
}

func (b *RsyncSSHBackend) ChecksumAlgos(ctx context.Context) ([]string, error) {
	algos, err := b.sshShell.ChecksumAlgos(ctx)
	if err != nil {
		return nil, err
	}
	return append(algos, "md4"), nil
}

// PrefetchChecksums fetches md4 for scope in one rsync run; the shell's
// algorithms are summed in batches (ChecksumFiles, S19).
func (b *RsyncSSHBackend) PrefetchChecksums(ctx context.Context, scope, algo string, recursive bool) (map[string]string, error) {
	if algo != "md4" {
		return nil, nil
	}
	return b.fetchMD4(ctx, scope, recursive)
}

func (b *RsyncSSHBackend) fetchMD4(ctx context.Context, scope string, recursive bool) (map[string]string, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-md4-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	client, err := newRsyncClient(md4ListFlags(recursive))
	if err != nil {
		return nil, err
	}
	remote := b.abs(scope)
	if recursive {
		remote += "/"
	}
	result, err := b.runRsync(ctx, "MD4", b.client, client, remote, []string{tmpDir + "/"}, nil, false)
	if err != nil {
		return nil, err
	}
	got := md4Sums(scope, recursive, result.FileList)
	Log.Add(b.proto, DirIn, fmt.Sprintf("MD4 %d checksums", len(got)))
	return got, nil
}

func (b *RsyncSSHBackend) invalidateAfterTreeSend(relPath string) {
	b.listCache.invalidateTree(relPath)
	b.listCache.invalidateAncestors(relPath)
}

func (b *RsyncSSHBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return rsyncOpenViaTemp(ctx, relPath, 0, func(dstDir string) error {
		client, err := newRsyncClient(transferFlags())
		if err != nil {
			return err
		}
		_, err = b.runPooled(ctx, "RECV", client, b.abs(relPath), []string{dstDir}, nil)
		return err
	})
}

func (b *RsyncSSHBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	conn, release := b.pool.acquire(ctx)
	return b.stream(ctx, conn, b.tailCmd(relPath, offset), release)
}

// RecvToLocalFile downloads straight to dstPath, through rsync's temp file
// next to it, renamed over it at the end (see rsyncTransferFlags).
func (b *RsyncSSHBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	return rsyncRecvToLocal(ctx, b.proto, relPath, dstPath, func(dstDir string) error {
		client, err := newRsyncClient(transferFlags())
		if err != nil {
			return err
		}
		_, err = b.runPooled(ctx, "RECV "+relPath+" -> "+dstPath+" via", client, b.abs(relPath), []string{dstDir}, nil)
		return err
	})
}

// SendLocalTree uploads the listed paths under srcRoot/relPath, and the
// folders above them, in one rsync session: nothing else of the folder
// (F20). -I (--ignore-times) sends every listed file, as the caller listed
// it because it differs. -c stood in for it: both ends read each listed
// file before a byte moved, and a listed file that matched was neither
// sent nor counted (R13). The sender names each file it begins
// (WithSendCallback), which feeds onFile. Its --progress output fed it,
// and a file sent within a second has a progress line with no \r, which
// was taken for a name (R13). Each file goes through a temp file renamed
// into place, as rsyncTransferFlags says.
func (b *RsyncSSHBackend) SendLocalTree(ctx context.Context, srcRoot, relPath string, files []string, onFile func(name string)) error {
	names, err := namesUnder(relPath, files)
	if err != nil || len(names) == 0 {
		// Never a run without a list: the client then sends everything.
		return err
	}
	srcPath := filepath.Join(srcRoot, relPath) + string(filepath.Separator)
	remoteDest := b.abs(relPath) + "/"
	if _, err := b.run(ctx, "mkdir -p "+shellQuote(remoteDest)); err != nil {
		return err
	}
	opts := []rsyncclient.Option{rsyncclient.WithSender(), rsyncclient.WithSendOnly(names)}
	if onFile != nil {
		opts = append(opts, rsyncclient.WithSendCallback(onFile))
	}
	client, err := newRsyncClient([]string{"-t", "-O", "-W", "-r", "-I"}, opts...)
	if err != nil {
		return err
	}
	var wrap func(io.ReadWriteCloser) io.ReadWriteCloser
	if counter := progressFromContext(ctx); counter != nil {
		wrap = countingRW(ctx, NewCappedAdder(counter, 1<<62))
	}
	_, err = b.runPooled(ctx, "BATCH "+srcPath+" via", client, remoteDest, []string{srcPath}, wrap)
	b.invalidateAfterTreeSend(relPath)
	if err == nil {
		Log.Add(b.proto, DirIn, "BATCH OK")
	}
	return err
}

// namesUnder names files as a send of relPath's contents lists them:
// relative to relPath. A path that is not under relPath is refused.
func namesUnder(relPath string, files []string) ([]string, error) {
	names := make([]string, 0, len(files))
	for _, f := range files {
		name, ok := f, true
		if relPath != "" {
			name, ok = strings.CutPrefix(f, relPath+"/")
		}
		for part := range strings.SplitSeq(name, "/") {
			ok = ok && model.IsFileName(part) // no "", ".", ".." or empty part
		}
		if !ok {
			return nil, fmt.Errorf("batch send %s: %q is not in it", relPath, f)
		}
		names = append(names, name)
	}
	return names, nil
}

// sendFile pushes one local file into relPath's directory, which mkdir -p
// makes first unless a Mkdir or a send made or went into it (sawDir). prep
// runs after the mkdir in the same round trip; the push credits adder as
// bytes flow and is settled against budget afterwards. A folder noted may
// have been removed from elsewhere since: a plain send into one that fails
// where the folder is gone makes it and sends again, once. No later try
// would: "no such file" fails a file for good.
func (b *RsyncSSHBackend) sendFile(ctx context.Context, label, srcPath, relPath, prep string, extraFlags []string, adder *CappedAdder, budget int64) error {
	dir := parentDir(relPath)
	remoteDest := b.abs(dir)
	known := b.knowsDir(dir)
	var cmds []string
	if !known {
		cmds = append(cmds, "mkdir -p "+shellQuote(remoteDest))
	}
	if prep != "" {
		cmds = append(cmds, prep)
	}
	if len(cmds) > 0 {
		if _, err := b.run(ctx, strings.Join(cmds, " && ")); err != nil {
			return err
		}
	}
	err := b.pushFile(ctx, label, srcPath, relPath, remoteDest, extraFlags, adder, budget)
	if err != nil && known && prep == "" && ctx.Err() == nil && b.remadeDir(ctx, dir) {
		if adder != nil { // the failed push took back what it credited
			adder = NewCappedAdder(adder.target, adder.cap)
		}
		err = b.pushFile(ctx, label, srcPath, relPath, remoteDest, extraFlags, adder, budget)
	}
	if err == nil {
		b.sawDir(dir)
	}
	return err
}

// pushFile is one rsync session that sends srcPath into remoteDest.
func (b *RsyncSSHBackend) pushFile(ctx context.Context, label, srcPath, relPath, remoteDest string, extraFlags []string, adder *CappedAdder, budget int64) error {
	client, err := newRsyncClient(append(transferFlags(), extraFlags...), rsyncclient.WithSender())
	if err != nil {
		return err
	}
	_, err = b.runPooled(ctx, label, client, remoteDest+"/", []string{srcPath}, countingRW(ctx, adder))
	b.listCache.invalidateAncestors(relPath)
	settlePush(ctx, adder, budget, err)
	return err
}

// remadeWord is the last line remadeDir's command prints when it made the
// folder; a login script may print lines ahead of it.
const remadeWord = "sc-made"

// remadeDir makes dir when it is not there, and reports whether it was
// gone.
func (b *RsyncSSHBackend) remadeDir(ctx context.Context, dir string) bool {
	d := shellQuote(b.abs(dir))
	out, err := b.run(ctx, "[ -d "+d+" ] || { mkdir -p "+d+" && echo "+remadeWord+"; }")
	if err != nil {
		return false
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	return lines[len(lines)-1] == remadeWord
}

// SendLocalFile sends an existing local file directly, no tmp copy. The push
// credits progress as data flows so the stall guard sees movement, and the
// final top-up covers whatever delta-sync did not send.
func (b *RsyncSSHBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, p model.PutParams) error {
	var adder *CappedAdder
	fileSize := max(p.Size, 0)
	if counter := progressFromContext(ctx); counter != nil && fileSize > 0 {
		adder = NewCappedAdder(counter, fileSize)
	}
	return b.sendFile(ctx, "SEND "+srcPath+" via", srcPath, relPath, "", nil, adder, fileSize)
}

// AppendFrom resumes a partial upload with rsync --append-verify: the remote
// inspects its dst size, only the missing tail is sent, and the remote
// checks the whole file against the sender's sum so a differing prefix fails
// the append. The dst is first sized to offset so a remote that shrank
// between the size probe and the upload cannot leave a hole. A source without
// a local path is spooled to a tmpfile.
func (b *RsyncSSHBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	srcPath := src.LocalPath()
	if srcPath == "" {
		tmpDir, err := os.MkdirTemp("", "rsync-ssh-append-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)
		srcPath = filepath.Join(tmpDir, filepath.Base(relPath))
		if err := spoolTo(ctx, src, srcPath, p); err != nil {
			return err
		}
	}
	tailSize := src.Size() - offset
	var adder *CappedAdder
	if counter := progressFromContext(ctx); counter != nil && tailSize > 0 {
		adder = NewCappedAdder(counter, tailSize)
	}
	label := fmt.Sprintf("APPEND %s @%d/%d via", relPath, offset, src.Size())
	ExpectSilence(ctx, appendVerifySilence(offset))
	return b.sendFile(ctx, label, srcPath, relPath, truncateCmd(b.abs(relPath), offset), []string{"--append-verify"}, adder, tailSize)
}

func (b *RsyncSSHBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	tmpDir, err := os.MkdirTemp("", "rsync-ssh-ul-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	tmpFile := filepath.Join(tmpDir, filepath.Base(relPath))
	pushAdder, pushBudget, err := stageUpload(ctx, src, tmpFile, p)
	if err != nil {
		return err
	}
	return b.sendFile(ctx, "SEND "+relPath+" via", tmpFile, relPath, "", nil, pushAdder, pushBudget)
}
