package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"

	"sc/model"
)

// RsyncBackend talks to an rsync daemon (rsync://). Listings and MD4 sums
// come from dry runs of the in-process client; transfers run through the
// rsync URL form so the daemon can recreate leading directories with -r.
type RsyncBackend struct {
	host        string
	user        string
	pass        string
	module      string
	base        string
	display     string
	useChecksum bool
	cksumAlgo   string
	md4         md4Cache
	listCache   *listCache
	sentMtime   sync.Map // mtime each upload sent with -t, see SetTimes
}

func NewRsyncBackend(rawURL string) (*RsyncBackend, error) {
	_, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if port == "" {
		port = "873"
	}
	module, base, _ := strings.Cut(strings.TrimPrefix(remotePath, "/"), "/")
	if module == "" {
		return nil, errors.New("rsync: module name required in URL")
	}
	base = strings.Trim(base, "/")
	if strings.ContainsAny(remotePath, "\r\n") {
		return nil, errors.New("rsync: a daemon takes no module or folder name with a line break")
	}

	displayHost := host
	if port != "873" {
		displayHost = net.JoinHostPort(host, port)
	}
	display := "rsync://" + displayHost + "/" + module
	if base != "" {
		display += "/" + base
	}
	return &RsyncBackend{
		host:      net.JoinHostPort(host, port),
		user:      user,
		pass:      pass,
		module:    module,
		base:      base,
		display:   display,
		listCache: newListCache(),
	}, nil
}

func (b *RsyncBackend) BasePath() string { return b.display }

func (b *RsyncBackend) OwnsCopyProgress() bool { return true }

// modulePath is the daemon-protocol path module/base/rel, with a trailing
// slash when the contents of a directory are wanted, written so that a
// stock daemon reads back this one path (see daemonPath).
func (b *RsyncBackend) modulePath(relPath string, dir bool) (string, error) {
	p := b.baseRel(relPath)
	arg, err := daemonPath(b.module, p)
	if err != nil {
		return "", err
	}
	full := b.module
	if arg != "" {
		full += "/" + arg
	}
	if dir {
		full += "/"
	}
	if arg != p && len(full) > maxDaemonPattern {
		return "", fmt.Errorf("rsync: %s: the name is too long to send to a daemon as a pattern (%d bytes, at most %d)", relPath, len(full), maxDaemonPattern)
	}
	return full, nil
}

// baseRel is relPath below the module: the base joined with relPath.
func (b *RsyncBackend) baseRel(relPath string) string {
	switch {
	case b.base == "":
		return relPath
	case relPath == "":
		return b.base
	}
	return b.base + "/" + relPath
}

// pushTarget is the daemon path of relDir for a push into it; the folder
// must be there. A path sent as a pattern (see daemonPath) that matches
// nothing is used as a plain name: the receiver creates a folder named with
// the backslashes and reports success. So for such a path the daemon is
// asked for the folder's own entry first (-d, no trailing slash: not its
// contents).
func (b *RsyncBackend) pushTarget(ctx context.Context, relDir string) (string, error) {
	remote, err := b.modulePath(relDir, false)
	if err != nil {
		return "", err
	}
	p := b.baseRel(relDir)
	if arg, _ := daemonPath(b.module, p); arg == p {
		return remote, nil
	}
	res, err := b.runDaemon(ctx, "STAT", []string{"-n", "-d"}, remote)
	if err != nil {
		return "", err
	}
	if len(res.FileList) != 1 || !rsyncModeToFileMode(res.FileList[0].Mode).IsDir() {
		return "", fmt.Errorf("rsync: %s: no such folder on the daemon", relDir)
	}
	return remote, nil
}

// maxDaemonPattern is the longest argument sc sends with a pattern in it. A
// daemon reads each argument into MAXPATHLEN+1024 bytes and drops the rest:
// 2047 bytes on BSD and macOS. A pattern cut short can match another name;
// a plain path that long cannot name anything there.
const maxDaemonPattern = 2047

// daemonPath writes p, a path below the module, so that a stock rsync daemon
// reads back this one path. The daemon splits each argument at
// " <module>/", and matches each part that holds *, ? or [ as a pattern
// against the names in its folder (rsync util1.c glob_expand_module and
// glob_match): "d[1]" lists d1 when that exists, and "old <module>/x" names
// two paths, "old" and "x". Such a part goes as a pattern that only its own
// name matches: a backslash before each of \ * ? [ ], and a space as [ ], so
// no " <module>/" is left to split at. The daemon takes other parts as they
// are, backslashes too. A line break ends the argument, so a name holding
// one is refused.
func daemonPath(module, p string) (string, error) {
	if strings.ContainsAny(p, "\r\n") {
		return "", fmt.Errorf("rsync: a daemon cannot be sent a name with a line break: %q", p)
	}
	parts := strings.Split(p, "/")
	for i, part := range parts {
		if strings.ContainsAny(part, "*?[") || strings.HasSuffix(part, " "+module) {
			parts[i] = daemonPattern(part)
		}
	}
	return strings.Join(parts, "/"), nil
}

// daemonPattern is the rsync wildcard pattern that matches name and nothing
// else, holding no space.
func daemonPattern(name string) string {
	var sb strings.Builder
	for i := 0; i < len(name); i++ {
		switch c := name[i]; c {
		case '\\', '*', '?', '[', ']':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case ' ':
			sb.WriteString("[ ]")
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}

// transfer runs one data-moving run between the daemon path remote and
// local: with send it pushes the local sources into remote, else it receives
// remote into the one local folder. stdout takes the --progress lines.
//
// It logs in with sc's own handshake, so the password only goes into the
// challenge response. It used to run gorsync's command line, with the
// password in its rsync:// URL, and gorsync writes that command line to
// stderr, which went into the error and the log. The error now carries the
// server's messages and nothing else from stderr. A run rsync would end
// with exit code 23 or 24 fails (transferErr).
func (b *RsyncBackend) transfer(ctx context.Context, flags []string, remote string, local []string, send bool, stdout io.Writer) error {
	peer := &peerMsgs{}
	opts := []rsyncclient.Option{rsyncclient.WithStderr(peer)}
	if send {
		opts = append(opts, rsyncclient.WithSender())
	}
	if stdout != nil {
		opts = append(opts, rsyncclient.WithStdout(stdout))
	}
	res, err := b.runDaemonPaths(ctx, flags, remote, local, opts...)
	if err == nil {
		err = transferErr(res.XferErrors, res.IOErrors)
	}
	if err == nil {
		return nil
	}
	msgs := peer.all()
	for _, m := range msgs {
		if strings.Contains(m, "module is read only") {
			Log.Add("rsync", DirFatal, "module is read only, aborting operation")
			TriggerFatalAbort(ctx)
			break
		}
	}
	if len(msgs) > 0 {
		err = fmt.Errorf("%w: %s", err, strings.Join(msgs[:min(len(msgs), 3)], "; "))
	}
	return err
}

// dialLimited is the dialer gorsync uses to reach an rsync:// daemon. gorsync
// opens that socket itself, so this hook is the only place the daemon's bytes
// can be metered; every other protocol is wrapped where sc dials.
// The Go resolver mirrors gorsync's own dialer: its restrict mode needs to
// know which files name resolution touches.
func dialLimited(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 30 * time.Second, Resolver: &net.Resolver{PreferGo: true}}
	c, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return LimitConn(c), nil
}

// runDaemon dials the daemon and runs client's dry run against remotePath,
// returning the file list. The handshake is sc's own (see daemonHandshake),
// so the credentials never touch process-wide state.
func (b *RsyncBackend) runDaemon(ctx context.Context, label string, flags []string, remotePath string, opts ...rsyncclient.Option) (*rsyncclient.Result, error) {
	dst, err := os.MkdirTemp("", "rsync-daemon-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dst)
	return b.runDaemonTo(ctx, label, flags, remotePath, dst+"/", opts...)
}

// runDaemonTo runs client against remotePath, receiving into local.
func (b *RsyncBackend) runDaemonTo(ctx context.Context, label string, flags []string, remotePath, local string, opts ...rsyncclient.Option) (*rsyncclient.Result, error) {
	Log.Add("rsync", DirOut, label+" "+remotePath)
	result, err := b.runDaemonPaths(ctx, flags, remotePath, []string{local}, opts...)
	if err != nil {
		Log.Add("rsync", DirErr, label+" "+remotePath+": "+err.Error())
	}
	return result, err
}

// runDaemonPaths dials the daemon, logs in with sc's own handshake (see
// daemonHandshake) and runs one client run between remotePath and the
// local paths.
func (b *RsyncBackend) runDaemonPaths(ctx context.Context, flags []string, remotePath string, local []string, opts ...rsyncclient.Option) (*rsyncclient.Result, error) {
	client, err := newRsyncClient(flags, append([]rsyncclient.Option{rsyncclient.WithoutNegotiate()}, opts...)...)
	if err != nil {
		return nil, err
	}
	conn, err := dialLimited(ctx, "tcp", b.host)
	if err != nil {
		return nil, err
	}
	// Bytes on the socket show the stall guard the transfer moves, also
	// when no progress is credited: a push of a source that counted its
	// own bytes (D5).
	conn = activeConn(ctx, conn)
	defer conn.Close()
	defer context.AfterFunc(ctx, func() { conn.Close() })()
	result := &rsyncclient.Result{}
	done, err := b.daemonHandshake(ctx, conn, client, remotePath)
	if err == nil && !done {
		result, err = client.Run(ctx, conn, local)
	}
	return result, err
}

// rsyncProgressWriter consumes `rsync --progress` output and credits the
// leading byte-offset of each progress line to adder. Updates are \r-separated
// (in-place line repaint) and the final line ends in \n; we split on either.
// Non-numeric lines (file names, banners, totals) are ignored.
type rsyncProgressWriter struct {
	adder *CappedAdder
	buf   []byte
	last  int64
}

func (w *rsyncProgressWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexAny(w.buf, "\r\n")
		if i < 0 {
			return len(p), nil
		}
		line := bytes.TrimSpace(w.buf[:i])
		w.buf = w.buf[i+1:]
		end := bytes.IndexByte(line, ' ')
		if end < 0 {
			continue
		}
		off, err := strconv.ParseInt(string(line[:end]), 10, 64)
		if err == nil && off > w.last {
			w.adder.Add(off - w.last)
			w.last = off
		}
	}
}

func (b *RsyncBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.liveList)
}

// liveList is a non-recursive dry run; -d makes the daemon send the
// directory's children, without it the sender skips a directory argument and
// hangs up after an empty list.
func (b *RsyncBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	remote, err := b.modulePath(relDir, true)
	if err != nil {
		return nil, err
	}
	label := "LIST " + remote
	peer := &peerMsgs{}
	result, err := b.runDaemon(ctx, "LIST", []string{"-n", "-d"}, remote, rsyncclient.WithStderr(peer))
	if err != nil {
		// A daemon that cannot open the folder says why, then hangs up:
		// its reason beats "connection reset".
		if probs := peer.problems(); len(probs) > 0 {
			err = fmt.Errorf("%w (%v)", incompleteListing(label, probs), err)
		}
		return nil, err
	}
	if err := listErr(label, result, peer); err != nil {
		Log.Add("rsync", DirErr, err.Error())
		return nil, err
	}
	entries := fileListEntries(relDir, result.FileList)
	Log.Add("rsync", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// Stat asks the daemon for relPath alone: a dry run of -d on the path, with
// no slash at its end, sends the path's own entry, a folder as a folder,
// and nothing of what is inside. -l and -D make stock rsync send a link or
// a pipe as what it is; without them it leaves both out, and a link would
// be "not there". gorsyncd before gorsync cf119b2 follows a link named
// this way and sends what it points to. A daemon says "not there" only in
// words: the link_stat message, ENOENT's text.
func (b *RsyncBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	remote, err := b.modulePath(relPath, relPath == "")
	if err != nil {
		return model.FileEntry{}, err
	}
	label := "STAT " + remote
	peer := &peerMsgs{}
	result, err := b.runDaemon(ctx, "STAT", []string{"-n", "-d", "-l", "-D"}, remote, rsyncclient.WithStderr(peer))
	if err != nil {
		if probs := peer.problems(); notThere(probs) {
			err = fmt.Errorf("%s: %w", label, fs.ErrNotExist)
		} else if len(probs) > 0 {
			err = fmt.Errorf("%s: %s (%w)", label, strings.Join(probs[:min(len(probs), 3)], "; "), err)
		}
		return model.FileEntry{}, err
	}
	return statEntry(label, relPath, result, peer)
}

// statEntry picks relPath's own entry out of a one-path dry run. The root
// is listed as its "." entry.
func statEntry(label, relPath string, result *rsyncclient.Result, peer *peerMsgs) (model.FileEntry, error) {
	want := path.Base(relPath)
	if relPath == "" {
		want = "."
	}
	for _, fi := range result.FileList {
		if fi.Name != want {
			continue
		}
		mode := rsyncModeToFileMode(fi.Mode)
		e := model.FileEntry{RelPath: relPath, Name: path.Base(relPath), Size: fi.Length, ModTime: fi.ModTime, IsDir: mode.IsDir(), Mode: mode}
		if relPath == "" {
			e.Name = ""
		}
		return e, nil
	}
	probs := peer.problems()
	if notThere(probs) {
		return model.FileEntry{}, fmt.Errorf("%s: %w", label, fs.ErrNotExist)
	}
	if len(probs) == 0 && result.IOErrors&ioErrGeneral == 0 && !peer.missed(result) {
		return model.FileEntry{}, fmt.Errorf("%s: the server sent no entry and no reason", label)
	}
	return model.FileEntry{}, incompleteListing(label, probs)
}

// notThere reports whether the messages of a one-path run say only that
// the path is missing: `link_stat "..." failed: No such file or directory
// (2)` from rsync, the same in Go's lower case from gorsyncd since gorsync
// cf119b2. An older gorsyncd says nothing and only sets io_error, as for a
// path it cannot read; that stays an error, never "not there".
func notThere(msgs []string) bool {
	for _, m := range msgs {
		if !strings.Contains(strings.ToLower(m), "no such file or directory") {
			return false
		}
	}
	return len(msgs) > 0
}

// PreloadRecursive fires one recursive dry run in the background and feeds
// the cache; List calls under scope then hit or wait on it.
func (b *RsyncBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

func (b *RsyncBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
	remote, err := b.modulePath(scope, true)
	if err != nil {
		return err
	}
	l := newRsyncListing(scope, emit, complete, b.msgRelPath)
	result, err := b.runDaemon(ctx, "RLIST", []string{"-n", "-r"}, remote, rsyncclient.WithFileListCallback(l.add), rsyncclient.WithStderr(l))
	if err != nil {
		return err
	}
	if err := l.finish("RLIST "+remote, result); err != nil {
		Log.Add("rsync", DirWarn, err.Error())
		return err
	}
	Log.Add("rsync", DirIn, fmt.Sprintf("RLIST %d entries", len(result.FileList)))
	return nil
}

// msgRelPath maps a path in a daemon message to a relPath under the
// backend's base. rsync gives it from the module root: "photos" when the
// run starts at the root, "/sub/photos" when it starts in sub.
func (b *RsyncBackend) msgRelPath(p string) (string, bool) {
	return relUnder("/"+b.base, "/"+strings.TrimPrefix(p, "/"))
}

func (b *RsyncBackend) ProbeChecksums() []string { return []string{"md4", "rsync"} }

func (b *RsyncBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
	b.useChecksum = algo == "rsync" || algo == "md4"
}

func (b *RsyncBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	switch {
	case b.cksumAlgo == "md4":
		return b.md4.lookup(ctx, relPath, b.fetchMD4)
	case !b.useChecksum:
		return "", errors.New("rsync: checksum not enabled")
	}
	return "rsync_internal", nil
}

func (b *RsyncBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	if b.cksumAlgo != "md4" {
		return nil
	}
	_, err := b.fetchMD4(ctx, scope, recursive)
	return err
}

func (b *RsyncBackend) fetchMD4(ctx context.Context, scope string, recursive bool) (map[string]string, error) {
	remote, err := b.modulePath(scope, recursive)
	if err != nil {
		return nil, err
	}
	result, err := b.runDaemon(ctx, "MD4", md4ListFlags(recursive), remote)
	if err != nil {
		return nil, err
	}
	got := b.md4.fromFileList(scope, recursive, result.FileList)
	Log.Add("rsync", DirIn, fmt.Sprintf("MD4 %d checksums", len(got)))
	return got, nil
}

// SetTimes cannot set times by itself: the daemon protocol carries them only
// with file data. Every upload sends the mtime with -t, so a SetTimes for the
// time an upload just sent is already done, to the second as protocol 27
// sends it.
func (b *RsyncBackend) SetTimes(_ context.Context, relPath string, mtime, _, _ time.Time) error {
	if sent, ok := b.sentMtime.LoadAndDelete(relPath); ok && sent.(time.Time).Unix() == mtime.Unix() {
		return nil
	}
	return errors.New("rsync: set times not supported")
}

// noteSent remembers the mtime an upload of relPath just sent with -t, read
// from the local file it pushed.
func (b *RsyncBackend) noteSent(relPath, localPath string) {
	if fi, err := os.Stat(localPath); err == nil {
		b.sentMtime.Store(relPath, fi.ModTime())
	}
}

// transferArgs are the flags for one data-moving call plus extra.
func (b *RsyncBackend) transferArgs(extra ...string) []string {
	args := transferFlags()
	if b.useChecksum {
		args = append(args, "-c")
	}
	return append(args, extra...)
}

// stageUploadPath builds, under tmpDir, the full relPath directory chain with
// an empty leaf, returning the top-level staging component and the leaf path.
// The rsync daemon protocol has no mkdir: a recursive (-r) send of the top
// component is the only way to recreate relPath's leading dirs on the remote,
// the same trick Mkdir uses. Callers populate leaf, then send stageTop to the
// module root with -r.
func stageUploadPath(tmpDir, relPath string) (stageTop, leaf string, err error) {
	clean := strings.Trim(path.Clean(relPath), "/")
	leaf = filepath.Join(tmpDir, filepath.FromSlash(clean))
	if err := os.MkdirAll(filepath.Dir(leaf), 0755); err != nil {
		return "", "", err
	}
	return filepath.Join(tmpDir, strings.SplitN(clean, "/", 2)[0]), leaf, nil
}

// push runs one send of src to dest with extra flags, crediting adder from
// the --progress offsets above start and settling it against budget
// afterwards.
func (b *RsyncBackend) push(ctx context.Context, label, src, dest string, adder *CappedAdder, budget, start int64, extra ...string) error {
	args := b.transferArgs(extra...)
	var stdout io.Writer = io.Discard
	if adder != nil {
		stdout = &rsyncProgressWriter{adder: adder, last: start}
		args = append(args, "--progress")
	}
	Log.Add("rsync", DirOut, label+" ["+strings.Join(args, " ")+"]")
	err := b.transfer(ctx, args, dest, []string{src}, true, stdout)
	if err != nil {
		Log.Add("rsync", DirErr, err.Error())
	}
	settlePush(ctx, adder, budget, err)
	return err
}

// SendLocalFile sends an existing local file to the daemon. The file is
// hardlinked under its full relPath and the top component sent with -r so the
// daemon recreates the leading dirs; across filesystems the link fails and a
// Mkdir plus flat send takes over.
func (b *RsyncBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, _ os.FileMode) error {
	tmpDir, err := os.MkdirTemp("", "rsync-send-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	parentRel := parentDir(relPath)
	src, destRel := srcPath, parentRel
	if stageTop, link, perr := stageUploadPath(tmpDir, relPath); perr == nil && os.Link(srcPath, link) == nil {
		src, destRel = stageTop, ""
	} else if err := b.Mkdir(ctx, parentRel, 0o755); err != nil {
		return err
	}
	dest, err := b.pushTarget(ctx, destRel)
	if err != nil {
		return err
	}
	var adder *CappedAdder
	fileSize, _ := fileSizeFromContext(ctx)
	if counter := progressFromContext(ctx); counter != nil && fileSize > 0 {
		adder = NewCappedAdder(counter, fileSize)
	}
	err = b.push(ctx, "SEND "+srcPath+" -> "+relPath, src, dest+"/", adder, fileSize, 0, "-r")
	if err == nil {
		b.noteSent(relPath, srcPath)
	}
	b.listCache.invalidateAncestors(relPath)
	b.md4.invalidate(relPath)
	return err
}

// RecvToLocalFile downloads directly to dstPath, through rsync's temp file
// next to it, renamed over it at the end (see rsyncTransferFlags).
func (b *RsyncBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	remote, err := b.modulePath(relPath, false)
	if err != nil {
		return err
	}
	return rsyncRecvToLocal(ctx, "rsync", relPath, dstPath, func(dstDir string) error {
		Log.Add("rsync", DirOut, "RECV "+relPath+" -> "+dstPath)
		err := b.transfer(ctx, b.transferArgs(), remote, []string{dstDir}, false, nil)
		if err != nil {
			Log.Add("rsync", DirErr, err.Error())
		}
		return err
	})
}

func (b *RsyncBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	tmpDir, err := os.MkdirTemp("", "rsync-upload-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	stageTop, tmpFile, err := stageUploadPath(tmpDir, relPath)
	if err != nil {
		return err
	}
	dest, err := b.pushTarget(ctx, "")
	if err != nil {
		return err
	}
	pushAdder, pushBudget, err := stageUpload(ctx, src, tmpFile, mode)
	if err != nil {
		return err
	}
	err = b.push(ctx, "SEND "+relPath, stageTop, dest+"/", pushAdder, pushBudget, 0, "-r")
	if err == nil {
		b.noteSent(relPath, tmpFile)
	}
	b.listCache.invalidateAncestors(relPath)
	b.md4.invalidate(relPath)
	return err
}

// AppendFrom resumes a partial upload with --append-verify: the daemon
// reports the size it holds, only the tail is sent, into the parent that the
// partial proves exists, and the daemon checks the whole file against the
// sender's sum so a prefix that differs fails the append instead of being
// kept. A local source already carrying the destination name is sent from
// its own path, anything else is hardlinked or spooled under that name first.
// Progress covers the tail only; the engine has credited the prefix.
func (b *RsyncBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	dest, err := b.pushTarget(ctx, parentDir(relPath))
	if err != nil {
		return err
	}
	name := path.Base(relPath)
	srcPath := src.LocalPath()
	if srcPath == "" || filepath.Base(srcPath) != name {
		tmpDir, err := os.MkdirTemp("", "rsync-append-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)
		staged := filepath.Join(tmpDir, name)
		if srcPath == "" || os.Link(srcPath, staged) != nil {
			if err := spoolTo(ctx, src, staged, mode); err != nil {
				return err
			}
		}
		srcPath = staged
	}
	tailSize := src.Size() - offset
	var adder *CappedAdder
	if counter := progressFromContext(ctx); counter != nil && tailSize > 0 {
		adder = NewCappedAdder(counter, tailSize)
	}
	label := fmt.Sprintf("APPEND %s @%d/%d", relPath, offset, src.Size())
	ExpectSilence(ctx, appendVerifySilence(offset))
	err = b.push(ctx, label, srcPath, dest+"/", adder, tailSize, offset, "--append-verify")
	if err == nil {
		b.noteSent(relPath, srcPath)
	}
	b.listCache.invalidateAncestors(relPath)
	b.md4.invalidate(relPath)
	return err
}

func (b *RsyncBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	clean := strings.Trim(path.Clean(relPath), "/")
	if clean == "" || clean == "." {
		return nil
	}
	tmpDir, err := os.MkdirTemp("", "rsync-mkdir-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	dest, err := b.pushTarget(ctx, "")
	if err != nil {
		return err
	}
	dirMode := mode.Perm()
	if dirMode == 0 {
		dirMode = 0755
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, filepath.FromSlash(clean)), dirMode); err != nil {
		return err
	}
	stageTop := filepath.Join(tmpDir, strings.SplitN(clean, "/", 2)[0])
	Log.Add("rsync", DirOut, "MKDIR "+clean)
	err = b.transfer(ctx, []string{"-r", "-t", "-O"}, dest+"/", []string{stageTop}, true, nil)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("rsync", DirErr, err.Error())
	}
	return err
}

func (b *RsyncBackend) Rename(context.Context, string, string) error {
	return errors.New("rsync: rename not supported")
}

// rsyncFilterName writes name into a filter rule so that the rule matches
// only that name. rsync reads a rule as a wildcard pattern when it holds *,
// ? or [ (exclude.c add_rule), and in a pattern a backslash makes the byte
// after it stand for itself; it compares any other rule as written,
// backslashes too. wild says the rule is a pattern whatever the name holds,
// as a rule ending in "/***" is.
func rsyncFilterName(name string, wild bool) string {
	if !wild && !strings.ContainsAny(name, "*?[") {
		return name
	}
	return rsyncFilterEscaper.Replace(name)
}

var rsyncFilterEscaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

func (b *RsyncBackend) Remove(ctx context.Context, relPath string) error {
	return b.remove(ctx, relPath, false)
}

func (b *RsyncBackend) RemoveAll(ctx context.Context, relPath string) error {
	return b.remove(ctx, relPath, true)
}

// remove deletes relPath by pushing an empty staging dir to its parent with
// --delete and a filter that names only the victim.
func (b *RsyncBackend) remove(ctx context.Context, relPath string, recursive bool) error {
	if isBaseRel(relPath) {
		return errors.New("rsync: refusing to remove module root")
	}
	clean := strings.Trim(path.Clean(relPath), "/")
	if strings.ContainsAny(clean, "\r\n") {
		return fmt.Errorf("rsync: a daemon cannot be sent a name with a line break: %q", clean)
	}
	dest, err := b.pushTarget(ctx, parentDir(clean))
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "rsync-remove-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	name := path.Base(clean)
	args := []string{"-r", "--delete", "--include=/" + rsyncFilterName(name, false)}
	op := "REMOVE"
	if recursive {
		args = append(args, "--include=/"+rsyncFilterName(name, true)+"/***")
		op = "REMOVEALL"
	}
	args = append(args, "--exclude=*")
	Log.Add("rsync", DirOut, op+" "+clean)
	err = b.transfer(ctx, args, dest+"/", []string{tmpDir + "/"}, true, nil)
	if recursive {
		b.listCache.invalidateTree(relPath)
		b.md4.invalidateTree(relPath)
	} else {
		b.md4.invalidate(relPath)
	}
	b.listCache.invalidate(parentDir(relPath))
	if err != nil {
		Log.Add("rsync", DirErr, op+" "+clean+": "+err.Error())
		return err
	}
	Log.Add("rsync", DirIn, op+" "+clean+" OK")
	return nil
}

func (b *RsyncBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.openAt(ctx, relPath, 0)
}

// OpenAt resumes a download: --append against a sparse placeholder of
// offset bytes makes the daemon send only the tail. The daemon's file sum
// covers the real prefix, which the placeholder does not hold, so the
// receive is unverified; the engine checksums the resumed file afterwards.
func (b *RsyncBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	return b.openAt(ctx, relPath, offset)
}

func (b *RsyncBackend) openAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	remote, err := b.modulePath(relPath, false)
	if err != nil {
		return nil, err
	}
	return rsyncOpenViaTemp(ctx, relPath, offset, func(dstDir string) error {
		if offset > 0 {
			res, err := b.runDaemonTo(ctx, fmt.Sprintf("RECV @%d", offset), b.transferArgs("--append"), remote, dstDir, rsyncclient.WithUnverifiedAppend())
			if err != nil {
				return err
			}
			// The placeholder already holds offset bytes, so a source the
			// daemon could not send leaves it in place and the run ends
			// without an error: only the error counts show it.
			if err := transferErr(res.XferErrors, res.IOErrors); err != nil {
				return fmt.Errorf("%s: %w", relPath, err)
			}
			// --append also leaves the placeholder alone when the source is
			// not longer than it, as rsync does, so a source shorter than
			// offset shows only in the file list.
			for _, fi := range res.FileList {
				if fi.Length < offset {
					return fmt.Errorf("%s: source is %d bytes, shorter than offset %d", relPath, fi.Length, offset)
				}
			}
			return nil
		}
		Log.Add("rsync", DirOut, "RECV "+relPath)
		err := b.transfer(ctx, b.transferArgs(), remote, []string{dstDir}, false, nil)
		if err != nil {
			Log.Add("rsync", DirErr, err.Error())
		}
		return err
	})
}
