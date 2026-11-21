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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"

	"sc/model"
)

// RsyncBackend talks to an rsync daemon (rsync://). Listings and MD4 sums
// come from dry runs of the in-process client; transfers run through the
// rsync URL form so the daemon can recreate leading directories with -r.
type RsyncBackend struct {
	host      string
	user      string
	pass      string
	module    string
	base      string
	display   string
	listCache *listCache
	sentMtime sync.Map // mtime each upload sent with -t, see SetTimes
	dirs      sync.Map // folders seen on the daemon, by path: see checkDirs
	noSum     sumNote  // says the first file the daemon gives no sum
	gate      connGate // the connections open to the daemon, past "max connections"
}

func NewRsyncBackend(rawURL string) (*RsyncBackend, error) {
	_, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if port == "" {
		port = "873"
	}
	module, base, _ := strings.Cut(strings.TrimPrefix(remotePath, "/"), "/")
	// A URL no try can use is final (R18).
	if module == "" {
		return nil, model.WithKind(fs.ErrInvalid, errors.New("rsync: module name required in URL"))
	}
	base = strings.Trim(base, "/")
	if strings.ContainsAny(remotePath, "\r\n") {
		return nil, model.WithKind(fs.ErrInvalid, errors.New("rsync: a daemon takes no module or folder name with a line break"))
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
		listCache: newListCache().retryingRuns("rsync"), // each run dials the daemon
	}, nil
}

func (b *RsyncBackend) BasePath() string { return b.display }

// Close does nothing: every call dials the daemon and hangs up after.
func (b *RsyncBackend) Close() error { return nil }

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

// baseRel is relPath below the module: the base joined with relPath
// (cleanRel).
func (b *RsyncBackend) baseRel(relPath string) string {
	relPath = cleanRel(relPath)
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
	res, err := b.runDaemon(ctx, "STAT", []string{"-n", "-d"}, remote, nil)
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
	// A download of what is not there: stock rsyncd says so, then resets
	// the connection, which read as a lost one (R18).
	if !send && notThere(peer.problems()) {
		return fmt.Errorf("RECV %s: %w", remote, fs.ErrNotExist)
	}
	msgs := peer.all()
	if readOnly(msgs) {
		Log.Add("rsync", DirFatal, "module is read only, aborting operation")
		TriggerFatalAbort(ctx)
	}
	if len(msgs) > 0 {
		err = fmt.Errorf("%w: %s", err, strings.Join(msgs[:min(len(msgs), 3)], "; "))
	}
	return err
}

// rsyncIdle is the longest a daemon run that no stall guard watches may go
// without a byte from the daemon: a stat, a listing, a mkdir, a delete, a
// checksum fetch. Only the dial had a deadline, so on a daemon gone silent,
// as at the end of an ssh -L whose far end is dead, such a run waited for a
// cancel (R17). A transfer has the copy's stall guard (-stall-timeout).
var rsyncIdle = idleSetting{def: 5 * time.Minute}

// SetRsyncIdleTimeout sets rsyncIdle; 0 or less waits for ever.
func SetRsyncIdleTimeout(d time.Duration) { rsyncIdle.set(d) }

// daemonIdle is how long a run with flags may wait for the daemon before it
// fails, 0 for as long as it takes, and the time the daemon's own hashing
// gets on top. A run under a stall guard is the guard's. With -c the daemon
// hashes each file it lists before it sends its entry, as long as the
// caller says that takes (model.HashBytes); a caller that does not say
// leaves the run no deadline, as its time cannot be told.
func daemonIdle(ctx context.Context, flags []string) (idle, hash time.Duration) {
	if watchFromContext(ctx) != nil {
		return 0, 0
	}
	n := model.HashBytes(ctx)
	if n <= 0 && slices.Contains(flags, "-c") {
		return 0, 0
	}
	return rsyncIdle.get(), HashTime(n)
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
func (b *RsyncBackend) runDaemon(ctx context.Context, label string, flags []string, remotePath string, peer *peerMsgs, opts ...rsyncclient.Option) (*rsyncclient.Result, error) {
	dst, err := tempDir("rsync-daemon-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dst)
	return b.runDaemonTo(ctx, label, flags, remotePath, dst+"/", peer, opts...)
}

// runDaemonTo runs client against remotePath, receiving into local; peer,
// when not nil, takes what the daemon says. A failed run is an ERR line,
// unless the caller ended ctx, as a scan that is done stops its preload
// (a stall guard's end is an ERR), or peer says the path is not there: stock rsyncd says so, then resets the
// connection, and the STAT of each folder a copy makes was an ERR line,
// which opened the log. The caller reads that answer.
func (b *RsyncBackend) runDaemonTo(ctx context.Context, label string, flags []string, remotePath, local string, peer *peerMsgs, opts ...rsyncclient.Option) (*rsyncclient.Result, error) {
	Log.Add("rsync", DirOut, label+" "+remotePath)
	if peer != nil {
		opts = append(opts, rsyncclient.WithStderr(peer))
	}
	result, err := b.runDaemonPaths(ctx, flags, remotePath, []string{local}, opts...)
	switch {
	case err == nil:
	case ctx.Err() != nil && !Stalled(ctx):
		Log.Add("rsync", DirIn, label+" "+remotePath+": canceled")
	case peer != nil && notThere(peer.problems()):
		Log.Add("rsync", DirIn, label+" "+remotePath+": not there")
	default:
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
	result := &rsyncclient.Result{}
	run := func(ctx context.Context) error {
		conn, done, err := b.login(ctx, client, remotePath)
		if err != nil {
			return err
		}
		defer b.gate.leave()
		defer conn.Close()
		if !done {
			// Bytes on the socket show the stall guard the run moves,
			// also when no progress is credited: a push of a source that
			// counted its own bytes (D5).
			defer context.AfterFunc(ctx, func() { conn.Close() })()
			result, err = client.Run(ctx, activeConn(ctx, conn), local)
		}
		return err
	}
	idle, hash := daemonIdle(ctx, flags)
	if idle <= 0 {
		return result, run(ctx)
	}
	err = WithStallGuard(ctx, new(atomic.Int64), idle, func(ctx context.Context) error {
		ExpectSilence(ctx, hash)
		return run(ctx)
	})
	if errors.Is(err, ErrStall) {
		err = fmt.Errorf("rsync: the daemon sent nothing for %v (-rsync-timeout): %w", idle, err)
	}
	return result, err
}

// login connects to the daemon and logs in, as the gate lets it (connGate).
// A daemon with as many connections as it takes ("max connections") turns
// the login away: sc then opens no more at a time than it says it takes,
// and waits for one of its own to close, or, within that, a while, and
// tries again. Each is a WARN line, no ERR: nothing failed. With none of
// sc's open, other clients hold them all, and after serverFullWait of that
// it gives up. The connection holds its place in the gate until the
// caller's leave. done: the daemon answered EXIT, there is nothing to run.
func (b *RsyncBackend) login(ctx context.Context, client *rsyncclient.Client, remotePath string) (net.Conn, bool, error) {
	refused, seen := 0, 0 // turns away in a row, and the limit when they began
	var fullSince time.Time
	warned := false
	for {
		waited, err := b.gate.enter(ctx)
		if err != nil {
			return nil, false, err
		}
		if waited {
			refused, fullSince = 0, time.Time{}
		}
		conn, done, err := b.dialLogin(ctx, client, remotePath)
		if err == nil {
			return conn, done, nil
		}
		if !errors.Is(err, model.ErrTooMany) || ctx.Err() != nil {
			b.gate.leave()
			return nil, false, err
		}
		if refused++; refused == 1 {
			seen = b.gate.limitNow()
		}
		most := 0
		if full := (*fullError)(nil); errors.As(err, &full) {
			most = full.most
		}
		limit, lowered, open := b.gate.turnedAway(refused >= stuckTurns, seen, most)
		if lowered {
			Log.Add("rsync", DirWarn, fmt.Sprintf("%s: %v: %d connection(s) at a time from now on", b.display, err, limit))
		}
		if lowered || limit != seen {
			refused, seen = 0, limit
		}
		if limit > 0 && open >= limit {
			continue // enter waits for one to close
		}
		switch {
		case open > 0:
			fullSince = time.Time{}
		case fullSince.IsZero():
			fullSince = time.Now()
		case time.Since(fullSince) >= serverFullWait:
			return nil, false, fmt.Errorf("%w for %v, with none of sc's open: %w", errServerFull, serverFullWait, err)
		}
		pause := closeLag << min(max(refused-1, 0), 5)
		if open == 0 && refused >= stuckTurns-1 && !warned {
			warned = true
			Log.Add("rsync", DirWarn, fmt.Sprintf("%s: %v: other clients hold them all; trying again for up to %v", b.display, err, serverFullWait))
		}
		if err := waitTouching(ctx, nil, pause); err != nil {
			return nil, false, err
		}
	}
}

// dialLogin dials the daemon and runs sc's handshake (daemonHandshake).
func (b *RsyncBackend) dialLogin(ctx context.Context, client *rsyncclient.Client, remotePath string) (net.Conn, bool, error) {
	conn, err := dialLimited(ctx, "tcp", b.host)
	if err != nil {
		return nil, false, err
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	done, err := b.daemonHandshake(ctx, activeConn(ctx, conn), client, remotePath)
	stop()
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		conn.Close()
		return nil, false, err
	}
	return conn, done, nil
}

// SetParallel is + and -: more copies than the daemon took when it turned
// one away let it show again what it takes.
func (b *RsyncBackend) SetParallel(n int) { b.gate.raise(n) }

// ConnLimit is what the daemon took when it turned a login away.
func (b *RsyncBackend) ConnLimit() int { return b.gate.limitNow() }

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
// hangs up after an empty list. -l makes it send links, with their targets:
// stock rsync left them out, gorsyncd sent them without targets.
func (b *RsyncBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	remote, err := b.modulePath(relDir, true)
	if err != nil {
		return nil, err
	}
	label := "LIST " + remote
	peer := &peerMsgs{}
	result, err := b.runDaemon(ctx, "LIST", []string{"-n", "-d", "-l"}, remote, peer)
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
	result, err := b.runDaemon(ctx, "STAT", []string{"-n", "-d", "-l", "-D"}, remote, peer)
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
		e := linkEntry(model.FileEntry{RelPath: relPath, Name: path.Base(relPath), Size: fi.Length, ModTime: fi.ModTime, TimeRes: model.TimeSecond, IsDir: mode.IsDir(), Mode: mode}, fi)
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

// readOnly reports the daemon's word that the module takes no writes:
// "ERROR: module is read only", from stock rsyncd and gorsyncd alike. It
// is a message's whole reason (reasons), never words in a name a message
// gives: an upload of a file named "module is read only" that the daemon
// could not write aborted the whole operation (R18).
func readOnly(msgs []string) bool {
	for _, m := range msgs {
		for _, r := range reasons(m) {
			if strings.TrimSpace(r) == "module is read only" {
				return true
			}
		}
	}
	return false
}

// notThere reports whether the messages of a one-path run say only that
// the path is missing: `link_stat "..." failed: No such file or directory
// (2)` from rsync, the same in Go's lower case from gorsyncd since gorsync
// cf119b2. An older gorsyncd says nothing and only sets io_error, as for a
// path it cannot read; that stays an error, never "not there". The words
// are read at the end of a message, where the reason is, never in the
// path it names: a file named "no such file or directory" that could not
// be read was "not there" (R18).
func notThere(msgs []string) bool {
	for _, m := range msgs {
		m = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(m)), " (2)")
		if !strings.HasSuffix(m, "no such file or directory") {
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
	result, err := b.runDaemon(ctx, "RLIST", []string{"-n", "-r", "-l"}, remote, nil, rsyncclient.WithFileListCallback(l.add), rsyncclient.WithStderr(l))
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

// ChecksumAlgos: a daemon gives md4 in its file list (-c) and nothing
// else. An "rsync" algorithm gave the same text for every file, so any two
// files would have compared equal, and switched -c on for transfers.
func (b *RsyncBackend) ChecksumAlgos(context.Context) ([]string, error) { return []string{"md4"}, nil }

func (b *RsyncBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	if algo != "md4" {
		return "", model.WithKind(ErrUnsupported, fmt.Errorf("rsync: no %q checksum, only md4", algo))
	}
	return md4Of(ctx, "rsync", relPath, b.fetchMD4, &b.noSum)
}

func (b *RsyncBackend) PrefetchChecksums(ctx context.Context, scope, algo string, recursive bool) (map[string]string, error) {
	if algo != "md4" {
		return nil, nil
	}
	return b.fetchMD4(ctx, scope, recursive)
}

func (b *RsyncBackend) fetchMD4(ctx context.Context, scope string, recursive bool) (map[string]string, error) {
	remote, err := b.modulePath(scope, recursive)
	if err != nil {
		return nil, err
	}
	peer := &peerMsgs{}
	result, err := b.runDaemon(ctx, "MD4", md4ListFlags(recursive), remote, peer)
	if err != nil {
		// Stock rsyncd says a path is not there, then resets the
		// connection, which read as a passing failure (R18).
		if notThere(peer.problems()) {
			return nil, fmt.Errorf("MD4 %s: %w", remote, fs.ErrNotExist)
		}
		return nil, err
	}
	got := md4Sums(scope, recursive, result.FileList)
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

// transferArgs are the flags for one data-moving call plus extra. Never
// -c: rsync checks it before --ignore-times and skips a file whose content
// matches, after both sides read it whole with no byte moving, which the
// stall guard takes for a hung transfer. It was on whenever a checksum
// compare had picked md4 (D2); rsync+ssh never used it.
func (b *RsyncBackend) transferArgs(extra ...string) []string {
	return append(transferFlags(), extra...)
}

// checkDirs reports whether relDir is a folder on the daemon, before a
// write that needs it. A staged push (see stageUploadPath) sends every
// folder above what it writes, and rsync puts a folder where a file is in
// its way: an upload or a Mkdir under a name that was a file there deleted
// that file, with no word, and a delete made the folder it deleted in when
// that was not there (R21). So a part of relDir that is no folder is an
// error, and one that is not there means relDir is not: a push makes it,
// and a delete has nothing to do. Folders seen before cost nothing: the
// first one not seen, from relDir up, costs a Stat, and so does each one
// above it, up to one that is there.
func (b *RsyncBackend) checkDirs(ctx context.Context, relDir string) (bool, error) {
	var unseen []string // relDir and its folders up to the first seen one
	for p := cleanRel(relDir); p != ""; p = parentDir(p) {
		if _, ok := b.dirs.Load(p); ok {
			break
		}
		unseen = append(unseen, p)
	}
	for i, p := range unseen {
		e, err := b.Stat(ctx, p)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case err != nil:
			return false, err
		case !e.IsDir:
			return false, fmt.Errorf("rsync: %s is there on the daemon and is no folder: %w", p, fs.ErrExist)
		}
		b.sawDirs(p)
		return i == 0, nil
	}
	return len(unseen) == 0, nil
}

// sawDirs notes relDir and the folders above it as folders on the daemon.
func (b *RsyncBackend) sawDirs(relDir string) {
	for p := cleanRel(relDir); p != ""; p = parentDir(p) {
		b.dirs.Store(p, true)
	}
}

// forgetDirs drops relPath, and with tree each folder under it, from the
// folders seen, after a delete there.
func (b *RsyncBackend) forgetDirs(relPath string, tree bool) {
	clean := cleanRel(relPath)
	b.dirs.Delete(clean)
	if !tree {
		return
	}
	b.dirs.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), clean+"/") {
			b.dirs.Delete(k)
		}
		return true
	})
}

// stageUploadPath builds, under tmpDir, the full relPath directory chain with
// an empty leaf, returning the top-level staging component and the leaf path.
// The rsync daemon protocol has no mkdir: a recursive (-r) send of the top
// component is the only way to recreate relPath's leading dirs on the remote,
// the same trick Mkdir uses. Callers populate leaf, then send stageTop to the
// module root with -r.
func stageUploadPath(tmpDir, relPath string) (stageTop, leaf string, err error) {
	clean := cleanRel(relPath)
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
func (b *RsyncBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, p model.PutParams) error {
	parentRel := parentDir(relPath)
	if _, err := b.checkDirs(ctx, parentRel); err != nil {
		return err
	}
	tmpDir, err := tempDir("rsync-send-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
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
	fileSize := max(p.Size, 0)
	if counter := progressFromContext(ctx); counter != nil && fileSize > 0 {
		adder = NewCappedAdder(counter, fileSize)
	}
	err = b.push(ctx, "SEND "+srcPath+" -> "+relPath, src, dest+"/", adder, fileSize, 0, "-r")
	if err == nil {
		b.noteSent(relPath, srcPath)
		b.sawDirs(parentRel)
	}
	b.listCache.invalidateAncestors(relPath)
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

func (b *RsyncBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	if _, err := b.checkDirs(ctx, parentDir(relPath)); err != nil {
		return err
	}
	tmpDir, err := tempDir("rsync-upload-*")
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
	pushAdder, pushBudget, unstage, err := stageUpload(ctx, src, tmpFile, p)
	if err != nil {
		return err
	}
	err = b.push(ctx, "SEND "+relPath, stageTop, dest+"/", pushAdder, pushBudget, 0, "-r")
	if err == nil {
		b.noteSent(relPath, tmpFile)
		b.sawDirs(parentDir(relPath))
	} else {
		unstage()
	}
	b.listCache.invalidateAncestors(relPath)
	return err
}

// AppendFrom resumes a partial upload with --append-verify: the daemon
// reports the size it holds, only the tail is sent, into the parent that the
// partial proves exists, and the daemon checks the whole file against the
// sender's sum so a prefix that differs fails the append instead of being
// kept. A local source already carrying the destination name is sent from
// its own path, anything else is hardlinked or spooled under that name first.
// Progress covers the tail only; the engine has credited the prefix.
func (b *RsyncBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	dest, err := b.pushTarget(ctx, parentDir(relPath))
	if err != nil {
		return err
	}
	name := path.Base(relPath)
	srcPath := src.LocalPath()
	if srcPath == "" || filepath.Base(srcPath) != name {
		tmpDir, err := tempDir("rsync-append-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)
		staged := filepath.Join(tmpDir, name)
		if srcPath == "" || os.Link(srcPath, staged) != nil {
			if err := spoolTo(ctx, src, staged, p); err != nil {
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
	return err
}

func (b *RsyncBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	clean := cleanRel(relPath)
	if clean == "" {
		return nil
	}
	// A folder that is there needs no push; a file in the way is an error.
	if there, err := b.checkDirs(ctx, clean); err != nil || there {
		return err
	}
	tmpDir, err := tempDir("rsync-mkdir-*")
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
		return err
	}
	b.sawDirs(clean)
	return nil
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
	clean := cleanRel(relPath)
	op := "REMOVE"
	if recursive {
		op = "REMOVEALL"
	}
	return b.removeRun(ctx, op+" "+clean, parentDir(clean), []model.RemoveItem{{Name: path.Base(clean), IsDir: recursive}})
}

// removeBatch is the most entries one run of RemoveIn deletes, which
// bounds the rules a run sends. The daemon matches each name in the folder
// against the rules in turn: a run of many costs what as many runs of one
// did in matching, and saves their listings of the folder.
var removeBatch = 1000

// RemoveIn deletes entries of dir in one --delete run per removeBatch of
// them, with the rules a Remove or a RemoveAll sends, one after the other.
// A run lists the folder, and a mirror made one per entry it deleted: V
// deletes in a folder of N entries listed it V times (R16).
func (b *RsyncBackend) RemoveIn(ctx context.Context, dir string, items []model.RemoveItem) error {
	dir = cleanRel(dir)
	where := dir
	if where == "" {
		where = "/"
	}
	for len(items) > 0 {
		n := min(len(items), removeBatch)
		if err := b.removeRun(ctx, fmt.Sprintf("REMOVE %d in %s", n, where), dir, items[:n]); err != nil {
			return err
		}
		items = items[n:]
	}
	return nil
}

// removeRun deletes items, entries of dir, in one run: an empty folder is
// pushed into dir with -r --delete, and filter rules name what goes. label
// names the run in the log.
func (b *RsyncBackend) removeRun(ctx context.Context, label, dir string, items []model.RemoveItem) error {
	for _, it := range items {
		full := path.Join(dir, it.Name)
		switch {
		case !model.IsFileName(it.Name):
			return fmt.Errorf("rsync: %s: %q is no name in a folder: %w", label, it.Name, fs.ErrInvalid)
		case strings.ContainsAny(full, "\r\n"):
			return fmt.Errorf("rsync: a daemon cannot be sent a name with a line break: %q", full)
		}
	}
	// The push below goes into dir, and makes it when it is not there.
	there, err := b.checkDirs(ctx, dir)
	if errors.Is(err, fs.ErrExist) || err == nil && !there {
		err := fmt.Errorf("rsync: %s: the folder it is in is not there: %w", label, fs.ErrNotExist)
		Log.Add("rsync", DirErr, err.Error())
		return err
	}
	if err != nil {
		return err
	}
	dest, err := b.pushTarget(ctx, dir)
	if err != nil {
		return err
	}
	tmpDir, err := tempDir("rsync-remove-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	args := []string{"-r", "--delete"}
	for _, it := range items {
		args = append(args, "--include=/"+rsyncFilterName(it.Name, false))
		if it.IsDir {
			args = append(args, "--include=/"+rsyncFilterName(it.Name, true)+"/***")
		}
	}
	args = append(args, "--exclude=*")
	Log.Add("rsync", DirOut, label)
	err = b.transfer(ctx, args, dest+"/", []string{tmpDir + "/"}, true, nil)
	for _, it := range items {
		rel := path.Join(dir, it.Name)
		b.forgetDirs(rel, it.IsDir)
		if it.IsDir {
			b.listCache.invalidateTree(rel)
		}
	}
	b.listCache.invalidate(dir)
	if err != nil {
		Log.Add("rsync", DirErr, label+": "+err.Error())
		return err
	}
	Log.Add("rsync", DirIn, label+" OK")
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
			peer := &peerMsgs{}
			res, err := b.runDaemonTo(ctx, fmt.Sprintf("RECV @%d", offset), b.transferArgs("--append"), remote, dstDir, peer, rsyncclient.WithUnverifiedAppend())
			if (err != nil || res.XferErrors > 0 || res.IOErrors != 0) && notThere(peer.problems()) {
				return fmt.Errorf("RECV %s: %w", remote, fs.ErrNotExist)
			}
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
