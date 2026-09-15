package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"
	"github.com/gokrazy/rsync/rsynccmd"

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

	displayHost := host
	if port != "873" {
		displayHost = net.JoinHostPort(host, port)
	}
	display := "rsync://" + displayHost + "/" + module
	if base != "" {
		display += "/" + base
	}
	// gorsync's daemon exchange takes the username only from the environment;
	// the password goes through a per-call --password-file.
	if user != "" {
		os.Setenv("RSYNC_USERNAME", user)
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
// slash when the contents of a directory are wanted.
func (b *RsyncBackend) modulePath(relPath string, dir bool) string {
	p := b.module
	if b.base != "" {
		p += "/" + b.base
	}
	if relPath != "" {
		p += "/" + relPath
	}
	if dir {
		p += "/"
	}
	return p
}

// remoteURL is the rsync:// form for transfers. gorsync parses it with
// net/url, so every path segment is percent-encoded or a name holding '#',
// '?' or '%' would cut or corrupt the path.
func (b *RsyncBackend) remoteURL(relPath string) string {
	segs := strings.Split(b.modulePath(relPath, false), "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	hostPart := b.host
	if b.user != "" {
		userinfo := url.PathEscape(b.user)
		if b.pass != "" {
			userinfo += ":" + url.PathEscape(b.pass)
		}
		hostPart = userinfo + "@" + b.host
	}
	return "rsync://" + hostPart + "/" + strings.Join(segs, "/")
}

func (b *RsyncBackend) rsyncRun(ctx context.Context, args ...string) (string, error) {
	var stdout bytes.Buffer
	if err := b.rsyncRunStdout(ctx, &stdout, args...); err != nil {
		return "", err
	}
	return stdout.String(), nil
}

// rsyncRunStdout invokes rsync with stdout routed through w. Used by transfer
// paths that pass --progress to credit byte progress via rsyncProgressWriter.
func (b *RsyncBackend) rsyncRunStdout(ctx context.Context, w io.Writer, args ...string) error {
	cmd := rsynccmd.Command("rsync", args...)
	cmd.DialContext = dialLimited
	var stderr bytes.Buffer
	cmd.Stdout = w
	cmd.Stderr = &stderr
	_, err := cmd.Run(ctx)
	if err == nil {
		return nil
	}
	errMsg := strings.TrimSpace(stderr.String())
	if strings.Contains(errMsg, "module is read only") {
		Log.Add("rsync", DirFatal, "module is read only, aborting operation")
		TriggerFatalAbort(ctx)
	}
	if errMsg != "" {
		return fmt.Errorf("%v: %s", err, errMsg)
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
// returning the file list. The password travels in a 0600 file via
// --password-file so no process-wide state is involved.
func (b *RsyncBackend) runDaemon(ctx context.Context, label string, flags []string, remotePath string) (*rsyncclient.Result, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-daemon-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	if b.pass != "" {
		pw := filepath.Join(tmpDir, "pw")
		if err := os.WriteFile(pw, []byte(b.pass), 0600); err != nil {
			return nil, err
		}
		flags = append(flags, "--password-file="+pw)
	}
	dst := filepath.Join(tmpDir, "dst")
	if err := os.Mkdir(dst, 0700); err != nil {
		return nil, err
	}
	client, err := newRsyncClient(flags, rsyncclient.WithoutNegotiate(), rsyncclient.DontRestrict())
	if err != nil {
		return nil, err
	}
	conn, err := dialLimited(ctx, "tcp", b.host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	Log.Add("rsync", DirOut, label+" "+remotePath)
	result, err := client.RunDaemon(ctx, conn, remotePath, []string{dst + "/"})
	if err != nil {
		Log.Add("rsync", DirErr, label+" "+remotePath+": "+err.Error())
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

func (b *RsyncBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	result, err := b.runDaemon(ctx, "LIST", []string{"-n"}, b.modulePath(relDir, true))
	if err != nil {
		return nil, err
	}
	entries := fileListEntries(relDir, result.FileList)
	Log.Add("rsync", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// PreloadRecursive fires one recursive dry run in the background and feeds
// the cache; List calls under scope then hit or wait on it.
func (b *RsyncBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

func (b *RsyncBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	result, err := b.runDaemon(ctx, "RLIST", []string{"-n", "-r"}, b.modulePath(scope, true))
	if err != nil {
		return err
	}
	emitFileList(scope, result.FileList, emit)
	Log.Add("rsync", DirIn, fmt.Sprintf("RLIST %d entries", len(result.FileList)))
	return nil
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
	result, err := b.runDaemon(ctx, "MD4", md4ListFlags(recursive), b.modulePath(scope, recursive))
	if err != nil {
		return nil, err
	}
	got := b.md4.fromFileList(scope, recursive, result.FileList)
	Log.Add("rsync", DirIn, fmt.Sprintf("MD4 %d checksums", len(got)))
	return got, nil
}

func (b *RsyncBackend) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return errors.New("rsync: set times not supported")
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

// push runs one recursive send of src to dest, crediting adder from the
// --progress output and settling it against budget afterwards.
func (b *RsyncBackend) push(ctx context.Context, label, src, dest string, adder *CappedAdder, budget int64) error {
	args := b.transferArgs("-r")
	var stdout io.Writer = io.Discard
	if adder != nil {
		stdout = &rsyncProgressWriter{adder: adder}
		args = append(args, "--progress")
	}
	Log.Add("rsync", DirOut, label+" ["+strings.Join(args, " ")+"]")
	err := b.rsyncRunStdout(ctx, stdout, append(args, src, dest)...)
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
	src, dest := srcPath, b.remoteURL(parentRel)+"/"
	if stageTop, link, perr := stageUploadPath(tmpDir, relPath); perr == nil && os.Link(srcPath, link) == nil {
		src, dest = stageTop, b.remoteURL("")+"/"
	} else if err := b.Mkdir(ctx, parentRel, 0o755); err != nil {
		return err
	}
	var adder *CappedAdder
	fileSize, _ := fileSizeFromContext(ctx)
	if counter := progressFromContext(ctx); counter != nil && fileSize > 0 {
		adder = NewCappedAdder(counter, fileSize)
	}
	err = b.push(ctx, "SEND "+srcPath+" -> "+relPath, src, dest, adder, fileSize)
	b.listCache.invalidateAncestors(relPath)
	b.md4.invalidate(relPath)
	return err
}

// RecvToLocalFile downloads directly to dstPath; --inplace reuses an existing
// prefix there for resume.
func (b *RsyncBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	return rsyncRecvToLocal(ctx, "rsync", relPath, dstPath, func(dstDir string) error {
		args := b.transferArgs(b.remoteURL(relPath), dstDir)
		Log.Add("rsync", DirOut, "RECV "+relPath+" -> "+dstPath)
		_, err := b.rsyncRun(ctx, args...)
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
	pushAdder, pushBudget, err := stageUpload(ctx, src, tmpFile, mode)
	if err != nil {
		return err
	}
	err = b.push(ctx, "SEND "+relPath, stageTop, b.remoteURL("")+"/", pushAdder, pushBudget)
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
	dirMode := mode.Perm()
	if dirMode == 0 {
		dirMode = 0755
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, filepath.FromSlash(clean)), dirMode); err != nil {
		return err
	}
	stageTop := filepath.Join(tmpDir, strings.SplitN(clean, "/", 2)[0])
	Log.Add("rsync", DirOut, "MKDIR "+clean)
	_, err = b.rsyncRun(ctx, "-r", "-t", stageTop, b.remoteURL("")+"/")
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("rsync", DirErr, err.Error())
	}
	return err
}

func (b *RsyncBackend) Rename(context.Context, string, string) error {
	return errors.New("rsync: rename not supported")
}

func rsyncFilterEscape(name string) string {
	if !strings.ContainsAny(name, "*?[") {
		return name
	}
	return strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`).Replace(name)
}

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
	tmpDir, err := os.MkdirTemp("", "rsync-remove-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	esc := rsyncFilterEscape(path.Base(clean))
	args := []string{"-r", "--delete", "--include=/" + esc}
	op := "REMOVE"
	if recursive {
		args = append(args, "--include=/"+esc+"/***")
		op = "REMOVEALL"
	}
	args = append(args, "--exclude=*", tmpDir+"/", b.remoteURL(parentDir(clean))+"/")
	Log.Add("rsync", DirOut, op+" "+clean)
	_, err = b.rsyncRun(ctx, args...)
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
	return rsyncOpenViaTemp(ctx, relPath, func(dstDir string) error {
		args := b.transferArgs(b.remoteURL(relPath), dstDir)
		Log.Add("rsync", DirOut, "RECV "+relPath)
		_, err := b.rsyncRun(ctx, args...)
		if err != nil {
			Log.Add("rsync", DirErr, err.Error())
		}
		return err
	})
}
