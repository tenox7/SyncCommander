package transport

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"
	"github.com/gokrazy/rsync/rsynccmd"

	"sc/model"
)

// rsyncTransferFlags are passed to every rsync invocation that moves file
// data. -t preserves mtime; --inplace writes directly to the destination so
// resume works at the rsync protocol level; --partial keeps interrupted files
// for the next run. Delta-sync is the default for remote transfers. Callers
// that know the destination has no file to delta against can attach
// ContextWithWholeFile to the ctx, which adds -W per call.
var rsyncTransferFlags = []string{"-t", "--inplace", "--partial"}

// rsyncFlagsForCtx returns rsyncTransferFlags plus -W if the ctx carries the
// whole-file hint. Returns a fresh slice; safe to append to.
func rsyncFlagsForCtx(ctx context.Context) []string {
	args := append([]string{}, rsyncTransferFlags...)
	if wholeFileFromContext(ctx) {
		args = append(args, "-W")
	}
	return args
}

type RsyncBackend struct {
	host        string
	user        string
	pass        string
	module      string
	base        string
	display     string
	useChecksum bool
	cksumAlgo   string
	md4mu       sync.Mutex
	md4cache    map[string]string
	listCache   *rsyncListCache
}

// rsyncListCache stores per-directory entries populated by an in-flight
// recursive listing call. List() consults this cache and waits for the dir
// to land before falling back to a live per-dir listing. preloadCtx is the
// outer scan context used by await, so per-call stall timeouts on List()
// don't prematurely abandon the cache when the remote is slow.
type rsyncListCache struct {
	mu         sync.Mutex
	cond       *sync.Cond
	entries    map[string][]model.FileEntry
	scope      string
	active     bool
	done       bool
	preloadCtx context.Context
}

func newRsyncListCache() *rsyncListCache {
	c := &rsyncListCache{entries: make(map[string][]model.FileEntry)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *rsyncListCache) reset(ctx context.Context, scope string) {
	c.mu.Lock()
	c.entries = make(map[string][]model.FileEntry)
	c.scope = scope
	c.active = true
	c.done = false
	c.preloadCtx = ctx
	c.mu.Unlock()
}

// emit appends entries for parent. We deliberately do NOT broadcast here:
// rsync's recursive output revisits the same parent multiple times (e.g.
// after descending into bin/, it returns to the top level for contrib/),
// so any single emit can be a partial view. Only finish() broadcasts; List
// awaiters block until the full listing is in.
func (c *rsyncListCache) emit(parent string, entries []model.FileEntry) {
	c.mu.Lock()
	c.entries[parent] = append(c.entries[parent], entries...)
	c.mu.Unlock()
}

func (c *rsyncListCache) finish() {
	c.mu.Lock()
	c.done = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *rsyncListCache) lookup(relDir string) (entries []model.FileEntry, hit, active, done bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, hit = c.entries[relDir]
	active = c.active
	done = c.done
	return
}

// await blocks until the preload finishes (or the preload context is
// canceled), then returns whatever is in the cache for relDir. We do NOT
// return on first cache hit because rsync's recursive output interleaves
// entries from different parents — a partial cache view is unsafe to act
// on. Intentionally ignores the caller's per-call ctx: that ctx typically
// carries a per-list stall timeout (~120s) which is unsuited to a slow
// remote where one recursive listing legitimately takes minutes.
func (c *rsyncListCache) await(relDir string) ([]model.FileEntry, bool) {
	c.mu.Lock()
	pctx := c.preloadCtx
	c.mu.Unlock()

	notify := make(chan struct{})
	if pctx != nil {
		go func() {
			select {
			case <-pctx.Done():
				c.mu.Lock()
				c.cond.Broadcast()
				c.mu.Unlock()
			case <-notify:
			}
		}()
	}
	c.mu.Lock()
	for !c.done && (pctx == nil || pctx.Err() == nil) {
		c.cond.Wait()
	}
	entries, ok := c.entries[relDir]
	c.mu.Unlock()
	close(notify)
	return entries, ok
}

func NewRsyncBackend(rawURL string) (*RsyncBackend, error) {
	_, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if port == "" {
		port = "873"
	}

	p := strings.TrimPrefix(remotePath, "/")
	parts := strings.SplitN(p, "/", 2)
	module := parts[0]
	if module == "" {
		return nil, fmt.Errorf("rsync: module name required in URL")
	}
	base := ""
	if len(parts) > 1 {
		base = strings.Trim(parts[1], "/")
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
		host:    net.JoinHostPort(host, port),
		user:    user,
		pass:    pass,
		module:  module,
		base:    base,
		display: display,
	}, nil
}

func (b *RsyncBackend) BasePath() string { return b.display }

func (b *RsyncBackend) OwnsCopyProgress() bool { return true }

func (b *RsyncBackend) remoteURL(relPath string) string {
	p := b.module
	if b.base != "" {
		p = p + "/" + b.base
	}
	if relPath != "" {
		p = p + "/" + relPath
	}
	hostPart := b.host
	if b.user != "" {
		userinfo := url.PathEscape(b.user)
		if b.pass != "" {
			userinfo += ":" + url.PathEscape(b.pass)
		}
		hostPart = userinfo + "@" + b.host
	}
	return "rsync://" + hostPart + "/" + p
}

func (b *RsyncBackend) rsyncRun(ctx context.Context, args ...string) (string, error) {
	cmd := rsynccmd.Command("rsync", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	_, err := cmd.Run(ctx)
	if err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg != "" {
			return "", fmt.Errorf("%v: %s", err, errMsg)
		}
		return "", err
	}
	return stdout.String(), nil
}

func (b *RsyncBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if b.listCache != nil {
		entries, hit, active, done := b.listCache.lookup(relDir)
		if hit {
			return entries, nil
		}
		if active && !done {
			if e, ok := b.listCache.await(relDir); ok {
				return e, nil
			}
		}
	}
	return b.liveList(ctx, relDir)
}

func (b *RsyncBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	u := b.remoteURL(relDir) + "/"
	Log.Add("rsync", ">>>", "LIST "+u)
	args := []string{}
	if b.useChecksum {
		args = append(args, "-c")
	}
	args = append(args, u)
	out, err := b.rsyncRun(ctx, args...)
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
		return nil, err
	}

	var entries []model.FileEntry
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		entry, ok := parseRsyncListLine(line, relDir)
		if !ok || entry.Name == "." || entry.Name == ".." {
			continue
		}
		entries = append(entries, entry)
	}
	Log.Add("rsync", "<<<", fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// PreloadRecursive fires a single rsync -r listing in the background,
// streaming entries into the cache. Returns immediately. Subsequent List
// calls under scope hit or wait on the cache. A second preload while one is
// in flight is a no-op.
func (b *RsyncBackend) PreloadRecursive(ctx context.Context, scope string) error {
	if b.listCache == nil {
		b.listCache = newRsyncListCache()
	}
	c := b.listCache
	c.mu.Lock()
	if c.active && !c.done {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	c.reset(ctx, scope)

	go func() {
		_ = b.runRecursiveList(ctx, scope, c.emit)
		c.finish()
	}()
	return nil
}

func (b *RsyncBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	u := b.remoteURL(scope) + "/"
	Log.Add("rsync", ">>>", "RLIST "+u)

	var current string
	var haveCurrent bool
	var batch []model.FileEntry
	var seenDirs []string
	flush := func() {
		if !haveCurrent {
			return
		}
		emit(current, batch)
		batch = nil
		haveCurrent = false
	}

	lw := newLineWriter(func(line string) {
		line = strings.TrimSpace(line)
		if line == "" {
			return
		}
		parent, entry, ok := parseRsyncRecursiveLine(line, scope)
		if !ok {
			return
		}
		if !haveCurrent || parent != current {
			flush()
			current = parent
			haveCurrent = true
		}
		batch = append(batch, entry)
		if entry.IsDir {
			seenDirs = append(seenDirs, entry.RelPath)
		}
	})

	args := []string{"-r"}
	if b.useChecksum {
		args = append(args, "-c")
	}
	args = append(args, u)
	cmd := rsynccmd.Command("rsync", args...)
	var stderr bytes.Buffer
	cmd.Stdout = lw
	cmd.Stderr = &stderr
	_, err := cmd.Run(ctx)
	lw.flush()
	flush()
	// Touch every dir we saw so the cache reports a hit (with no children)
	// instead of falling through to a per-dir live LIST. Without this,
	// every empty leaf dir costs a fresh TCP+handshake round trip.
	for _, d := range seenDirs {
		emit(d, nil)
	}
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			err = fmt.Errorf("%v: %s", err, msg)
		}
		Log.Add("rsync", "ERR", "RLIST: "+err.Error())
		return err
	}
	Log.Add("rsync", "<<<", "RLIST done")
	return nil
}

// lineWriter buffers stdout bytes and invokes cb for each complete '\n'-
// terminated line. Used to stream-parse rsync recursive listings as they
// arrive instead of buffering the full output.
type lineWriter struct {
	buf []byte
	cb  func(string)
}

func newLineWriter(cb func(string)) *lineWriter {
	return &lineWriter{cb: cb}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.cb(string(w.buf[:i]))
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	if len(w.buf) > 0 {
		w.cb(string(w.buf))
		w.buf = nil
	}
}

// parseRsyncRecursiveLine parses one line of `rsync -r URL/scope/` output.
// Each entry's path is relative to scope; we return the parent dir relative
// to the backend's base (i.e. path.Join(scope, path.Dir(name))).
func parseRsyncRecursiveLine(line, scope string) (parentRel string, entry model.FileEntry, ok bool) {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return "", model.FileEntry{}, false
	}
	modeStr := fields[0]
	if len(modeStr) > 0 && modeStr[0] == 'l' {
		return "", model.FileEntry{}, false
	}
	size, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return "", model.FileEntry{}, false
	}
	modTime, err := time.ParseInLocation("2006/01/02 15:04:05", fields[2]+" "+fields[3], time.Local)
	if err != nil {
		return "", model.FileEntry{}, false
	}
	nameIdx := strings.Index(line, fields[3]) + len(fields[3])
	if nameIdx >= len(line) {
		return "", model.FileEntry{}, false
	}
	full := strings.TrimLeft(line[nameIdx:], " ")
	if full == "." || full == ".." {
		return "", model.FileEntry{}, false
	}
	parent := path.Dir(full)
	if parent == "." {
		parent = ""
	}
	base := path.Base(full)
	isDir := len(modeStr) > 0 && modeStr[0] == 'd'

	parentRel = path.Join(scope, parent)

	return parentRel, model.FileEntry{
		RelPath: path.Join(scope, full),
		Name:    base,
		Size:    size,
		ModTime: modTime,
		IsDir:   isDir,
		Mode:    parseRsyncMode(modeStr),
	}, true
}

func parseRsyncListLine(line, relDir string) (model.FileEntry, bool) {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return model.FileEntry{}, false
	}
	modeStr := fields[0]
	size, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return model.FileEntry{}, false
	}
	modTime, err := time.ParseInLocation("2006/01/02 15:04:05", fields[2]+" "+fields[3], time.Local)
	if err != nil {
		return model.FileEntry{}, false
	}
	nameIdx := strings.Index(line, fields[3]) + len(fields[3])
	if nameIdx >= len(line) {
		return model.FileEntry{}, false
	}
	name := strings.TrimLeft(line[nameIdx:], " ")
	if len(modeStr) > 0 && modeStr[0] == 'l' {
		return model.FileEntry{}, false
	}
	isDir := len(modeStr) > 0 && modeStr[0] == 'd'

	return model.FileEntry{
		RelPath: path.Join(relDir, name),
		Name:    name,
		Size:    size,
		ModTime: modTime,
		IsDir:   isDir,
		Mode:    parseRsyncMode(modeStr),
	}, true
}

func parseRsyncMode(s string) os.FileMode {
	if len(s) < 10 {
		return 0644
	}
	var mode os.FileMode
	if s[0] == 'd' {
		mode |= os.ModeDir
	}
	if s[0] == 'l' {
		mode |= os.ModeSymlink
	}
	bits := [9]os.FileMode{0400, 0200, 0100, 0040, 0020, 0010, 0004, 0002, 0001}
	for i, b := range bits {
		if s[1+i] != '-' {
			mode |= b
		}
	}
	return mode
}

func (b *RsyncBackend) ProbeChecksums() []string {
	return []string{"md4", "rsync"}
}

func (b *RsyncBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
	b.useChecksum = algo == "rsync" || algo == "md4"
}

func (b *RsyncBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "md4" {
		b.md4mu.Lock()
		sum, ok := b.md4cache[relPath]
		b.md4mu.Unlock()
		if ok {
			return sum, nil
		}
		got, err := b.fetchMD4(ctx, relPath, false)
		if err != nil {
			return "", err
		}
		sum, ok = got[relPath]
		if !ok {
			return "", fmt.Errorf("md4: no checksum for %s", relPath)
		}
		return sum, nil
	}
	if !b.useChecksum {
		return "", fmt.Errorf("rsync: checksum not enabled")
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
	tmpDir, err := os.MkdirTemp("", "rsync-md4-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	args := []string{"-c"}
	if recursive {
		args = append(args, "-r")
	}
	client, err := rsyncclient.New(args, rsyncclient.WithStderr(io.Discard), rsyncclient.WithoutNegotiate(), rsyncclient.DontRestrict())
	if err != nil {
		return nil, err
	}

	remotePath := b.module + "/"
	if b.base != "" {
		remotePath += b.base + "/"
	}
	if scope != "" {
		remotePath += scope
		if recursive {
			remotePath += "/"
		}
	}

	if b.user != "" {
		os.Setenv("RSYNC_USERNAME", b.user)
	}
	if b.pass != "" {
		os.Setenv("RSYNC_PASSWORD", b.pass)
	}

	conn, err := net.DialTimeout("tcp", b.host, 30*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	Log.Add("rsync", ">>>", "MD4 "+remotePath)
	result, err := client.RunDaemon(ctx, conn, remotePath, []string{tmpDir + "/"})
	if err != nil {
		return nil, err
	}

	prefix := ""
	if recursive {
		prefix = scope
	} else if scope != "" {
		prefix = path.Dir(scope)
		if prefix == "." {
			prefix = ""
		}
	}

	got := make(map[string]string, len(result.FileList))
	var zero [16]byte
	for _, fi := range result.FileList {
		if fi.Checksum == zero {
			continue
		}
		key := fi.Name
		if prefix != "" {
			key = path.Join(prefix, fi.Name)
		}
		got[key] = hex.EncodeToString(fi.Checksum[:])
	}
	Log.Add("rsync", "<<<", fmt.Sprintf("MD4 %d checksums", len(got)))

	b.md4mu.Lock()
	if b.md4cache == nil {
		b.md4cache = make(map[string]string, len(got))
	}
	for k, v := range got {
		b.md4cache[k] = v
	}
	b.md4mu.Unlock()
	return got, nil
}

func (b *RsyncBackend) SetTimes(_ context.Context, _ string, _, _, _ time.Time) error {
	return fmt.Errorf("rsync: set times not supported")
}

// SendLocalFile sends an existing local file directly to the rsync daemon
// without writing to a tmp dir first. With rsyncTransferFlags it does delta
// sync against any partial data already at dst.
func (b *RsyncBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, _ os.FileMode) error {
	dest := b.remoteURL(path.Dir(relPath)) + "/"
	args := rsyncFlagsForCtx(ctx)
	if b.useChecksum {
		args = append(args, "-c")
	}
	args = append(args, srcPath, dest)
	Log.Add("rsync", ">>>", "SEND "+srcPath+" -> "+relPath+" ["+strings.Join(args[:len(args)-2], " ")+"]")
	_, err := b.rsyncRun(ctx, args...)
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
	}
	return err
}

// RecvToLocalFile downloads a file from the rsync daemon directly to a local
// path, no tmp dir. With --inplace any existing prefix at dstPath is reused
// for delta sync (resume).
func (b *RsyncBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	parent := filepath.Dir(dstPath)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	if fi, err := os.Stat(dstPath); err == nil && fi.IsDir() {
		return fmt.Errorf("rsync: refuse to receive into existing directory %s", dstPath)
	}

	counter := progressFromContext(ctx)
	base := baseProgressFromContext(ctx)
	var stop chan struct{}
	if counter != nil {
		size := fileSizeFromContext(ctx)
		if size <= 0 {
			size = 1 << 62
		}
		var baseAdder *CappedAdder
		if base != nil {
			baseAdder = NewCappedAdder(base, size)
		}
		stop = make(chan struct{})
		go tailDirSize(stop, parent, filepath.Base(dstPath), NewCappedAdder(counter, size), baseAdder)
	}

	u := b.remoteURL(relPath)
	args := rsyncFlagsForCtx(ctx)
	if b.useChecksum {
		args = append(args, "-c")
	}
	args = append(args, u, parent+"/")
	Log.Add("rsync", ">>>", "RECV "+relPath+" -> "+dstPath+" ["+strings.Join(args[:len(args)-2], " ")+"]")
	_, err := b.rsyncRun(ctx, args...)
	if stop != nil {
		close(stop)
	}
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
		return err
	}
	if fi, statErr := os.Stat(dstPath); statErr != nil {
		Log.Add("rsync", "ERR", "RECV "+relPath+": dst missing after rsync: "+statErr.Error())
		return fmt.Errorf("rsync: dst missing after recv: %w", statErr)
	} else if fi.IsDir() {
		Log.Add("rsync", "ERR", "RECV "+relPath+": dst is a directory after rsync (rsync misbehavior)")
		return fmt.Errorf("rsync: dst became a directory after recv: %s", dstPath)
	} else {
		Log.Add("rsync", "<<<", fmt.Sprintf("RECV %s OK (%d bytes)", relPath, fi.Size()))
	}
	return nil
}

func (b *RsyncBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	tmpDir, err := os.MkdirTemp("", "rsync-upload-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	tmpFile := filepath.Join(tmpDir, filepath.Base(relPath))
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	counter := progressFromContext(ctx)
	fileSize := fileSizeFromContext(ctx)
	var copyDst io.Writer = f
	if counter != nil && fileSize > 0 && !IsPreCounted(src) {
		copyDst = &CountingWriter{W: f, Adder: NewCappedAdder(counter, fileSize)}
	}
	if _, err := io.Copy(copyDst, src); err != nil {
		f.Close()
		return err
	}
	f.Close()

	dest := b.remoteURL(path.Dir(relPath)) + "/"
	sendArgs := rsyncFlagsForCtx(ctx)
	if b.useChecksum {
		sendArgs = append(sendArgs, "-c")
	}
	sendArgs = append(sendArgs, tmpFile, dest)
	Log.Add("rsync", ">>>", "SEND "+relPath+" ["+strings.Join(sendArgs[:len(sendArgs)-2], " ")+"]")
	_, err = b.rsyncRun(ctx, sendArgs...)
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
		return err
	}
	Log.Add("rsync", "<<<", "OK")
	return nil
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
	stageLeaf := filepath.Join(tmpDir, filepath.FromSlash(clean))
	if err := os.MkdirAll(stageLeaf, dirMode); err != nil {
		return err
	}

	topLeaf := strings.SplitN(clean, "/", 2)[0]
	stageTop := filepath.Join(tmpDir, topLeaf)
	dest := b.remoteURL("") + "/"
	args := []string{"-r", "-t", stageTop, dest}
	Log.Add("rsync", ">>>", "MKDIR "+clean+" ["+strings.Join(args[:len(args)-2], " ")+"]")
	_, err = b.rsyncRun(ctx, args...)
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
	} else {
		Log.Add("rsync", "<<<", "MKDIR "+clean+" OK")
	}
	return err
}

func (b *RsyncBackend) Rename(_ context.Context, _, _ string) error {
	return fmt.Errorf("rsync: rename not supported")
}

func (b *RsyncBackend) Remove(_ context.Context, _ string) error {
	return fmt.Errorf("rsync: remove not supported")
}

func (b *RsyncBackend) RemoveAll(_ context.Context, _ string) error {
	return fmt.Errorf("rsync: remove not supported")
}

func (b *RsyncBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-download-*")
	if err != nil {
		return nil, err
	}

	counter := progressFromContext(ctx)
	base := baseProgressFromContext(ctx)
	var stop chan struct{}
	if counter != nil {
		size := fileSizeFromContext(ctx)
		if size <= 0 {
			size = 1 << 62
		}
		var baseAdder *CappedAdder
		if base != nil {
			baseAdder = NewCappedAdder(base, size)
		}
		stop = make(chan struct{})
		go tailDirSize(stop, tmpDir, filepath.Base(relPath), NewCappedAdder(counter, size), baseAdder)
	}

	u := b.remoteURL(relPath)
	recvArgs := rsyncFlagsForCtx(ctx)
	if b.useChecksum {
		recvArgs = append(recvArgs, "-c")
	}
	recvArgs = append(recvArgs, u, tmpDir+"/")
	Log.Add("rsync", ">>>", "RECV "+relPath+" ["+strings.Join(recvArgs[:len(recvArgs)-2], " ")+"]")
	_, err = b.rsyncRun(ctx, recvArgs...)
	if stop != nil {
		close(stop)
	}
	if err != nil {
		os.RemoveAll(tmpDir)
		Log.Add("rsync", "ERR", err.Error())
		return nil, err
	}
	Log.Add("rsync", "<<<", "OK")

	f, err := os.Open(filepath.Join(tmpDir, filepath.Base(relPath)))
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	rc := &tempReadCloser{File: f, tmpDir: tmpDir}
	if counter != nil {
		return WrapPreCounted(rc), nil
	}
	return rc, nil
}

type tempReadCloser struct {
	*os.File
	tmpDir string
}

func (t *tempReadCloser) Close() error {
	t.File.Close()
	return os.RemoveAll(t.tmpDir)
}
