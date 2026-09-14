package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jlaffaye/ftp"

	"sc/model"
)

type FTPBackend struct {
	base    string
	conn    *ftp.ServerConn
	rawConn net.Conn
	hasher  *ftpHasher
	display string
	addr    string
	user    string
	pass    string
	scheme  string
	host    string
	port    string
	tlsCfg  *tls.Config
	pool    *sshPool[*ftpConn]
	mu      sync.Mutex
	dirs    sync.Map // directories known to exist, so uploads skip the MKD dance
}

// ftpConn is one extra control+data connection used for a parallel transfer.
// A single ftp.ServerConn is not safe for concurrent use (one data conn at a
// time), so each parallel slot gets its own. lastUsed gates a liveness probe
// on acquire; see acquireExtra.
type ftpConn struct {
	conn     *ftp.ServerConn
	rawConn  net.Conn
	lastUsed time.Time
}

func (c *ftpConn) close() {
	if c != nil && c.conn != nil {
		c.conn.Quit()
	}
}

// dialFTPConn opens one control connection: dial, login, binary mode. The
// dial hook meters every socket and adds the TLS the library would have: with
// a custom dialer jlaffaye skips its own implicit-TLS dial and hands out data
// connections before its TLS branch, so ftps would speak plaintext to port
// 990 and ftpes data channels would ignore PROT P. The returned net.Conn is
// the raw control socket, closed on ctx cancel to abort blocked transfers.
func dialFTPConn(addr, scheme, user, pass string, tlsCfg *tls.Config) (*ftp.ServerConn, net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var rawConn net.Conn
	var dials atomic.Int32
	dialFunc := func(network, address string) (net.Conn, error) {
		c, err := dialer.Dial(network, address)
		if err != nil {
			return nil, err
		}
		c = LimitConn(c)
		n := dials.Add(1)
		if n == 1 {
			rawConn = c
		}
		// Explicit TLS upgrades the control connection itself after AUTH TLS
		// and leaves only the data sockets to us.
		if scheme == "ftps" || (scheme == "ftpes" && n > 1) {
			c = tls.Client(c, tlsCfg)
		}
		return c, nil
	}
	opts := []ftp.DialOption{ftp.DialWithTimeout(10 * time.Second), ftp.DialWithDialFunc(dialFunc)}
	switch scheme {
	case "ftps":
		opts = append(opts, ftp.DialWithTLS(tlsCfg))
	case "ftpes":
		opts = append(opts, ftp.DialWithExplicitTLS(tlsCfg))
	}
	conn, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("ftp dial %s: %v", addr, err)
	}
	if err := conn.Login(user, pass); err != nil {
		conn.Quit()
		return nil, nil, fmt.Errorf("ftp login %s@%s: %v", user, addr, err)
	}
	if err := conn.Type(ftp.TransferTypeBinary); err != nil {
		conn.Quit()
		return nil, nil, fmt.Errorf("ftp binary mode: %v", err)
	}
	return conn, rawConn, nil
}

func NewFTPBackend(rawURL string, insecure bool, parallel int) (*FTPBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if user == "" {
		user = "anonymous"
	}
	if pass == "" && user == "anonymous" {
		pass = "sc@"
	}
	if port == "" {
		port = "21"
		if scheme == "ftps" {
			port = "990"
		}
	}
	tlsCfg := &tls.Config{ServerName: host, InsecureSkipVerify: insecure}
	addr := host + ":" + port

	conn, rawConn, err := dialFTPConn(addr, scheme, user, pass, tlsCfg)
	if err != nil {
		return nil, err
	}
	if remotePath == "/~" || strings.HasPrefix(remotePath, "/~/") {
		if wd, err := conn.CurrentDir(); err == nil {
			remotePath = path.Join(wd, strings.TrimPrefix(remotePath, "/~"))
		}
	}
	hasher, _ := newFTPHasher(host, port, user, pass, scheme, tlsCfg)

	displayHost := host
	if port != "21" && port != "990" {
		displayHost = addr
	}
	b := &FTPBackend{
		base:    remotePath,
		conn:    conn,
		rawConn: rawConn,
		hasher:  hasher,
		display: fmt.Sprintf("%s://%s@%s%s", scheme, user, displayHost, remotePath),
		addr:    addr,
		user:    user,
		pass:    pass,
		scheme:  scheme,
		host:    host,
		port:    port,
		tlsCfg:  tlsCfg,
	}

	// Extra connections for parallel transfers. Primary is nil: when the pool
	// is at capacity acquireExtra returns false and the caller serializes on
	// the primary conn under b.mu. maxExtras=0 (parallel<=1) means every
	// transfer takes the primary path.
	dial := func() (*ftpConn, error) {
		c, raw, derr := dialFTPConn(addr, scheme, user, pass, tlsCfg)
		if derr != nil {
			return nil, derr
		}
		Log.Add("ftp", "<<<", "extra connection dialed")
		return &ftpConn{conn: c, rawConn: raw, lastUsed: time.Now()}, nil
	}
	b.pool = newSSHPool[*ftpConn](nil, parallel-1, dial, func(c *ftpConn) { c.close() })
	return b, nil
}

// abortOnCancel returns a stop func that, while running, closes the raw
// control conn when ctx is canceled. Closing the underlying net.Conn
// unblocks blocked Stor/Retr/List/etc by failing the next read/write on
// the control conn. Data conns (which the library opens separately) get
// TCP keepalive via the shared Dialer, so dead peers there are detected
// at the OS level.
func (b *FTPBackend) abortOnCancel(ctx context.Context) func() {
	return cancelCloser(ctx, b.rawConn)
}

func (b *FTPBackend) BasePath() string { return b.display }

func (b *FTPBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pool.close()
	if b.hasher != nil {
		b.hasher.close()
	}
	return b.conn.Quit()
}

// acquireExtra returns a dedicated transfer connection and its release func,
// or ok=false when the pool is at capacity (caller must serialize on the
// primary conn under b.mu). Extras sit idle while the UI scans (which only
// uses the primary), so a long-idle extra may have been dropped by the
// server; probe with NoOp and redial before handing it out.
func (b *FTPBackend) acquireExtra(ctx context.Context) (*ftpConn, func(), bool) {
	c, poolRelease := b.pool.acquire()
	if c == nil {
		poolRelease()
		return nil, nil, false
	}
	// On cancel, cancelCloser force-closes this conn's control socket, so it
	// goes back dead. Zero lastUsed to force a liveness probe on next acquire
	// rather than handing out a corpse within the 60s trust window.
	release := func() {
		c.lastUsed = time.Now()
		if ctx.Err() != nil {
			c.lastUsed = time.Time{}
		}
		poolRelease()
	}
	if time.Since(c.lastUsed) > 60*time.Second && c.conn.NoOp() != nil {
		conn, raw, err := dialFTPConn(b.addr, b.scheme, b.user, b.pass, b.tlsCfg)
		if err != nil {
			Log.Add("ftp", "ERR", "extra reconnect failed: "+err.Error())
			c.lastUsed = time.Time{} // still dead: keep the probe armed
			poolRelease()
			return nil, nil, false
		}
		c.conn.Quit()
		c.conn, c.rawConn, c.lastUsed = conn, raw, time.Now()
		Log.Add("ftp", "<<<", "extra reconnected")
	}
	return c, release, true
}

func (b *FTPBackend) reconnectLocked() error {
	b.conn.Quit()
	conn, rawConn, err := dialFTPConn(b.addr, b.scheme, b.user, b.pass, b.tlsCfg)
	if err != nil {
		b.rawConn = nil
		return err
	}
	b.conn, b.rawConn = conn, rawConn
	if b.hasher != nil {
		algo := b.hasher.algo
		b.hasher.close()
		if b.hasher, _ = newFTPHasher(b.host, b.port, b.user, b.pass, b.scheme, b.tlsCfg); b.hasher != nil {
			b.hasher.algo = algo
		}
	}
	Log.Add("ftp", "<<<", "reconnected")
	return nil
}

// withPrimary runs op on the primary control connection under the lock and,
// when the connection turns out dead, reconnects and runs it once more. Only
// for ops that can safely repeat.
func (b *FTPBackend) withPrimary(ctx context.Context, op func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.abortOnCancel(ctx)()
	err := op()
	if err == nil || ctx.Err() != nil || b.conn.NoOp() == nil {
		return err
	}
	Log.Add("ftp", "ERR", "connection lost, reconnecting...")
	if rerr := b.reconnectLocked(); rerr != nil {
		Log.Add("ftp", "ERR", "reconnect failed: "+rerr.Error())
		return err
	}
	return op()
}

func (b *FTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	var result []model.FileEntry
	err := b.withPrimary(ctx, func() (err error) {
		result, err = b.listDirLocked(ctx, relDir)
		return err
	})
	return result, err
}

func (b *FTPBackend) listDirLocked(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	dir := path.Join(b.base, relDir)
	Log.Add("ftp", ">>>", "LIST "+dir)
	entries, err := b.conn.List(dir)
	if err == nil && len(entries) == 0 {
		// Some servers answer an empty 226 for a missing dir; CWD tells them apart.
		err = b.conn.ChangeDir(dir)
	}
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
		return nil, err
	}
	Log.Add("ftp", "<<<", fmt.Sprintf("%d entries", len(entries)))
	// MLSD already carries exact times; only fall back to one MDTM per entry
	// when the listing came from LIST.
	useMDTM := b.conn.IsGetTimeSupported() && !b.conn.IsTimePreciseInList()
	result := make([]model.FileEntry, 0, len(entries))
	for _, e := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if e.Name == "." || e.Name == ".." || e.Type == ftp.EntryTypeLink {
			continue
		}
		isDir := e.Type == ftp.EntryTypeFolder
		mode := os.FileMode(0644)
		if isDir {
			mode = os.ModeDir | 0755
		}
		modTime := e.Time
		if useMDTM {
			if t, err := b.conn.GetTime(path.Join(dir, e.Name)); err == nil {
				modTime = t
			}
		}
		result = append(result, model.FileEntry{
			RelPath: path.Join(relDir, e.Name),
			Name:    e.Name,
			Size:    int64(e.Size),
			ModTime: modTime,
			IsDir:   isDir,
			Mode:    mode,
		})
	}
	return result, nil
}

func (b *FTPBackend) currentHasher() *ftpHasher {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hasher
}

func (b *FTPBackend) Checksum(_ context.Context, relPath string) (string, error) {
	h := b.currentHasher()
	if h == nil {
		return "", errors.New("no checksum support on this FTP server")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hash(path.Join(b.base, relPath))
}

func (b *FTPBackend) ProbeChecksums() []string {
	h := b.currentHasher()
	if h == nil {
		return nil
	}
	var algos []string
	for _, a := range []string{"sha256", "sha1", "md5"} {
		if h.cmds[a] != "" {
			algos = append(algos, a)
		}
	}
	return algos
}

func (b *FTPBackend) SetChecksumAlgo(algo string) {
	if h := b.currentHasher(); h != nil {
		h.mu.Lock()
		h.algo = algo
		h.mu.Unlock()
	}
}

func (b *FTPBackend) SetTimes(ctx context.Context, relPath string, mtime, _, _ time.Time) error {
	err := b.withPrimary(ctx, func() error { return b.conn.SetTime(path.Join(b.base, relPath), mtime) })
	if err != nil {
		Log.Add("ftp", "ERR", "MFMT "+relPath+": "+err.Error())
	}
	return err
}

// store uploads src to relPath, appending at offset when it is positive, on
// an extra connection when one is free and on the locked primary otherwise.
// No retry: src is consumed.
func (b *FTPBackend) store(ctx context.Context, relPath string, src io.Reader, offset int64) error {
	fullPath := path.Join(b.base, relPath)
	if ex, release, ok := b.acquireExtra(ctx); ok {
		defer release()
		return b.storeOn(ctx, ex.conn, ex.rawConn, fullPath, src, offset)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.storeOn(ctx, b.conn, b.rawConn, fullPath, src, offset)
}

func (b *FTPBackend) storeOn(ctx context.Context, conn *ftp.ServerConn, raw net.Conn, fullPath string, src io.Reader, offset int64) error {
	defer cancelCloser(ctx, raw)()
	err := b.ensureDir(conn, path.Dir(fullPath))
	if err == nil && offset > 0 {
		err = conn.StorFrom(fullPath, src, uint64(offset))
	} else if err == nil {
		err = conn.Stor(fullPath, src)
	}
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
	}
	return err
}

func (b *FTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	Log.Add("ftp", ">>>", "STOR "+relPath)
	return b.store(ctx, relPath, src, 0)
}

func (b *FTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, _ os.FileMode, offset int64) error {
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	Log.Add("ftp", ">>>", fmt.Sprintf("STOR %s @%d", relPath, offset))
	return b.store(ctx, relPath, rd, offset)
}

func (b *FTPBackend) Mkdir(ctx context.Context, relPath string, _ os.FileMode) error {
	fullPath := path.Join(b.base, relPath)
	Log.Add("ftp", ">>>", "MKDIR "+fullPath)
	return b.withPrimary(ctx, func() error { return b.ensureDir(b.conn, fullPath) })
}

// ensureDir creates dir and any missing parents on conn, remembering what
// exists so a copy of many files into one directory costs one probe, not an
// MKD plus a listing per file. CWD is the existence test: a failed MKD cannot
// tell "already there" from "no parent".
func (b *FTPBackend) ensureDir(conn *ftp.ServerConn, dir string) error {
	if dir == "/" || dir == "." || dir == "" {
		return nil
	}
	if _, ok := b.dirs.Load(dir); ok {
		return nil
	}
	if conn.ChangeDir(dir) != nil {
		if err := b.ensureDir(conn, path.Dir(dir)); err != nil {
			return err
		}
		// A parallel creator may win the race; CWD settles it.
		if err := conn.MakeDir(dir); err != nil && conn.ChangeDir(dir) != nil {
			return err
		}
	}
	b.dirs.Store(dir, struct{}{})
	return nil
}

func (b *FTPBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	b.dirs.Clear()
	err := b.withPrimary(ctx, func() error {
		return b.conn.Rename(path.Join(b.base, oldRelPath), path.Join(b.base, newRelPath))
	})
	if err != nil {
		Log.Add("ftp", "ERR", "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) Remove(ctx context.Context, relPath string) error {
	err := b.withPrimary(ctx, func() error { return b.conn.Delete(path.Join(b.base, relPath)) })
	if err != nil {
		Log.Add("ftp", "ERR", "DELETE "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) RemoveAll(ctx context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("ftp: refusing to remove the base directory")
	}
	b.dirs.Clear()
	err := b.withPrimary(ctx, func() error { return b.removeAllLocked(path.Join(b.base, relPath)) })
	if err != nil {
		Log.Add("ftp", "ERR", "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) removeAllLocked(fullPath string) error {
	entries, err := b.conn.List(fullPath)
	if err != nil {
		return b.conn.Delete(fullPath)
	}
	for _, e := range entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		child := path.Join(fullPath, e.Name)
		if e.Type == ftp.EntryTypeFolder {
			err = b.removeAllLocked(child)
		} else {
			err = b.conn.Delete(child)
		}
		if err != nil {
			return err
		}
	}
	return b.conn.RemoveDir(fullPath)
}

func (b *FTPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.retr(ctx, relPath, 0)
}

func (b *FTPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	return b.retr(ctx, relPath, offset)
}

// retr opens a download on an extra connection when one is free; otherwise
// it holds the primary's lock until the reader is closed.
func (b *FTPBackend) retr(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	Log.Add("ftp", ">>>", fmt.Sprintf("RETR %s @%d", relPath, offset))
	fullPath := path.Join(b.base, relPath)
	if ex, release, ok := b.acquireExtra(ctx); ok {
		resp, err := retrFrom(ex.conn, fullPath, offset)
		if err != nil {
			release()
			Log.Add("ftp", "ERR", err.Error())
			return nil, err
		}
		return &ftpReader{rc: resp, stop: cancelCloser(ctx, ex.rawConn), done: release}, nil
	}
	b.mu.Lock()
	resp, err := retrFrom(b.conn, fullPath, offset)
	if err != nil {
		b.mu.Unlock()
		Log.Add("ftp", "ERR", err.Error())
		return nil, err
	}
	return &ftpReader{rc: resp, stop: b.abortOnCancel(ctx), done: b.mu.Unlock}, nil
}

func retrFrom(conn *ftp.ServerConn, fullPath string, offset int64) (*ftp.Response, error) {
	if offset > 0 {
		return conn.RetrFrom(fullPath, uint64(offset))
	}
	return conn.Retr(fullPath)
}

// ftpReader wraps a RETR body. Close stops the cancel watcher, closes the data
// stream and runs done (a pool release or the primary unlock) exactly once,
// since the copy layer may Close more than once.
type ftpReader struct {
	rc   io.ReadCloser
	stop func()
	done func()
	once sync.Once
	err  error
}

func (r *ftpReader) Read(p []byte) (int, error) { return r.rc.Read(p) }

func (r *ftpReader) Close() error {
	r.once.Do(func() {
		r.stop()
		r.err = r.rc.Close()
		r.done()
	})
	return r.err
}

// ftpHasher is a second control connection dedicated to XCRC/XSHA/HASH
// commands, so checksums never queue behind a transfer on the primary.
type ftpHasher struct {
	mu     sync.Mutex
	raw    net.Conn
	conn   io.Closer
	reader *textproto.Reader
	writer *textproto.Writer
	cmds   map[string]string
	algo   string
}

func newFTPHasher(host, port, user, pass, scheme string, tlsCfg *tls.Config) (*ftpHasher, error) {
	rawTCP, err := net.DialTimeout("tcp", host+":"+port, 10*time.Second)
	if err != nil {
		return nil, err
	}
	rawConn := LimitConn(rawTCP)
	var conn io.ReadWriteCloser = rawConn
	if scheme == "ftps" {
		conn = tls.Client(rawConn, tlsCfg)
	}
	h := &ftpHasher{raw: rawConn, conn: conn}
	h.setConn(conn)

	code, _, err := h.readResp()
	if err != nil || code/100 != 2 {
		conn.Close()
		return nil, fmt.Errorf("ftp hash conn welcome: %d %v", code, err)
	}
	if scheme == "ftpes" {
		code, _, err := h.sendCmd("AUTH TLS")
		if err != nil || code != 234 {
			conn.Close()
			return nil, fmt.Errorf("AUTH TLS: %d %v", code, err)
		}
		tlsConn := tls.Client(rawConn, tlsCfg)
		if err := tlsConn.Handshake(); err != nil {
			rawConn.Close()
			return nil, err
		}
		h.conn = tlsConn
		h.setConn(tlsConn)
	}

	code, _, err = h.sendCmd("USER %s", user)
	if err == nil && code == 331 {
		code, _, err = h.sendCmd("PASS %s", pass)
	}
	if err != nil || code != 230 {
		h.close()
		if err == nil {
			err = fmt.Errorf("ftp hash login: %d", code)
		}
		return nil, err
	}

	code, msg, _ := h.sendCmd("FEAT")
	h.cmds = make(map[string]string)
	if code == 211 {
		h.parseFeat(msg)
	}
	if len(h.cmds) == 0 {
		h.close()
		return nil, errors.New("no hash commands available")
	}
	return h, nil
}

func (h *ftpHasher) setConn(conn io.ReadWriter) {
	h.reader = textproto.NewReader(bufio.NewReader(conn))
	h.writer = textproto.NewWriter(bufio.NewWriter(conn))
}

func (h *ftpHasher) sendCmd(format string, args ...any) (int, string, error) {
	cmd := fmt.Sprintf(format, args...)
	if strings.HasPrefix(strings.ToUpper(cmd), "PASS ") {
		cmd = "PASS ***"
	}
	Log.Add("ftp", ">>>", cmd)
	if err := h.writer.PrintfLine(format, args...); err != nil {
		Log.Add("ftp", "ERR", err.Error())
		return 0, "", err
	}
	code, msg, err := h.readResp()
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
	} else {
		Log.Add("ftp", "<<<", fmt.Sprintf("%d %s", code, msg))
	}
	return code, msg, err
}

func (h *ftpHasher) readResp() (int, string, error) {
	return h.reader.ReadResponse(0)
}

// close says QUIT politely but with a deadline: it is often called precisely
// because the network is known to be dead.
func (h *ftpHasher) close() {
	h.raw.SetDeadline(time.Now().Add(2 * time.Second))
	h.sendCmd("QUIT")
	h.conn.Close()
}

var ftpAlgoName = map[string]string{
	"sha256": "SHA-256",
	"sha512": "SHA-512",
	"sha1":   "SHA-1",
	"md5":    "MD5",
}

// ftpFeatCmds maps FEAT lines to the algorithm they serve.
var ftpFeatCmds = map[string]string{"XSHA256": "sha256", "XSHA512": "sha512", "XSHA1": "sha1", "XMD5": "md5", "XCRC": "crc32"}

// ftpHashAlgos maps HASH algorithm names to sc's.
var ftpHashAlgos = map[string]string{"SHA-256": "sha256", "SHA-512": "sha512", "SHA-1": "sha1", "MD5": "md5"}

func (h *ftpHasher) parseFeat(msg string) {
	for _, line := range strings.Split(msg, "\n") {
		upper := strings.ToUpper(strings.TrimSpace(line))
		if algo, ok := ftpFeatCmds[upper]; ok {
			h.cmds[algo] = upper
			continue
		}
		if !strings.HasPrefix(upper, "HASH ") {
			continue
		}
		for _, a := range strings.Split(upper[5:], ";") {
			algo, ok := ftpHashAlgos[strings.TrimSpace(strings.TrimRight(a, "*"))]
			if ok && h.cmds[algo] == "" {
				h.cmds[algo] = "HASH"
			}
		}
	}
}

func (h *ftpHasher) hash(fullPath string) (string, error) {
	cmd := h.cmds[h.algo]
	if cmd == "" {
		return "", fmt.Errorf("no hash command for %q", h.algo)
	}
	if cmd == "HASH" {
		if name := ftpAlgoName[h.algo]; name != "" {
			h.sendCmd("OPTS HASH %s", name)
		}
		code, msg, err := h.sendCmd("HASH %s", fullPath)
		if err != nil {
			return "", err
		}
		if code != 213 {
			return "", fmt.Errorf("HASH: %d %s", code, msg)
		}
		fields := strings.Fields(msg)
		if len(fields) < 3 {
			return "", fmt.Errorf("invalid HASH response: %s", msg)
		}
		return strings.ToLower(fields[2]), nil
	}
	code, msg, err := h.sendCmd("%s %s", cmd, fullPath)
	if err != nil {
		return "", err
	}
	if code != 213 {
		return "", fmt.Errorf("%s: %d %s", cmd, code, msg)
	}
	return strings.ToLower(strings.TrimSpace(msg)), nil
}
