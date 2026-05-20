package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"path"
	"strings"
	"sync"
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
}

// ftpConn is one extra control+data connection used for a parallel transfer.
// A single ftp.ServerConn is not safe for concurrent use (one data conn at a
// time), so each parallel slot gets its own. lastUsed gates a liveness probe
// on acquire — see acquireExtra.
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
// returned net.Conn is the raw control socket, closed on ctx cancel to abort
// blocked transfers (see abortOnCancel).
func dialFTPConn(addr, scheme, user, pass string, tlsCfg *tls.Config) (*ftp.ServerConn, net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var rawConn net.Conn
	dialFunc := func(network, address string) (net.Conn, error) {
		c, err := dialer.Dial(network, address)
		if err == nil {
			rawConn = c
		}
		return c, err
	}
	opts := []ftp.DialOption{
		ftp.DialWithTimeout(10 * time.Second),
		ftp.DialWithDialer(*dialer),
		ftp.DialWithDialFunc(dialFunc),
	}
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

	tlsCfg := &tls.Config{ServerName: host, InsecureSkipVerify: insecure}

	switch scheme {
	case "ftps":
		if port == "" {
			port = "990"
		}
	case "ftpes":
		if port == "" {
			port = "21"
		}
	default:
		if port == "" {
			port = "21"
		}
	}

	addr := host + ":" + port

	// dialFTPConn captures the raw control conn so we can close it on
	// cancellation, and propagates Dialer.KeepAlive to both control and data
	// conns (the library reuses the Dialer for data conns) so TCP keepalive
	// detects dead peers.
	conn, rawConn, err := dialFTPConn(addr, scheme, user, pass, tlsCfg)
	if err != nil {
		return nil, err
	}

	if remotePath == "/~" || strings.HasPrefix(remotePath, "/~/") {
		if wd, err := conn.CurrentDir(); err == nil {
			if remotePath == "/~" {
				remotePath = wd
			} else {
				remotePath = path.Join(wd, remotePath[3:])
			}
		}
	}

	hasher, _ := newFTPHasher(host, port, user, pass, scheme, tlsCfg)

	displayHost := host
	if port != "21" && port != "990" {
		displayHost = host + ":" + port
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
	// transfer takes the primary path — identical to the pre-pool behavior.
	maxExtras := parallel - 1
	dial := func() (*ftpConn, error) {
		c, raw, derr := dialFTPConn(addr, scheme, user, pass, tlsCfg)
		if derr != nil {
			return nil, derr
		}
		Log.Add("ftp", "<<<", "extra connection dialed")
		return &ftpConn{conn: c, rawConn: raw, lastUsed: time.Now()}, nil
	}
	b.pool = newSSHPool[*ftpConn](nil, maxExtras, dial, func(c *ftpConn) { c.close() })

	return b, nil
}

// abortOnCancel returns a stop func that, while running, closes the raw
// control conn when ctx is canceled. Closing the underlying net.Conn
// unblocks blocked Stor/Retr/List/etc by failing the next read/write on
// the control conn. Data conns (which the library opens separately) get
// TCP keepalive via the shared Dialer, so dead peers there are detected
// at the OS level.
func (b *FTPBackend) abortOnCancel(ctx context.Context) func() {
	if ctx == nil || b.rawConn == nil {
		return func() {}
	}
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
// server; probe with NoOp and redial before handing it out. SFTP needs no
// such probe — its multiplexed client keeps the transport alive.
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
		if ctx.Err() != nil {
			c.lastUsed = time.Time{}
		} else {
			c.lastUsed = time.Now()
		}
		poolRelease()
	}
	if time.Since(c.lastUsed) > 60*time.Second && c.conn.NoOp() != nil {
		conn, raw, err := dialFTPConn(b.addr, b.scheme, b.user, b.pass, b.tlsCfg)
		if err != nil {
			Log.Add("ftp", "ERR", "extra reconnect failed: "+err.Error())
			release()
			return nil, nil, false
		}
		c.conn.Quit()
		c.conn = conn
		c.rawConn = raw
		c.lastUsed = time.Now()
		Log.Add("ftp", "<<<", "extra reconnected")
	}
	return c, release, true
}

func (b *FTPBackend) connAliveLocked() bool {
	return b.conn.NoOp() == nil
}

func (b *FTPBackend) reconnectLocked() error {
	b.conn.Quit()
	conn, rawConn, err := dialFTPConn(b.addr, b.scheme, b.user, b.pass, b.tlsCfg)
	if err != nil {
		b.rawConn = nil
		return err
	}
	b.conn = conn
	b.rawConn = rawConn
	if b.hasher != nil {
		algo := b.hasher.algo
		b.hasher.close()
		b.hasher, _ = newFTPHasher(b.host, b.port, b.user, b.pass, b.scheme, b.tlsCfg)
		if b.hasher != nil {
			b.hasher.algo = algo
		}
	}
	Log.Add("ftp", "<<<", "reconnected")
	return nil
}

func (b *FTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.abortOnCancel(ctx)()
	result, err := b.listDirLocked(ctx, relDir)
	if err != nil && ctx.Err() == nil && !b.connAliveLocked() {
		Log.Add("ftp", "ERR", "connection lost, reconnecting...")
		if rerr := b.reconnectLocked(); rerr != nil {
			Log.Add("ftp", "ERR", "reconnect failed: "+rerr.Error())
			return nil, err
		}
		return b.listDirLocked(ctx, relDir)
	}
	return result, err
}

func (b *FTPBackend) listDirLocked(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	dir := path.Join(b.base, relDir)
	Log.Add("ftp", ">>>", "LIST "+dir)
	entries, err := b.conn.List(dir)
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
		return nil, err
	}
	Log.Add("ftp", "<<<", fmt.Sprintf("%d entries", len(entries)))
	useMDTM := b.conn.IsGetTimeSupported()
	result := make([]model.FileEntry, 0, len(entries))
	for _, e := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if e.Name == "." || e.Name == ".." {
			continue
		}
		if e.Type == ftp.EntryTypeLink {
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

func (b *FTPBackend) Checksum(_ context.Context, relPath string) (string, error) {
	if b.hasher == nil || b.hasher.algo == "" {
		return "", fmt.Errorf("no checksum support on this FTP server")
	}
	b.hasher.mu.Lock()
	defer b.hasher.mu.Unlock()
	return b.hasher.hash(path.Join(b.base, relPath))
}

func (b *FTPBackend) ProbeChecksums() []string {
	if b.hasher == nil {
		return nil
	}
	var algos []string
	for _, a := range []string{"sha256", "sha1", "md5"} {
		if b.hasher.cmds[a] != "" {
			algos = append(algos, a)
		}
	}
	return algos
}

func (b *FTPBackend) SetChecksumAlgo(algo string) {
	if b.hasher != nil {
		b.hasher.algo = algo
	}
}

func (b *FTPBackend) SetTimes(_ context.Context, relPath string, mtime, _, _ time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.conn.SetTime(path.Join(b.base, relPath), mtime)
	if err != nil {
		Log.Add("ftp", "ERR", "MFMT "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	Log.Add("ftp", ">>>", "STOR "+relPath)
	fullPath := path.Join(b.base, relPath)
	if ex, release, ok := b.acquireExtra(ctx); ok {
		defer release()
		defer cancelCloser(ctx, ex.rawConn)()
		mkdirAllOn(ex.conn, path.Dir(fullPath))
		err := ex.conn.Stor(fullPath, src)
		if err != nil {
			Log.Add("ftp", "ERR", err.Error())
		}
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.abortOnCancel(ctx)()
	b.mkdirAllLocked(path.Dir(fullPath))
	err := b.conn.Stor(fullPath, src)
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
	}
	return err
}

func (b *FTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, _ os.FileMode, offset int64) error {
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	Log.Add("ftp", ">>>", fmt.Sprintf("STOR %s @%d", relPath, offset))
	fullPath := path.Join(b.base, relPath)
	if ex, release, ok := b.acquireExtra(ctx); ok {
		defer release()
		defer cancelCloser(ctx, ex.rawConn)()
		mkdirAllOn(ex.conn, path.Dir(fullPath))
		err = ex.conn.StorFrom(fullPath, rd, uint64(offset))
		if err != nil {
			Log.Add("ftp", "ERR", err.Error())
		}
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.abortOnCancel(ctx)()
	b.mkdirAllLocked(path.Dir(fullPath))
	err = b.conn.StorFrom(fullPath, rd, uint64(offset))
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
	}
	return err
}

func (b *FTPBackend) Mkdir(_ context.Context, relPath string, _ os.FileMode) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	fullPath := path.Join(b.base, relPath)
	Log.Add("ftp", ">>>", "MKDIR "+fullPath)
	b.mkdirAllLocked(fullPath)
	return nil
}

func (b *FTPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.conn.Rename(path.Join(b.base, oldRelPath), path.Join(b.base, newRelPath))
	if err != nil {
		Log.Add("ftp", "ERR", "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) Remove(_ context.Context, relPath string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.conn.Delete(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("ftp", "ERR", "DELETE "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) RemoveAll(_ context.Context, relPath string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.removeAllLocked(path.Join(b.base, relPath))
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
			if err := b.removeAllLocked(child); err != nil {
				return err
			}
			continue
		}
		if err := b.conn.Delete(child); err != nil {
			return err
		}
	}
	return b.conn.RemoveDirRecur(fullPath)
}

func (b *FTPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	if ex, release, ok := b.acquireExtra(ctx); ok {
		Log.Add("ftp", ">>>", "RETR "+relPath)
		resp, err := ex.conn.Retr(path.Join(b.base, relPath))
		if err != nil {
			release()
			Log.Add("ftp", "ERR", err.Error())
			return nil, err
		}
		return &ftpPooledReader{rc: resp, release: release, stop: cancelCloser(ctx, ex.rawConn)}, nil
	}
	b.mu.Lock()
	Log.Add("ftp", ">>>", "RETR "+relPath)
	resp, err := b.conn.Retr(path.Join(b.base, relPath))
	if err != nil {
		b.mu.Unlock()
		Log.Add("ftp", "ERR", err.Error())
		return nil, err
	}
	stop := b.abortOnCancel(ctx)
	return &lockedFTPReader{rc: resp, mu: &b.mu, stop: stop}, nil
}

func (b *FTPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	if ex, release, ok := b.acquireExtra(ctx); ok {
		Log.Add("ftp", ">>>", fmt.Sprintf("RETR %s @%d", relPath, offset))
		resp, err := ex.conn.RetrFrom(path.Join(b.base, relPath), uint64(offset))
		if err != nil {
			release()
			Log.Add("ftp", "ERR", err.Error())
			return nil, err
		}
		return &ftpPooledReader{rc: resp, release: release, stop: cancelCloser(ctx, ex.rawConn)}, nil
	}
	b.mu.Lock()
	Log.Add("ftp", ">>>", fmt.Sprintf("RETR %s @%d", relPath, offset))
	resp, err := b.conn.RetrFrom(path.Join(b.base, relPath), uint64(offset))
	if err != nil {
		b.mu.Unlock()
		Log.Add("ftp", "ERR", err.Error())
		return nil, err
	}
	stop := b.abortOnCancel(ctx)
	return &lockedFTPReader{rc: resp, mu: &b.mu, stop: stop}, nil
}

func (b *FTPBackend) mkdirAllLocked(dir string) {
	mkdirAllOn(b.conn, dir)
}

// mkdirAllOn creates dir and any missing parents on conn. Safe to run on an
// extra connection concurrently with others: MakeDir on an existing dir just
// errors and the List check then passes, so concurrent creators converge.
func mkdirAllOn(conn *ftp.ServerConn, dir string) {
	if dir == "/" || dir == "." || dir == "" {
		return
	}
	conn.MakeDir(dir)
	if _, err := conn.List(dir); err != nil {
		mkdirAllOn(conn, path.Dir(dir))
		conn.MakeDir(dir)
	}
}

// ftpPooledReader wraps a RETR response served by an extra connection. Close
// stops the cancel watcher, closes the data stream, and returns the conn to
// the pool — exactly once, since the copy layer may Close more than once.
type ftpPooledReader struct {
	rc      io.ReadCloser
	release func()
	stop    func()
	once    sync.Once
}

func (r *ftpPooledReader) Read(p []byte) (int, error) {
	return r.rc.Read(p)
}

func (r *ftpPooledReader) Close() error {
	var err error
	r.once.Do(func() {
		if r.stop != nil {
			r.stop()
		}
		err = r.rc.Close()
		r.release()
	})
	return err
}

type lockedFTPReader struct {
	rc     io.ReadCloser
	mu     *sync.Mutex
	stop   func()
	closed bool
}

func (r *lockedFTPReader) Read(p []byte) (int, error) {
	return r.rc.Read(p)
}

func (r *lockedFTPReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.stop != nil {
		r.stop()
	}
	err := r.rc.Close()
	r.mu.Unlock()
	return err
}

type ftpHasher struct {
	mu     sync.Mutex
	conn   io.Closer
	reader *textproto.Reader
	writer *textproto.Writer
	cmds   map[string]string
	algo   string
}

func newFTPHasher(host, port, user, pass, scheme string, tlsCfg *tls.Config) (*ftpHasher, error) {
	addr := host + ":" + port
	rawConn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}

	var conn io.ReadWriteCloser = rawConn

	if scheme == "ftps" {
		conn = tls.Client(rawConn, tlsCfg)
	}

	h := &ftpHasher{conn: conn}
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
		conn = tlsConn
		h.conn = conn
		h.setConn(conn)
	}

	code, _, err = h.sendCmd("USER %s", user)
	if err != nil {
		h.close()
		return nil, err
	}
	if code == 331 {
		code, _, err = h.sendCmd("PASS %s", pass)
		if err != nil {
			h.close()
			return nil, err
		}
	}
	if code != 230 {
		h.close()
		return nil, fmt.Errorf("ftp hash login: %d", code)
	}

	code, msg, _ := h.sendCmd("FEAT")
	h.cmds = make(map[string]string)
	if code == 211 {
		h.parseFeat(msg)
	}

	if len(h.cmds) == 0 {
		h.close()
		return nil, fmt.Errorf("no hash commands available")
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
		Log.Add("ftp", ">>>", "PASS ***")
	} else {
		Log.Add("ftp", ">>>", cmd)
	}
	if err := h.writer.PrintfLine(format, args...); err != nil {
		Log.Add("ftp", "ERR", err.Error())
		return 0, "", err
	}
	code, msg, err := h.reader.ReadResponse(0)
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

func (h *ftpHasher) close() {
	h.sendCmd("QUIT")
	h.conn.Close()
}

var ftpAlgoName = map[string]string{
	"sha256": "SHA-256",
	"sha512": "SHA-512",
	"sha1":   "SHA-1",
	"md5":    "MD5",
}

func (h *ftpHasher) parseFeat(msg string) {
	for _, line := range strings.Split(msg, "\n") {
		line = strings.TrimSpace(line)
		upper := strings.ToUpper(line)
		switch {
		case upper == "XSHA256":
			h.cmds["sha256"] = "XSHA256"
		case upper == "XSHA512":
			h.cmds["sha512"] = "XSHA512"
		case upper == "XSHA1":
			h.cmds["sha1"] = "XSHA1"
		case upper == "XMD5":
			h.cmds["md5"] = "XMD5"
		case upper == "XCRC":
			h.cmds["crc32"] = "XCRC"
		case strings.HasPrefix(upper, "HASH "):
			for _, a := range strings.Split(line[5:], ";") {
				a = strings.TrimSpace(strings.TrimRight(a, "*"))
				switch strings.ToUpper(a) {
				case "SHA-256":
					if h.cmds["sha256"] == "" {
						h.cmds["sha256"] = "HASH"
					}
				case "SHA-512":
					if h.cmds["sha512"] == "" {
						h.cmds["sha512"] = "HASH"
					}
				case "SHA-1":
					if h.cmds["sha1"] == "" {
						h.cmds["sha1"] = "HASH"
					}
				case "MD5":
					if h.cmds["md5"] == "" {
						h.cmds["md5"] = "HASH"
					}
				}
			}
		}
	}
}

func (h *ftpHasher) hash(fullPath string) (string, error) {
	cmd := h.cmds[h.algo]
	if cmd == "" {
		return "", fmt.Errorf("no hash command for %s", h.algo)
	}

	if cmd == "HASH" {
		name := ftpAlgoName[h.algo]
		if name != "" {
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
