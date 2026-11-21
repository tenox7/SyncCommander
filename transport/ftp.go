package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/textproto"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jlaffaye/ftp"

	"sc/model"
)

type FTPBackend struct {
	base string
	conn *ftp.ServerConn
	// socks are the primary's sockets, which a cancel closes. A reconnect
	// replaces them under a cancel watcher.
	socks   atomic.Pointer[ftpSockets]
	hash    *ftpHashConn
	display string
	addr    string
	user    string
	pass    string
	scheme  string
	tlsCfg  *tls.Config
	pool    *connPool[*ftpConn]
	// prim is held while the primary connection is in use: a call waits for
	// it only as long as its ctx lets it, as a call stuck on the server
	// holds it (L8). It guards conn.
	prim    chan struct{}
	closed  atomic.Bool
	dirs    sync.Map // directories known to exist, so uploads skip the MKD dance
	capture *listCapture
	// dashA is whether LIST takes -a on a server without MLSD: 0 not known
	// yet, 1 yes, -1 no. See listOn. The listings on every connection
	// share it.
	dashA atomic.Int32
	noSum sumNote // says the first file the server's sum answer holds none for
}

// ftpIdle is the longest one read or one write on an FTP socket may wait for
// its bytes: a reply on the control connection, or the next bytes of a
// transfer or a listing. A server that stops answering fails the call after
// it, with no cancel needed (L8).
var ftpIdle = idleSetting{def: 5 * time.Minute}

// SetFTPIdleTimeout sets ftpIdle; 0 or less waits for ever.
func SetFTPIdleTimeout(d time.Duration) { ftpIdle.set(d) }

// opDeadlineConn gives each read and each write a deadline from when it
// starts. A socket may sit idle between them as long as it likes, as a
// control connection does while its transfer runs; idleTimeoutConn moves
// the deadline after each one, which would expire a control connection
// during a long transfer. A deadline set from outside still holds when it
// comes first: TLS sets one to send its close alert. A read or a write that
// runs out of time closes the socket: a reply that came after its call gave
// up would be read as the reply to the next one.
type opDeadlineConn struct {
	net.Conn
	idle   time.Duration
	mu     sync.Mutex
	rd, wd time.Time // set from outside; zero for none
}

// ftpWriteChunk is the most one write sends under one deadline, so a large
// buffer that a slow link moves steadily does not run out of time.
const ftpWriteChunk = 64 << 10

func (c *opDeadlineConn) Read(p []byte) (int, error) {
	if c.idle > 0 {
		c.mu.Lock()
		c.Conn.SetReadDeadline(sooner(c.rd, c.idle))
		c.mu.Unlock()
	}
	n, err := c.Conn.Read(p)
	c.closeOnTimeout(err)
	return n, err
}

func (c *opDeadlineConn) Write(p []byte) (int, error) {
	if c.idle <= 0 {
		n, err := c.Conn.Write(p)
		c.closeOnTimeout(err)
		return n, err
	}
	total := 0
	for len(p) > 0 {
		c.mu.Lock()
		c.Conn.SetWriteDeadline(sooner(c.wd, c.idle))
		c.mu.Unlock()
		n, err := c.Conn.Write(p[:min(len(p), ftpWriteChunk)])
		total += n
		if err != nil {
			c.closeOnTimeout(err)
			return total, err
		}
		p = p[n:]
	}
	return total, nil
}

func (c *opDeadlineConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rd, c.wd = t, t
	return c.Conn.SetDeadline(t)
}

func (c *opDeadlineConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rd = t
	return c.Conn.SetReadDeadline(t)
}

func (c *opDeadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wd = t
	return c.Conn.SetWriteDeadline(t)
}

func (c *opDeadlineConn) closeOnTimeout(err error) {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		c.Conn.Close()
	}
}

// sooner is idle from now, or set when that comes first.
func sooner(set time.Time, idle time.Duration) time.Time {
	d := time.Now().Add(idle)
	if !set.IsZero() && set.Before(d) {
		return set
	}
	return d
}

// ftpSockets are the sockets of one control connection: its own, and the
// data connections it opens. Close closes them all, which ends any call on
// the connection: closing the control socket alone left a transfer or a
// listing waiting on its data socket (L8). A data connection dialed after
// Close is closed at once.
type ftpSockets struct {
	ctx    context.Context // ends at Close, and with it a data dial
	cancel context.CancelFunc
	mu     sync.Mutex
	ctrl   net.Conn
	data   map[*ftpDataConn]struct{}
	closed bool
}

func newFTPSockets() *ftpSockets {
	ctx, cancel := context.WithCancel(context.Background())
	return &ftpSockets{ctx: ctx, cancel: cancel, data: map[*ftpDataConn]struct{}{}}
}

func (s *ftpSockets) setCtrl(c net.Conn) {
	s.mu.Lock()
	s.ctrl = c
	s.mu.Unlock()
}

// addData keeps c, a data connection, until it closes.
func (s *ftpSockets) addData(c net.Conn) (net.Conn, error) {
	d := &ftpDataConn{Conn: c, s: s}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		c.Close()
		return nil, net.ErrClosed
	}
	s.data[d] = struct{}{}
	return d, nil
}

func (s *ftpSockets) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	s.mu.Lock()
	s.closed = true
	ctrl, data := s.ctrl, s.data
	s.data = nil
	s.mu.Unlock()
	for d := range data {
		d.Conn.Close()
	}
	if ctrl != nil {
		return ctrl.Close()
	}
	return nil
}

// ftpDataConn is a data connection its ftpSockets keeps until it closes.
type ftpDataConn struct {
	net.Conn
	s *ftpSockets
}

func (d *ftpDataConn) Close() error {
	d.s.mu.Lock()
	delete(d.s.data, d)
	d.s.mu.Unlock()
	return d.Conn.Close()
}

// ftpConn is one extra control+data connection, for a parallel transfer or
// for a listing beside another (L15). A single ftp.ServerConn is not safe
// for concurrent use (one data conn at a time), so each parallel slot gets
// its own. lastUsed gates a liveness probe on acquire; see acquireExtra.
// capture reads its listings, as the primary's does.
type ftpConn struct {
	conn     *ftp.ServerConn
	socks    *ftpSockets
	capture  *listCapture
	lastUsed time.Time
}

// lister is the extra as a listing runs on it.
func (c *ftpConn) lister() ftpLister { return ftpLister{c.conn, c.capture} }

func (c *ftpConn) close() {
	if c != nil && c.conn != nil {
		c.conn.Quit()
	}
}

// dialFTPConn opens one control connection: dial, login, binary mode. The
// dial hook meters every socket and adds the TLS the library would have: with
// a custom dialer jlaffaye skips its own implicit-TLS dial and hands out data
// connections before its TLS branch, so ftps would speak plaintext to port
// 990 and ftpes data channels would ignore PROT P. It returns the sockets
// of the connection, which a cancel closes to end a call that waits on one.
// Until the login is done a cancel of ctx closes the control socket, which
// ends a dial, a greeting or a login that waits on a server that does not
// answer (D1). The data connections it opens later are dialled under the
// call that needs them.
func dialFTPConn(ctx context.Context, addr, scheme, user, pass string, tlsCfg *tls.Config, lc *listCapture) (*ftp.ServerConn, *ftpSockets, error) {
	// A data connection resumes the TLS session of the control connection
	// it carries data for: vsftpd (require_ssl_reuse), ProFTPD and
	// FileZilla Server refuse one that does not, by default. With no
	// session cache Go resumes none, and against such a server the first
	// listing failed (L18). Each control connection has a cache of its
	// own, so its data connections take its session, not another's.
	if tlsCfg != nil && (scheme == "ftps" || scheme == "ftpes") {
		tlsCfg = tlsCfg.Clone()
		tlsCfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	socks := newFTPSockets()
	idle := ftpIdle.get()
	var dials atomic.Int32
	stopWatch := func() bool { return true }
	dialFunc := func(network, address string) (net.Conn, error) {
		n := dials.Add(1)
		dialCtx := socks.ctx
		if n == 1 {
			dialCtx = ctx
		}
		raw, err := dialer.DialContext(dialCtx, network, address)
		if err != nil {
			return nil, err
		}
		// The limiter sleeps outside the deadline: each chunk it sends
		// gets its own.
		c := LimitConn(&opDeadlineConn{Conn: raw, idle: idle})
		if n == 1 {
			socks.setCtrl(c)
			stopWatch = context.AfterFunc(ctx, func() { c.Close() })
		} else if c, err = socks.addData(c); err != nil {
			return nil, err
		}
		// Explicit TLS upgrades the control connection itself after AUTH TLS
		// and leaves only the data sockets to us.
		if scheme == "ftps" || (scheme == "ftpes" && n > 1) {
			c = tls.Client(c, tlsCfg)
		}
		if n > 1 && lc != nil {
			c = &captureConn{Conn: c, lc: lc}
		}
		return c, nil
	}
	// With writing MDTM, a server with no MFMT that offers MDTM sets times
	// with "MDTM time path", as vsftpd does (mdtm_write): SetTimes failed
	// on every vsftpd upload (L17). The library sends MFMT where it can.
	opts := []ftp.DialOption{ftp.DialWithTimeout(10 * time.Second), ftp.DialWithDialFunc(dialFunc), ftp.DialWithWritingMDTM(true)}
	if lc != nil {
		opts = append(opts, ftp.DialWithDebugOutput(lc))
	}
	switch scheme {
	case "ftps":
		opts = append(opts, ftp.DialWithTLS(tlsCfg))
	case "ftpes":
		opts = append(opts, ftp.DialWithExplicitTLS(tlsCfg))
	}
	conn, err := loginFTP(addr, user, pass, opts)
	if !stopWatch() && err == nil {
		conn.Quit() // ctx ended as the login finished: the watcher closed it
		err = ctx.Err()
	}
	if err != nil {
		socks.Close()
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("ftp %s: %w", addr, ctx.Err())
		}
		return nil, nil, err
	}
	return conn, socks, nil
}

// loginFTP dials addr, logs in and switches to binary mode.
func loginFTP(addr, user, pass string, opts []ftp.DialOption) (*ftp.ServerConn, error) {
	conn, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("ftp dial %s: %w", addr, err)
	}
	if err := conn.Login(user, pass); err != nil {
		conn.Quit()
		err = fmt.Errorf("ftp login %s@%s: %w", user, addr, err)
		// 530: not logged in, the server refused the user or the password.
		if tp := (*textproto.Error)(nil); errors.As(err, &tp) && tp.Code == ftp.StatusNotLoggedIn {
			err = model.WithKind(model.ErrAuth, err)
		}
		return nil, err
	}
	if err := conn.Type(ftp.TransferTypeBinary); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("ftp binary mode: %w", err)
	}
	return conn, nil
}

// NewFTPBackend connects to rawURL; a cancel of ctx stops the connect.
func NewFTPBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*FTPBackend, error) {
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
	// An IPv6 literal gets its brackets back: ftp://[::1]/ was dialed as
	// "::1:21", which no dial takes (L25).
	addr := net.JoinHostPort(host, port)

	capture := &listCapture{}
	conn, socks, err := dialFTPConn(ctx, addr, scheme, user, pass, tlsCfg, capture)
	if err != nil {
		return nil, err
	}
	if remotePath == "/~" || strings.HasPrefix(remotePath, "/~/") {
		if wd, err := conn.CurrentDir(); err == nil {
			remotePath = path.Join(wd, strings.TrimPrefix(remotePath, "/~"))
		}
	}
	hasher, hashErr := newFTPHasher(ctx, host, port, user, pass, scheme, tlsCfg)
	if err := ctx.Err(); err != nil {
		// Cancelled while the hash connection was set up: a backend
		// without it would quietly have no checksums.
		if hasher != nil {
			hasher.close()
		}
		conn.Quit()
		return nil, fmt.Errorf("ftp %s: %w", addr, err)
	}

	displayHost := host
	if port != "21" && port != "990" {
		displayHost = addr
	}
	b := &FTPBackend{
		base: remotePath,
		conn: conn,
		prim: make(chan struct{}, 1),
		hash: newFTPHashConn(func(ctx context.Context) (*ftpHasher, error) {
			return newFTPHasher(ctx, host, port, user, pass, scheme, tlsCfg)
		}, hasher, hashErr),
		display: fmt.Sprintf("%s://%s@%s%s", scheme, user, displayHost, remotePath),
		addr:    addr,
		user:    user,
		pass:    pass,
		scheme:  scheme,
		tlsCfg:  tlsCfg,
		capture: capture,
	}
	b.socks.Store(socks)

	// A connection of its own for each of the parallel transfers. A control
	// connection runs one command at a time: the n-th transfer ran on the
	// primary and held up every listing, Mkdir and SetTimes behind it for
	// as long as its file took (L16). A transfer now waits for an extra in
	// use, and takes the primary (nil from the pool) only when there is
	// none: the server takes no connection beyond the primary and the
	// checksum one.
	dial := func(ctx context.Context) (*ftpConn, error) {
		lc := new(listCapture)
		c, socks, derr := dialFTPConn(ctx, addr, scheme, user, pass, tlsCfg, lc)
		if derr != nil {
			return nil, derr
		}
		Log.Add("ftp", DirIn, "extra connection dialed")
		return &ftpConn{conn: c, socks: socks, capture: lc, lastUsed: time.Now()}, nil
	}
	b.pool = newConnPool[*ftpConn]("ftp", nil, parallel, dial, func(c *ftpConn) { c.close() })
	b.pool.waits = true
	return b, nil
}

// abortOnCancel returns a stop func that, while running, closes the
// primary's sockets when ctx is canceled: the control socket and the data
// sockets, so a call that waits on either ends.
func (b *FTPBackend) abortOnCancel(ctx context.Context) func() {
	return CancelCloser(ctx, primarySockets{b})
}

// primarySockets closes the primary's sockets as they are when it is
// called: a reconnect under a call replaces them, and the call that runs
// again on the new ones is ended by a cancel too.
type primarySockets struct{ b *FTPBackend }

func (p primarySockets) Close() error { return p.b.socks.Load().Close() }

// lockPrimary takes the primary connection, waiting no longer than ctx.
func (b *FTPBackend) lockPrimary(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case b.prim <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *FTPBackend) unlockPrimary() { <-b.prim }

func (b *FTPBackend) BasePath() string { return b.display }

// abs is relPath's path on the server, under the base (cleanRel).
func (b *FTPBackend) abs(relPath string) string { return path.Join(b.base, cleanRel(relPath)) }

// SetParallel keeps n extra connections, one for each of n transfers: the
// primary is for the calls in between (L16).
func (b *FTPBackend) SetParallel(n int) { b.pool.resize(n) }

// Close says QUIT on the primary connection: now when it is free, or else
// once the call that holds it lets it go. Close waited for that call, so
// one stuck on a server that does not answer held quit for good (L8); its
// cancel, or ftpIdle, ends such a call now. A call that fails after Close
// makes no new connection.
func (b *FTPBackend) Close() error {
	b.closed.Store(true)
	b.pool.close()
	b.hash.close()
	select {
	case b.prim <- struct{}{}:
		defer b.unlockPrimary()
		return b.conn.Quit()
	default:
	}
	go func() {
		b.prim <- struct{}{}
		defer b.unlockPrimary()
		b.conn.Quit()
	}()
	return nil
}

// acquireExtra returns an extra connection and its release, which takes
// the error of the call on it, or ok=false when there is none to have: the
// server takes no extra connection, the pool is closed or ctx ended, or,
// with wait false, each extra is lent out. The caller then runs on the
// primary, under prim. An extra may sit idle a long while, so one idle for
// a minute may have been dropped by the server, and one a call failed on
// may be dead: such an extra is asked (NOOP) before it is lent out. One
// found dead is dropped, and the pool dials the next, its wait and all: it
// was dialed again here, outside that wait, once per transfer (L16).
func (b *FTPBackend) acquireExtra(ctx context.Context, wait bool) (*ftpConn, func(error), bool) {
	acquire := b.pool.acquire
	if !wait {
		acquire = b.pool.tryAcquire
	}
	for {
		c, poolRelease := acquire(ctx)
		if c == nil {
			poolRelease()
			return nil, nil, false
		}
		if time.Since(c.lastUsed) <= 60*time.Second || c.probe(ctx) == nil {
			return c, func(err error) {
				c.lastUsed = time.Now()
				if err != nil || ctx.Err() != nil {
					// A cancel closed its sockets, or the transfer failed on
					// it: it is asked before its next use.
					c.lastUsed = time.Time{}
				}
				poolRelease()
			}, true
		}
		Log.Add("ftp", DirIn, "an extra connection is gone: dropped")
		b.pool.discard(c)
		if ctx.Err() != nil {
			return nil, nil, false
		}
	}
}

// probe asks c whether it still answers; a cancel of ctx ends the wait.
func (c *ftpConn) probe(ctx context.Context) error {
	defer CancelCloser(ctx, c.socks)()
	return c.conn.NoOp()
}

func (b *FTPBackend) reconnectLocked(ctx context.Context) error {
	if b.closed.Load() {
		return net.ErrClosed // a call Close ended makes no new connection
	}
	b.conn.Quit()
	conn, socks, err := dialFTPConn(ctx, b.addr, b.scheme, b.user, b.pass, b.tlsCfg, b.capture)
	if err != nil {
		b.socks.Store(nil)
		return err
	}
	b.conn = conn
	b.socks.Store(socks)
	Log.Add("ftp", DirIn, "reconnected")
	return nil
}

// withPrimary runs op on the primary control connection under the lock and,
// when the connection turns out dead, reconnects and runs it once more. Only
// for ops that can safely repeat.
func (b *FTPBackend) withPrimary(ctx context.Context, op func() error) error {
	if err := b.lockPrimary(ctx); err != nil {
		return err
	}
	defer b.unlockPrimary()
	return b.onPrimaryLocked(ctx, op)
}

// onPrimaryLocked is withPrimary once prim is held.
func (b *FTPBackend) onPrimaryLocked(ctx context.Context, op func() error) error {
	defer b.abortOnCancel(ctx)()
	err := op()
	if err == nil || ctx.Err() != nil || b.conn.NoOp() == nil {
		return err
	}
	Log.Add("ftp", DirErr, "connection lost, reconnecting...")
	if rerr := b.reconnectLocked(ctx); rerr != nil {
		Log.Add("ftp", DirErr, "reconnect failed: "+rerr.Error())
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err() // a cancel during the reconnect closed the old sockets
	}
	return op()
}

// List lists relDir on the primary when it is free, else on an extra, so
// listings run side by side: on the primary alone a scan of 8 folders at
// once listed them in turn (L15). A listing waits for no extra a transfer
// holds. Only with no extra to have does it wait for the primary; that
// wait is not the listing's time, which starts once it has the primary
// (ListWait). A listing on an extra that died under it runs on the
// primary.
func (b *FTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	var result []model.FileEntry
	onPrimary := func(ctx context.Context) error {
		defer b.unlockPrimary()
		return b.onPrimaryLocked(ctx, func() (err error) {
			result, err = b.listDir(ctx, b.lister(), relDir)
			return err
		})
	}
	if b.tryLockPrimary() {
		err := onPrimary(ctx)
		return result, err
	}
	if c, release, ok := b.acquireExtra(ctx, false); ok {
		stop := CancelCloser(ctx, c.socks)
		entries, err := b.listDir(ctx, c.lister(), relDir)
		stop()
		if err == nil || ctx.Err() != nil || c.probe(ctx) == nil {
			release(err)
			return entries, err
		}
		Log.Add("ftp", DirIn, "an extra connection is gone: dropped")
		b.pool.discard(c)
	}
	wait, d, stopWait := model.ListWait(ctx)
	defer stopWait()
	if err := b.lockPrimary(wait); err != nil {
		return nil, err
	}
	if d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(wait, d)
		defer cancel()
	}
	err := onPrimary(ctx)
	return result, err
}

// tryLockPrimary takes prim if it is free now.
func (b *FTPBackend) tryLockPrimary() bool {
	select {
	case b.prim <- struct{}{}:
		return true
	default:
		return false
	}
}

// ftpLister is a control connection a listing runs on, with the capture
// that reads its listing streams: the primary, or an extra (L15).
type ftpLister struct {
	conn    *ftp.ServerConn
	capture *listCapture
}

// lister is the primary as a listing runs on it; prim is held.
func (b *FTPBackend) lister() ftpLister { return ftpLister{b.conn, b.capture} }

// listDir lists relDir on l: the primary, under prim, or an extra.
func (b *FTPBackend) listDir(ctx context.Context, l ftpLister, relDir string) ([]model.FileEntry, error) {
	dir, entries, err := b.rawList(ctx, l, relDir)
	if err != nil {
		return nil, err
	}
	result := make([]model.FileEntry, 0, len(entries))
	for _, e := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if e.Name == "." || e.Name == ".." {
			continue
		}
		result = append(result, b.entry(l, path.Join(relDir, e.Name), path.Join(dir, e.Name), e, false))
	}
	return result, nil
}

// rawList lists relDir, at dir on the server, on l, as the library reads it.
func (b *FTPBackend) rawList(ctx context.Context, l ftpLister, relDir string) (dir string, entries []*ftp.Entry, err error) {
	if ctx.Err() != nil {
		return "", nil, ctx.Err()
	}
	dir = b.abs(relDir)
	entries, err = b.listOn(l, dir)
	if err == nil && len(entries) == 0 {
		// Some servers answer an empty 226 for a missing dir; CWD tells them apart.
		err = l.conn.ChangeDir(dir)
	}
	if err != nil {
		Log.Add("ftp", DirErr, err.Error())
		return "", nil, err
	}
	Log.Add("ftp", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return dir, entries, nil
}

// entry is the row of relPath, at p on the server, which a listing or MLST
// on l described as e. MLSD and MLST carry times to the second. A LIST
// time is kept to the minute, or the day (listTimeRes), which the compare
// takes as exactly as that (L17); with exact, as for a Stat, MDTM reads it
// to the second. A listing asked MDTM for every entry: a round trip a
// file, which the time for a listing ran out on, and 14 hours for 1M
// files at 50 ms (L15).
func (b *FTPBackend) entry(l ftpLister, relPath, p string, e *ftp.Entry, exact bool) model.FileEntry {
	mode := os.FileMode(0644)
	switch e.Type {
	case ftp.EntryTypeFolder:
		mode = os.ModeDir | 0755
	case ftp.EntryTypeLink:
		mode = os.ModeSymlink | 0777 // listed, never read or written through (L2)
	case ftpEntrySpecial:
		mode = os.ModeIrregular | 0644
	}
	modTime, res := e.Time, model.TimeSecond
	if !l.conn.IsTimePreciseInList() {
		res = listTimeRes(e.Time)
		if exact && l.conn.IsGetTimeSupported() {
			if t, err := l.conn.GetTime(p); err == nil {
				modTime, res = t, model.TimeSecond
			}
		}
	}
	fe := model.FileEntry{
		RelPath: relPath,
		Name:    path.Base(relPath),
		Size:    int64(e.Size),
		ModTime: modTime,
		TimeRes: res,
		IsDir:   e.Type == ftp.EntryTypeFolder,
		Mode:    mode,
	}
	if relPath == "" {
		fe.Name = ""
	}
	if e.Type == ftp.EntryTypeLink {
		// LIST shows a link as "name -> target", MLSD as the fact
		// OS.unix=slink:target (markMLSDTypes); a server may give none.
		fe.Link = model.LinkTo(e.Target)
	}
	return fe
}

// listTimeRes is how exact a time from a LIST line is: "Nov 16 10:00" is
// to the minute; "Mar  4  2020", for a file more than six months old, is a
// day, read as its midnight. A time compared to the second was "different"
// after every copy (L17).
func listTimeRes(t time.Time) model.TimeRes {
	if h, m, s := t.Clock(); h == 0 && m == 0 && s == 0 {
		return model.TimeDay
	}
	return model.TimeMinute
}

// Stat asks for relPath alone. With MLST that is one command on the control
// connection. The library reads only "file" and "dir" from the answer and
// takes a link for a file, so the type fact is read from the raw answer, as
// for a listing (markMLSDTypes). A server without MLST has no one-path
// command that does not follow links (SIZE, MDTM do), so there relPath is
// picked out of its folder's listing. 550, the answer for a missing path,
// is fs.ErrNotExist.
func (b *FTPBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	var e model.FileEntry
	err := b.withPrimary(ctx, func() (err error) {
		e, err = b.statLocked(ctx, relPath)
		return err
	})
	return e, err
}

func (b *FTPBackend) statLocked(ctx context.Context, relPath string) (model.FileEntry, error) {
	if ctx.Err() != nil {
		return model.FileEntry{}, ctx.Err()
	}
	p := b.abs(relPath)
	if !b.conn.IsTimePreciseInList() { // the library offers MLST with MLSD
		return b.statByListingLocked(ctx, relPath, p)
	}
	Log.Add("ftp", DirOut, "MLST "+p)
	b.capture.ctrl.Store(new(bytes.Buffer))
	e, err := b.conn.GetEntry(p)
	raw := b.capture.ctrl.Swap(nil)
	if err != nil {
		return model.FileEntry{}, ftpStatErr(relPath, err)
	}
	if raw != nil {
		markMLSDTypes(raw.Bytes(), []*ftp.Entry{e})
	}
	return b.entry(b.lister(), relPath, p, e, true), nil
}

func (b *FTPBackend) statByListingLocked(ctx context.Context, relPath, p string) (model.FileEntry, error) {
	if relPath == "" {
		if err := b.conn.ChangeDir(p); err != nil {
			return model.FileEntry{}, ftpStatErr(relPath, err)
		}
		return model.FileEntry{IsDir: true, Mode: os.ModeDir | 0755}, nil
	}
	dir, entries, err := b.rawList(ctx, b.lister(), model.DirOf(relPath))
	if err != nil {
		return model.FileEntry{}, ftpStatErr(relPath, err)
	}
	name := path.Base(relPath)
	for _, e := range entries {
		if e.Name == name {
			return b.entry(b.lister(), relPath, path.Join(dir, name), e, true), nil
		}
	}
	return model.FileEntry{}, fmt.Errorf("ftp: %s: %w", relPath, fs.ErrNotExist)
}

// ftpStatErr makes the server's 550 for a path fs.ErrNotExist.
func ftpStatErr(relPath string, err error) error {
	var reply *textproto.Error
	if errors.As(err, &reply) && reply.Code == ftp.StatusFileUnavailable {
		return fmt.Errorf("ftp: %s: %s: %w", relPath, reply.Msg, fs.ErrNotExist)
	}
	return err
}

// listOn lists dir on l: the primary, under prim, or an extra. A server
// without MLSD gets LIST, and servers that work like ls, vsftpd by default,
// then leave out names starting with "." unless asked with LIST -a. A server
// that takes no options could read "-a" as a name and answer with nothing.
// So the first listing that has entries runs both forms, and -a is kept
// only when its listing is the plain one plus names starting with ".".
func (b *FTPBackend) listOn(l ftpLister, dir string) ([]*ftp.Entry, error) {
	if l.conn.IsTimePreciseInList() || b.dashA.Load() < 0 {
		return l.listChecked(dir)
	}
	if b.dashA.Load() > 0 {
		return l.listChecked("-a " + dir)
	}
	plain, err := l.listChecked(dir)
	if err != nil {
		return nil, err
	}
	all, err := l.listChecked("-a " + dir)
	var reply *textproto.Error
	switch {
	case err != nil && !errors.As(err, &reply):
		return nil, err // the connection failed, not the -a
	case err != nil || !onlyAddsDotNames(plain, all):
		b.dashA.Store(-1)
		if !hasDotName(plain) {
			Log.Add("ftp", DirWarn, `this server has no MLSD and does not take LIST -a: names starting with "." may be left out of its listings`)
		}
		return plain, nil
	case len(realEntries(plain)) > 0:
		b.dashA.Store(1)
	}
	return all, nil
}

// listChecked runs one LIST or MLSD. The library drops a line it cannot
// parse; such a line is an entry the listing would lack, so it fails the
// listing here.
func (l ftpLister) listChecked(arg string) ([]*ftp.Entry, error) {
	Log.Add("ftp", DirOut, "LIST "+arg)
	l.capture.start()
	entries, err := l.conn.List(arg)
	raw := l.capture.stop()
	if err != nil {
		return nil, err
	}
	markMLSDTypes(raw, entries)
	if n, example := unparsedLines(raw, entries); n > 0 {
		return nil, fmt.Errorf("LIST %s: the server sent %d listing line(s) sc cannot read, such as %q", arg, n, example)
	}
	return entries, nil
}

func realEntries(es []*ftp.Entry) []*ftp.Entry {
	var out []*ftp.Entry
	for _, e := range es {
		if e.Name != "." && e.Name != ".." {
			out = append(out, e)
		}
	}
	return out
}

func hasDotName(es []*ftp.Entry) bool {
	for _, e := range realEntries(es) {
		if strings.HasPrefix(e.Name, ".") {
			return true
		}
	}
	return false
}

// onlyAddsDotNames reports whether all holds every name of plain, and
// nothing else but names starting with ".": what ls -a adds.
func onlyAddsDotNames(plain, all []*ftp.Entry) bool {
	seen := map[string]bool{}
	for _, e := range realEntries(plain) {
		seen[e.Name] = true
	}
	for _, e := range realEntries(all) {
		if !seen[e.Name] && !strings.HasPrefix(e.Name, ".") {
			return false
		}
		delete(seen, e.Name)
	}
	return len(seen) == 0
}

var lsTotalLine = regexp.MustCompile(`(?i)^total\s+\d+$`)

// unparsedLines counts the lines of a raw listing that gave no entry: all
// lines but blank ones and ls's "total N" are entries. example is one of
// them, found by the names the entries came with.
func unparsedLines(raw []byte, entries []*ftp.Entry) (n int, example string) {
	var lines []string
	s := bufio.NewScanner(bytes.NewReader(raw))
	for s.Scan() {
		if line := strings.TrimSpace(s.Text()); line != "" && !lsTotalLine.MatchString(line) {
			lines = append(lines, line)
		}
	}
	if len(lines) <= len(entries) {
		return 0, ""
	}
	left := lines // each entry takes one line, so at least one is left
	for _, e := range entries {
		for i, l := range left {
			if strings.HasSuffix(l, " "+e.Name) || strings.HasSuffix(l, ";"+e.Name) || strings.Contains(l, " "+e.Name+" -> ") {
				left = append(left[:i:i], left[i+1:]...)
				break
			}
		}
	}
	return len(lines) - len(entries), left[0]
}

// ftpEntrySpecial marks an MLSD entry whose type is neither file nor
// folder nor symbolic link: a device or a pipe.
const ftpEntrySpecial ftp.EntryType = -1

// markMLSDTypes reads the type fact of the raw MLSD lines. The library knows
// only file and dir, and lists any other type as a file: a link
// (OS.unix=symlink, OS.unix=slink:target) would be read and written
// through, and a device opened (L2, L9). The name is what follows the
// first space, as the library takes it. A link's target is the text after
// slink:, where the server gives it.
func markMLSDTypes(raw []byte, entries []*ftp.Entry) {
	var special map[string]ftp.EntryType
	var targets map[string]string
	s := bufio.NewScanner(bytes.NewReader(raw))
	for s.Scan() {
		// An MLST answer indents its fact line by a space.
		line := strings.TrimLeft(strings.TrimRight(s.Text(), "\r"), " ")
		semi, space := strings.Index(line, ";"), strings.Index(line, " ")
		if semi < 0 || space < 0 || semi > space {
			continue // not an MLSD line
		}
		for _, fact := range strings.Split(line[:space], ";") {
			k, v, _ := strings.Cut(fact, "=")
			if !strings.EqualFold(k, "type") {
				continue
			}
			t := mlsdType(v)
			if t == ftp.EntryTypeFile || t == ftp.EntryTypeFolder {
				break
			}
			if special == nil {
				special = make(map[string]ftp.EntryType)
			}
			special[line[space+1:]] = t
			if len(v) > len(slinkFact) && strings.EqualFold(v[:len(slinkFact)], slinkFact) {
				if targets == nil {
					targets = make(map[string]string)
				}
				targets[line[space+1:]] = v[len(slinkFact):]
			}
		}
	}
	for _, e := range entries {
		if t, ok := special[e.Name]; ok {
			e.Type = t
			e.Target = targets[e.Name]
		}
	}
}

// slinkFact starts the value of a type fact that gives a link's target.
const slinkFact = "OS.unix=slink:"

func mlsdType(v string) ftp.EntryType {
	switch v = strings.ToLower(v); {
	case v == "file":
		return ftp.EntryTypeFile
	case v == "dir" || v == "cdir" || v == "pdir":
		return ftp.EntryTypeFolder
	case strings.HasPrefix(v, "os.unix=symlink") || strings.HasPrefix(v, "os.unix=slink"):
		return ftp.EntryTypeLink
	}
	return ftpEntrySpecial
}

// listCapture records what a listing's data connection reads while a List
// runs on its connection, the primary or an extra: each has its own. The
// library reads it on the List caller's goroutine, so no lock is needed
// around the buffer.
type listCapture struct {
	buf  atomic.Pointer[bytes.Buffer]
	ctrl atomic.Pointer[bytes.Buffer] // the control connection, while a Stat runs
}

// Write takes what the library's debug output sees of the primary
// connection: the control connection in plain text, after TLS, and the
// listing streams. It keeps the control connection's part only while a
// Stat runs, under the backend's lock.
func (lc *listCapture) Write(p []byte) (int, error) {
	if b := lc.ctrl.Load(); b != nil {
		b.Write(p)
	}
	return len(p), nil
}

func (lc *listCapture) start() { lc.buf.Store(new(bytes.Buffer)) }

func (lc *listCapture) stop() []byte {
	if b := lc.buf.Swap(nil); b != nil {
		return b.Bytes()
	}
	return nil
}

// captureConn is a data connection of the primary FTP connection.
type captureConn struct {
	net.Conn
	lc *listCapture
}

func (c *captureConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if b := c.lc.buf.Load(); b != nil && n > 0 {
		b.Write(p[:n])
	}
	return n, err
}

// Checksum asks the checksum connection for relPath's sum. A connection
// found dead is dialed again and asked once more: an idle one the server
// ended while a long listing ran fails no file.
func (b *FTPBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	full := b.abs(relPath)
	for try := 0; ; try++ {
		h, err := b.hash.get(ctx)
		if err != nil {
			return "", err
		}
		if h == nil {
			return "", b.noSum.say("ftp", model.WithKind(ErrUnsupported, errors.New("no checksum support on this FTP server")))
		}
		sum, err := h.sum(ctx, full, algo)
		if errors.Is(err, ErrUnsupported) {
			return "", b.noSum.say("ftp", err)
		}
		if !errors.Is(err, errHashConnGone) {
			return sum, err
		}
		b.hash.drop(h)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if try > 0 {
			return "", &hashConnError{err}
		}
	}
}

// ChecksumAlgos are the hash commands the server named in its FEAT answer
// when the checksum connection logged in. With no connection, it dials one:
// one that cannot be made is an error, never "no checksums", which the
// scanner would keep for the session (F24).
func (b *FTPBackend) ChecksumAlgos(ctx context.Context) ([]string, error) {
	h, err := b.hash.get(ctx)
	if err != nil || h == nil {
		return nil, err
	}
	var algos []string
	for _, a := range []string{"sha256", "sha1", "md5"} {
		if h.cmds[a] != "" {
			algos = append(algos, a)
		}
	}
	return algos, nil
}

func (b *FTPBackend) SetTimes(ctx context.Context, relPath string, mtime, _, _ time.Time) error {
	err := b.withPrimary(ctx, func() error { return b.conn.SetTime(b.abs(relPath), mtime) })
	if err != nil {
		Log.Add("ftp", DirErr, "setting the mtime of "+relPath+": "+err.Error())
	}
	return err
}

// store uploads src to relPath, appending at offset when it is positive, on
// an extra connection, and on the locked primary when there is none to
// have (acquireExtra). No retry: src is consumed.
func (b *FTPBackend) store(ctx context.Context, relPath string, src io.Reader, offset int64) (err error) {
	fullPath := b.abs(relPath)
	if ex, release, ok := b.acquireExtra(ctx, true); ok {
		defer func() { release(err) }()
		return b.storeOn(ctx, ex.conn, ex.socks, fullPath, src, offset)
	}
	if err := b.lockPrimary(ctx); err != nil {
		return err
	}
	defer b.unlockPrimary()
	return b.storeOn(ctx, b.conn, b.socks.Load(), fullPath, src, offset)
}

func (b *FTPBackend) storeOn(ctx context.Context, conn *ftp.ServerConn, socks *ftpSockets, fullPath string, src io.Reader, offset int64) error {
	defer CancelCloser(ctx, socks)()
	err := b.ensureDir(conn, path.Dir(fullPath))
	if err == nil && offset > 0 {
		err = conn.StorFrom(fullPath, src, uint64(offset))
	} else if err == nil {
		err = conn.Stor(fullPath, src)
	}
	if err != nil {
		Log.Add("ftp", DirErr, err.Error())
	}
	// 552 and 452: the server has no room for the file (D3).
	if tp := (*textproto.Error)(nil); errors.As(err, &tp) && (tp.Code == ftp.StatusExceededStorage || tp.Code == ftp.Status452) {
		err = model.WithKind(model.ErrNoSpace, err)
	}
	return err
}

func (b *FTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ model.PutParams) error {
	Log.Add("ftp", DirOut, "STOR "+relPath)
	return b.store(ctx, relPath, src, 0)
}

// AppendFrom writes the source from offset on at the end of relPath, which
// must hold exactly offset bytes by SIZE (L14, S8): REST past the end of a
// shorter file leaves a gap of zeros. A server without SIZE gets no append.
func (b *FTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, _ model.PutParams, offset int64) error {
	size, err := b.size(ctx, relPath)
	if err != nil {
		return fmt.Errorf("ftp: size of %s: %v: %w", relPath, err, ErrAppendOffset)
	}
	if size != offset {
		return appendOffsetErr("ftp", relPath, size, offset)
	}
	rd, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rd.Close()
	Log.Add("ftp", DirOut, fmt.Sprintf("STOR %s @%d", relPath, offset))
	return b.store(ctx, relPath, rd, offset)
}

// size asks the server for relPath's size (SIZE).
func (b *FTPBackend) size(ctx context.Context, relPath string) (int64, error) {
	var n int64
	err := b.withPrimary(ctx, func() error {
		var err error
		n, err = b.conn.FileSize(b.abs(relPath))
		return err
	})
	return n, err
}

// PartSize asks for the size of relPath's part (model.PartWriter). A
// negative reply, for no such file or no SIZE, means copying from the start.
func (b *FTPBackend) PartSize(ctx context.Context, relPath string) (int64, error) {
	n, err := b.size(ctx, model.PartPath(relPath))
	var reply *textproto.Error
	if errors.As(err, &reply) && reply.Code >= 500 {
		return 0, nil
	}
	return n, err
}

// CommitPart renames relPath's part over relPath (model.PartWriter). A
// server that will not rename onto a name in use gets the file there
// deleted first, and only while the part is still there to take its place.
// A folder is never deleted (DELE refuses one). FTP has no mode and no sync.
func (b *FTPBackend) CommitPart(ctx context.Context, relPath string, _ os.FileMode) error {
	part, final := b.abs(model.PartPath(relPath)), b.abs(relPath)
	Log.Add("ftp", DirOut, "COMMIT "+relPath)
	err := b.withPrimary(ctx, func() error {
		err := b.conn.Rename(part, final)
		if err == nil {
			return nil
		}
		if _, serr := b.conn.FileSize(part); serr != nil {
			return err
		}
		if b.conn.Delete(final) != nil {
			return err
		}
		return b.conn.Rename(part, final)
	})
	if err != nil {
		Log.Add("ftp", DirErr, "COMMIT "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) Mkdir(ctx context.Context, relPath string, _ os.FileMode) error {
	fullPath := b.abs(relPath)
	Log.Add("ftp", DirOut, "MKDIR "+fullPath)
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
	if err := renameBase("ftp", oldRelPath, newRelPath); err != nil {
		return err
	}
	if taken, err := nameTaken(ctx, b.List, newRelPath); err != nil {
		return err
	} else if taken {
		return renameTargetTaken("ftp", newRelPath)
	}
	b.dirs.Clear()
	err := b.withPrimary(ctx, func() error {
		return b.conn.Rename(b.abs(oldRelPath), b.abs(newRelPath))
	})
	if err != nil {
		Log.Add("ftp", DirErr, "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) Remove(ctx context.Context, relPath string) error {
	err := b.withPrimary(ctx, func() error { return b.conn.Delete(b.abs(relPath)) })
	if err != nil {
		Log.Add("ftp", DirErr, "DELETE "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) RemoveAll(ctx context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("ftp: refusing to remove the base directory")
	}
	b.dirs.Clear()
	err := b.withPrimary(ctx, func() error {
		full := b.abs(relPath)
		e, err := b.entryInFolderLocked(full)
		if err != nil {
			return err
		}
		if e != nil && e.Type != ftp.EntryTypeFolder {
			return b.conn.Delete(full) // a link goes as a link
		}
		return b.removeAllLocked(full)
	})
	if err != nil {
		Log.Add("ftp", DirErr, "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

// entryInFolderLocked finds fullPath in its folder's listing; nil when the
// listing lacks it. FTP has no lstat: asked for a link's path, a server
// lists the folder the link points to, and a folder delete there deleted
// through the link, out of the base too. Only the link's own folder lists
// it as a link.
func (b *FTPBackend) entryInFolderLocked(fullPath string) (*ftp.Entry, error) {
	entries, err := b.listOn(b.lister(), path.Dir(fullPath))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.Name == path.Base(fullPath) {
			return e, nil
		}
	}
	return nil, nil
}

func (b *FTPBackend) removeAllLocked(fullPath string) error {
	entries, err := b.listOn(b.lister(), fullPath)
	if err != nil {
		return b.conn.Delete(fullPath)
	}
	// Each listed name is joined onto fullPath, so each must name a
	// child: with a slash or a ".." in it, it points elsewhere, and then
	// nothing in fullPath is removed (L7).
	for _, e := range entries {
		if e.Name != "." && e.Name != ".." && !model.IsFileName(e.Name) {
			return fmt.Errorf("ftp: %s lists %q, which is no file name: nothing in it is removed", fullPath, e.Name)
		}
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

// retr opens a download on an extra connection; with none to have
// (acquireExtra), it holds the primary's lock until the reader is closed.
func (b *FTPBackend) retr(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	Log.Add("ftp", DirOut, fmt.Sprintf("RETR %s @%d", relPath, offset))
	fullPath := b.abs(relPath)
	if ex, release, ok := b.acquireExtra(ctx, true); ok {
		stop := CancelCloser(ctx, ex.socks)
		resp, err := retrFrom(ex.conn, fullPath, offset)
		if err != nil {
			stop()
			release(err)
			Log.Add("ftp", DirErr, err.Error())
			return nil, err
		}
		return &ftpReader{rc: resp, stop: stop, done: release}, nil
	}
	if err := b.lockPrimary(ctx); err != nil {
		return nil, err
	}
	stop := b.abortOnCancel(ctx)
	resp, err := retrFrom(b.conn, fullPath, offset)
	if err != nil {
		stop()
		b.unlockPrimary()
		Log.Add("ftp", DirErr, err.Error())
		return nil, err
	}
	return &ftpReader{rc: resp, stop: stop, done: func(error) { b.unlockPrimary() }}, nil
}

func retrFrom(conn *ftp.ServerConn, fullPath string, offset int64) (*ftp.Response, error) {
	if offset > 0 {
		return conn.RetrFrom(fullPath, uint64(offset))
	}
	return conn.Retr(fullPath)
}

// ftpReader wraps a RETR body. Close closes the data stream, reads the
// server's reply, stops the cancel watcher and runs done (a pool release or
// the primary unlock) with that reply's error exactly once, since the copy
// layer may Close more than once.
type ftpReader struct {
	rc   io.ReadCloser
	stop func()
	done func(error)
	once sync.Once
	err  error

	ended   bool      // the data stream ended; only Read uses it
	shut    sync.Once // closes the data stream and reads the server's reply
	shutErr error
}

// Read ends with the server's verdict. A server ends the data connection
// the same way for a whole file and for an aborted one, and says which only
// on the control connection: 226, or 426 when it gave up half way. Read
// returns that reply's error in place of io.EOF (L3), and the same again on
// every later Read.
func (r *ftpReader) Read(p []byte) (int, error) {
	if r.ended {
		return 0, r.endErr()
	}
	n, err := r.rc.Read(p)
	if err == io.EOF {
		r.ended = true
		err = r.endErr()
	}
	return n, err
}

func (r *ftpReader) endErr() error {
	if err := r.shutdown(); err != nil {
		return err
	}
	return io.EOF
}

func (r *ftpReader) shutdown() error {
	r.shut.Do(func() { r.shutErr = r.rc.Close() })
	return r.shutErr
}

// Close reads the server's final reply with the cancel watcher still on:
// stopped first, a server that never sent the reply held Close for good
// (L8).
func (r *ftpReader) Close() error {
	r.once.Do(func() {
		r.err = r.shutdown()
		r.stop()
		r.done(r.err)
	})
	return r.err
}

// ftpHashConn holds the connection that asks the server for checksums. It
// is dialed at connect, and again when an ask finds none: the dial failed,
// or the connection died. A server whose FEAT names no hash command has
// none for good. A connection that could not be made is no answer: the
// next ask dials again, after a wait that doubles while dials fail, so a
// server refusing connections is not dialed once per file (F24).
type ftpHashConn struct {
	dial   func(ctx context.Context) (*ftpHasher, error)
	mu     sync.Mutex
	h      *ftpHasher
	none   bool // the server names no hash command
	closed bool
	err    error         // why the last dial failed
	wait   time.Duration // after it, before the next
	next   time.Time
}

// ftpHashRedial is the wait after a failed dial of the checksum connection;
// it doubles while dials fail, up to ftpHashRedialMax.
var ftpHashRedial, ftpHashRedialMax = 5 * time.Second, 2 * time.Minute

// errNoHashCmds is a server whose FEAT names no hash command.
var errNoHashCmds = errors.New("the server names no hash command")

// errHashConnGone is an ask that failed because the checksum connection
// did: the connection is dropped and the next ask dials a new one.
var errHashConnGone = errors.New("the checksum connection is gone")

// hashConnError is a failure of the checksum connection. It hides its cause
// from the error classes of the connection wrapper: the primary connection
// may be fine, so it is neither closed as lost nor held off as unreachable.
type hashConnError struct{ err error }

func (e *hashConnError) Error() string { return "ftp: the checksum connection: " + e.err.Error() }

// newFTPHashConn starts with h and err, the dial made at connect.
func newFTPHashConn(dial func(ctx context.Context) (*ftpHasher, error), h *ftpHasher, err error) *ftpHashConn {
	c := &ftpHashConn{dial: dial}
	c.mu.Lock()
	c.tookLocked(h, err)
	c.mu.Unlock()
	return c
}

// get returns the connection, dialing one when there is none. nil with no
// error is a server with no hash command.
func (c *ftpHashConn) get(ctx context.Context) (*ftpHasher, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		return nil, &hashConnError{net.ErrClosed}
	case c.h != nil || c.none:
		return c.h, nil
	case time.Now().Before(c.next):
		return nil, c.err
	}
	h, err := c.dial(ctx)
	if ctx.Err() != nil {
		// A stopped dial is no failure of the server's: it starts no wait.
		if h != nil {
			h.close()
		}
		return nil, ctx.Err()
	}
	return h, c.tookLocked(h, err)
}

// tookLocked keeps the result of a dial: the connection, the server's "no
// hash command", or the failure and the wait before the next dial. mu held.
func (c *ftpHashConn) tookLocked(h *ftpHasher, err error) error {
	switch {
	case err == nil:
		c.h, c.err, c.wait = h, nil, 0
		return nil
	case errors.Is(err, errNoHashCmds):
		c.none = true
		return nil
	}
	c.wait = min(max(2*c.wait, ftpHashRedial), ftpHashRedialMax)
	c.next = time.Now().Add(c.wait)
	c.err = &hashConnError{err}
	Log.Add("ftp", DirErr, fmt.Sprintf("%v; asking again in %v", c.err, c.wait))
	return c.err
}

// drop forgets h, found dead: the next ask dials again at once.
func (c *ftpHashConn) drop(h *ftpHasher) {
	c.mu.Lock()
	if c.h == h {
		c.h = nil
	}
	c.mu.Unlock()
	h.close()
}

func (c *ftpHashConn) close() {
	c.mu.Lock()
	h := c.h
	c.h, c.closed = nil, true
	c.mu.Unlock()
	if h != nil {
		h.close()
	}
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
	utf8   bool   // FEAT named UTF8
	algo   string // what OPTS HASH chose on this connection; "" before one was asked
	closed bool   // guarded by mu
}

// newFTPHasher opens the connection that asks for checksums. Until it is
// logged in a cancel of ctx closes its socket.
func newFTPHasher(ctx context.Context, host, port, user, pass, scheme string, tlsCfg *tls.Config) (*ftpHasher, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	rawTCP, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, err
	}
	stopWatch := context.AfterFunc(ctx, func() { rawTCP.Close() })
	h, err := startFTPHasher(rawTCP, user, pass, scheme, tlsCfg)
	if !stopWatch() {
		if err == nil {
			h.conn.Close()
		}
		return nil, ctx.Err()
	}
	return h, err
}

// startFTPHasher logs in on rawTCP and reads which hash commands the
// server has.
func startFTPHasher(rawTCP net.Conn, user, pass, scheme string, tlsCfg *tls.Config) (*ftpHasher, error) {
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

	// A FEAT that got no answer says nothing of the server: only one it
	// answered can say it has no hash command.
	code, msg, err := h.cmd("FEAT")
	if err != nil {
		h.close()
		return nil, err
	}
	h.cmds = make(map[string]string)
	if code == 211 {
		h.parseFeat(msg)
	}
	if len(h.cmds) == 0 {
		h.close()
		return nil, errNoHashCmds
	}
	// Names go over this connection as over the main one, which the library
	// switches to UTF-8 when FEAT names it. Without it, a server that starts
	// in another code page did not find a name with an é in it here, or
	// found another file (L25). A refusal leaves the server as it was.
	if h.utf8 {
		if _, _, err := h.cmd("OPTS UTF8 ON"); err != nil {
			h.close()
			return nil, err
		}
	}
	return h, nil
}

func (h *ftpHasher) setConn(conn io.ReadWriter) {
	h.reader = textproto.NewReader(bufio.NewReader(conn))
	h.writer = textproto.NewWriter(bufio.NewWriter(conn))
}

// errLineBreak is the error for a command with a line break in it, which is
// never sent. The server read the rest as a command of its own, and each
// reply from then on was taken for the one before's: a file got the sum of
// what the rest asked for (L25). The library refuses those on the main
// connection.
var errLineBreak = errors.New("ftp: a name with a line break in it cannot be sent")

func (h *ftpHasher) sendCmd(format string, args ...any) (int, string, error) {
	line := fmt.Sprintf(format, args...)
	shown := line
	if strings.HasPrefix(strings.ToUpper(line), "PASS ") {
		shown = "PASS ***"
	}
	if strings.ContainsAny(line, "\r\n") {
		return 0, "", fmt.Errorf("%w: %q", errLineBreak, shown)
	}
	Log.Add("ftp", DirOut, shown)
	if err := h.writer.PrintfLine("%s", line); err != nil {
		Log.Add("ftp", DirErr, err.Error())
		return 0, "", err
	}
	code, msg, err := h.readResp()
	if err != nil {
		Log.Add("ftp", DirErr, err.Error())
	} else {
		Log.Add("ftp", DirIn, fmt.Sprintf("%d %s", code, msg))
	}
	return code, msg, err
}

func (h *ftpHasher) readResp() (int, string, error) {
	return h.reader.ReadResponse(0)
}

// cmd sends a command and reads the reply. A connection that fails, or a
// server that says it is closing it (421), is errHashConnGone.
func (h *ftpHasher) cmd(format string, args ...any) (int, string, error) {
	code, msg, err := h.sendCmd(format, args...)
	switch {
	case errors.Is(err, errLineBreak):
		return 0, "", err // nothing was sent: the connection is as it was
	case err != nil:
		return 0, "", fmt.Errorf("%w: %w", errHashConnGone, err)
	case code == 421:
		return 0, "", fmt.Errorf("%w: %d %s", errHashConnGone, code, msg)
	}
	return code, msg, nil
}

// sum asks for the checksum of fullPath. A stop of ctx cuts the ask short;
// its reply may still come, so the connection is done with.
func (h *ftpHasher) sum(ctx context.Context, fullPath, algo string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return "", errHashConnGone
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	stop := context.AfterFunc(ctx, func() { h.raw.SetDeadline(time.Now()) })
	sum, err := h.hash(fullPath, algo)
	if !stop() {
		return "", fmt.Errorf("%w: %w", errHashConnGone, ctx.Err())
	}
	return sum, err
}

// close says QUIT politely but with a deadline: it is often called precisely
// because the network is known to be dead. The deadline also ends an ask
// that waits for a reply, so close does not wait long for mu.
func (h *ftpHasher) close() {
	h.raw.SetDeadline(time.Now().Add(2 * time.Second))
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	h.sendCmd("QUIT")
	h.conn.Close()
}

// The algorithms an FTP server sums with that sc compares with its own:
// ChecksumAlgos offers these. XCRC and SHA-512 were read from FEAT too,
// and never offered (L25).
var ftpAlgoName = map[string]string{
	"sha256": "SHA-256",
	"sha1":   "SHA-1",
	"md5":    "MD5",
}

// ftpFeatCmds maps FEAT lines to the algorithm they serve.
var ftpFeatCmds = map[string]string{"XSHA256": "sha256", "XSHA1": "sha1", "XMD5": "md5"}

// ftpHashAlgos maps HASH algorithm names to sc's.
var ftpHashAlgos = map[string]string{"SHA-256": "sha256", "SHA-1": "sha1", "MD5": "md5"}

// ftpSumLen is how many hex digits a sum of each algorithm has.
var ftpSumLen = map[string]int{"sha256": 64, "sha1": 40, "md5": 32}

func (h *ftpHasher) parseFeat(msg string) {
	for _, line := range strings.Split(msg, "\n") {
		upper := strings.ToUpper(strings.TrimSpace(line))
		if upper == "UTF8" {
			h.utf8 = true
			continue
		}
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

// hash asks for the algo sum of fullPath. A sum is taken only when it is
// one of algo: before, a HASH of the server's own choice, after a refused
// OPTS HASH, and any text a 213 reply held were taken for one, and the
// file compared as different from its copy (L25). An answer with no sum of
// algo is final (ErrUnsupported): another try gets the same (L12).
func (h *ftpHasher) hash(fullPath, algo string) (string, error) {
	cmd := h.cmds[algo]
	if cmd == "" {
		return "", model.WithKind(ErrUnsupported, fmt.Errorf("no hash command for %q", algo))
	}
	if cmd != "HASH" {
		code, msg, err := h.cmd("%s %s", cmd, fullPath)
		if err != nil {
			return "", err
		}
		if code != 213 {
			return "", fmt.Errorf("%s: %w", cmd, &textproto.Error{Code: code, Msg: msg})
		}
		sum, _, _ := strings.Cut(strings.TrimSpace(msg), " ")
		return ftpSum(cmd, algo, sum)
	}
	// The choice holds for the connection: it is made again only for
	// another algorithm, not before each file, a round trip each (L25).
	name := ftpAlgoName[algo]
	if h.algo != name {
		code, msg, err := h.cmd("OPTS HASH %s", name)
		if err != nil {
			return "", err
		}
		if code != 200 {
			err := fmt.Errorf("OPTS HASH %s: %w", name, &textproto.Error{Code: code, Msg: msg})
			if code >= 500 { // no algo sum to give by HASH, for any file
				err = model.WithKind(ErrUnsupported, err)
			}
			return "", err
		}
		h.algo = name
	}
	code, msg, err := h.cmd("HASH %s", fullPath)
	if err != nil {
		return "", err
	}
	if code != 213 {
		return "", fmt.Errorf("HASH: %w", &textproto.Error{Code: code, Msg: msg})
	}
	// 213 SHA-256 0-49 <sum> <name>
	fields := strings.Fields(msg)
	if len(fields) < 3 {
		return "", model.WithKind(ErrUnsupported, fmt.Errorf("invalid HASH response: %s", msg))
	}
	if !strings.EqualFold(fields[0], name) {
		return "", model.WithKind(ErrUnsupported, fmt.Errorf("HASH answered with a %s sum, %s was asked for: %s", fields[0], name, msg))
	}
	return ftpSum("HASH", algo, fields[2])
}

// ftpSum returns sum in lower case when it is an algo sum: hex, as many
// digits as algo has.
func ftpSum(cmd, algo, sum string) (string, error) {
	sum = strings.ToLower(sum)
	if _, err := hex.DecodeString(sum); err != nil || len(sum) != ftpSumLen[algo] {
		return "", model.WithKind(ErrUnsupported, fmt.Errorf("%s answered %q, which is no %s sum", cmd, sum, algo))
	}
	return sum, nil
}
