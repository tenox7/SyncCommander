package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
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
	pool    *connPool[*ftpConn]
	mu      sync.Mutex
	dirs    sync.Map // directories known to exist, so uploads skip the MKD dance
	capture *listCapture
	// dashA is whether LIST takes -a on a server without MLSD: 0 not known
	// yet, 1 yes, -1 no. See listLocked. Guarded by mu.
	dashA int
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
func dialFTPConn(addr, scheme, user, pass string, tlsCfg *tls.Config, lc *listCapture) (*ftp.ServerConn, net.Conn, error) {
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
		if n > 1 && lc != nil {
			c = &captureConn{Conn: c, lc: lc}
		}
		return c, nil
	}
	opts := []ftp.DialOption{ftp.DialWithTimeout(10 * time.Second), ftp.DialWithDialFunc(dialFunc)}
	if lc != nil {
		opts = append(opts, ftp.DialWithDebugOutput(lc))
	}
	switch scheme {
	case "ftps":
		opts = append(opts, ftp.DialWithTLS(tlsCfg))
	case "ftpes":
		opts = append(opts, ftp.DialWithExplicitTLS(tlsCfg))
	}
	conn, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("ftp dial %s: %w", addr, err)
	}
	if err := conn.Login(user, pass); err != nil {
		conn.Quit()
		err = fmt.Errorf("ftp login %s@%s: %w", user, addr, err)
		// 530: not logged in, the server refused the user or the password.
		if tp := (*textproto.Error)(nil); errors.As(err, &tp) && tp.Code == ftp.StatusNotLoggedIn {
			err = model.WithKind(model.ErrAuth, err)
		}
		return nil, nil, err
	}
	if err := conn.Type(ftp.TransferTypeBinary); err != nil {
		conn.Quit()
		return nil, nil, fmt.Errorf("ftp binary mode: %w", err)
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

	capture := &listCapture{}
	conn, rawConn, err := dialFTPConn(addr, scheme, user, pass, tlsCfg, capture)
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
		capture: capture,
	}

	// Extra connections for parallel transfers. Primary is nil: when the pool
	// is at capacity acquireExtra returns false and the caller serializes on
	// the primary conn under b.mu. maxExtras=0 (parallel<=1) means every
	// transfer takes the primary path.
	dial := func(context.Context) (*ftpConn, error) {
		c, raw, derr := dialFTPConn(addr, scheme, user, pass, tlsCfg, nil)
		if derr != nil {
			return nil, derr
		}
		Log.Add("ftp", DirIn, "extra connection dialed")
		return &ftpConn{conn: c, rawConn: raw, lastUsed: time.Now()}, nil
	}
	b.pool = newConnPool[*ftpConn](nil, parallel-1, dial, func(c *ftpConn) { c.close() })
	return b, nil
}

// abortOnCancel returns a stop func that, while running, closes the raw
// control conn when ctx is canceled. Closing the underlying net.Conn
// unblocks blocked Stor/Retr/List/etc by failing the next read/write on
// the control conn. Data conns (which the library opens separately) get
// TCP keepalive via the shared Dialer, so dead peers there are detected
// at the OS level.
func (b *FTPBackend) abortOnCancel(ctx context.Context) func() {
	return CancelCloser(ctx, b.rawConn)
}

func (b *FTPBackend) BasePath() string { return b.display }

// SetParallel keeps n-1 extra connections; the n-th transfer shares the
// primary under b.mu.
func (b *FTPBackend) SetParallel(n int) { b.pool.resize(n - 1) }

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
	c, poolRelease := b.pool.acquire(ctx)
	if c == nil {
		poolRelease()
		return nil, nil, false
	}
	// On cancel, CancelCloser force-closes this conn's control socket, so it
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
		conn, raw, err := dialFTPConn(b.addr, b.scheme, b.user, b.pass, b.tlsCfg, nil)
		if err != nil {
			Log.Add("ftp", DirErr, "extra reconnect failed: "+err.Error())
			c.lastUsed = time.Time{} // still dead: keep the probe armed
			poolRelease()
			return nil, nil, false
		}
		c.conn.Quit()
		c.conn, c.rawConn, c.lastUsed = conn, raw, time.Now()
		Log.Add("ftp", DirIn, "extra reconnected")
	}
	return c, release, true
}

func (b *FTPBackend) reconnectLocked() error {
	b.conn.Quit()
	conn, rawConn, err := dialFTPConn(b.addr, b.scheme, b.user, b.pass, b.tlsCfg, b.capture)
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
	Log.Add("ftp", DirIn, "reconnected")
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
	Log.Add("ftp", DirErr, "connection lost, reconnecting...")
	if rerr := b.reconnectLocked(); rerr != nil {
		Log.Add("ftp", DirErr, "reconnect failed: "+rerr.Error())
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
	dir, entries, err := b.rawListLocked(ctx, relDir)
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
		result = append(result, b.entryLocked(path.Join(relDir, e.Name), path.Join(dir, e.Name), e))
	}
	return result, nil
}

// rawListLocked lists relDir, at dir on the server, as the library reads it.
func (b *FTPBackend) rawListLocked(ctx context.Context, relDir string) (dir string, entries []*ftp.Entry, err error) {
	if ctx.Err() != nil {
		return "", nil, ctx.Err()
	}
	dir = path.Join(b.base, relDir)
	entries, err = b.listLocked(dir)
	if err == nil && len(entries) == 0 {
		// Some servers answer an empty 226 for a missing dir; CWD tells them apart.
		err = b.conn.ChangeDir(dir)
	}
	if err != nil {
		Log.Add("ftp", DirErr, err.Error())
		return "", nil, err
	}
	Log.Add("ftp", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return dir, entries, nil
}

// entryLocked is the row of relPath, at p on the server, which a listing or
// MLST described as e. MLSD and MLST carry exact times; only a listing that
// came from LIST costs one MDTM, per entry.
func (b *FTPBackend) entryLocked(relPath, p string, e *ftp.Entry) model.FileEntry {
	mode := os.FileMode(0644)
	switch e.Type {
	case ftp.EntryTypeFolder:
		mode = os.ModeDir | 0755
	case ftp.EntryTypeLink:
		mode = os.ModeSymlink | 0777 // listed, never read or written through (L2)
	case ftpEntrySpecial:
		mode = os.ModeIrregular | 0644
	}
	modTime := e.Time
	if b.conn.IsGetTimeSupported() && !b.conn.IsTimePreciseInList() {
		if t, err := b.conn.GetTime(p); err == nil {
			modTime = t
		}
	}
	fe := model.FileEntry{
		RelPath: relPath,
		Name:    path.Base(relPath),
		Size:    int64(e.Size),
		ModTime: modTime,
		IsDir:   e.Type == ftp.EntryTypeFolder,
		Mode:    mode,
	}
	if relPath == "" {
		fe.Name = ""
	}
	return fe
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
	p := path.Join(b.base, relPath)
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
	return b.entryLocked(relPath, p, e), nil
}

func (b *FTPBackend) statByListingLocked(ctx context.Context, relPath, p string) (model.FileEntry, error) {
	if relPath == "" {
		if err := b.conn.ChangeDir(p); err != nil {
			return model.FileEntry{}, ftpStatErr(relPath, err)
		}
		return model.FileEntry{IsDir: true, Mode: os.ModeDir | 0755}, nil
	}
	dir, entries, err := b.rawListLocked(ctx, model.DirOf(relPath))
	if err != nil {
		return model.FileEntry{}, ftpStatErr(relPath, err)
	}
	name := path.Base(relPath)
	for _, e := range entries {
		if e.Name == name {
			return b.entryLocked(relPath, path.Join(dir, name), e), nil
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

// listLocked lists dir on the primary connection; b.mu is held. A server
// without MLSD gets LIST, and servers that work like ls, vsftpd by default,
// then leave out names starting with "." unless asked with LIST -a. A server
// that takes no options could read "-a" as a name and answer with nothing.
// So the first listing that has entries runs both forms, and -a is kept
// only when its listing is the plain one plus names starting with ".".
func (b *FTPBackend) listLocked(dir string) ([]*ftp.Entry, error) {
	if b.conn.IsTimePreciseInList() || b.dashA < 0 {
		return b.listChecked(dir)
	}
	if b.dashA > 0 {
		return b.listChecked("-a " + dir)
	}
	plain, err := b.listChecked(dir)
	if err != nil {
		return nil, err
	}
	all, err := b.listChecked("-a " + dir)
	var reply *textproto.Error
	switch {
	case err != nil && !errors.As(err, &reply):
		return nil, err // the connection failed, not the -a
	case err != nil || !onlyAddsDotNames(plain, all):
		b.dashA = -1
		if !hasDotName(plain) {
			Log.Add("ftp", DirWarn, `this server has no MLSD and does not take LIST -a: names starting with "." may be left out of its listings`)
		}
		return plain, nil
	case len(realEntries(plain)) > 0:
		b.dashA = 1
	}
	return all, nil
}

// listChecked runs one LIST or MLSD. The library drops a line it cannot
// parse; such a line is an entry the listing would lack, so it fails the
// listing here.
func (b *FTPBackend) listChecked(arg string) ([]*ftp.Entry, error) {
	Log.Add("ftp", DirOut, "LIST "+arg)
	b.capture.start()
	entries, err := b.conn.List(arg)
	raw := b.capture.stop()
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
// first space, as the library takes it.
func markMLSDTypes(raw []byte, entries []*ftp.Entry) {
	var special map[string]ftp.EntryType
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
		}
	}
	for _, e := range entries {
		if t, ok := special[e.Name]; ok {
			e.Type = t
		}
	}
}

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
// runs on the primary connection. The library reads it on the List
// caller's goroutine, so no lock is needed around the buffer.
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
		Log.Add("ftp", DirErr, "MFMT "+relPath+": "+err.Error())
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
	defer CancelCloser(ctx, raw)()
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

func (b *FTPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	Log.Add("ftp", DirOut, "STOR "+relPath)
	return b.store(ctx, relPath, src, 0)
}

// AppendFrom writes the source from offset on at the end of relPath, which
// must hold exactly offset bytes by SIZE (L14, S8): REST past the end of a
// shorter file leaves a gap of zeros. A server without SIZE gets no append.
func (b *FTPBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, _ os.FileMode, offset int64) error {
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
		n, err = b.conn.FileSize(path.Join(b.base, relPath))
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
	part, final := path.Join(b.base, model.PartPath(relPath)), path.Join(b.base, relPath)
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
	fullPath := path.Join(b.base, relPath)
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
	if taken, err := nameTaken(ctx, b.List, newRelPath); err != nil {
		return err
	} else if taken {
		return renameTargetTaken("ftp", newRelPath)
	}
	b.dirs.Clear()
	err := b.withPrimary(ctx, func() error {
		return b.conn.Rename(path.Join(b.base, oldRelPath), path.Join(b.base, newRelPath))
	})
	if err != nil {
		Log.Add("ftp", DirErr, "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) Remove(ctx context.Context, relPath string) error {
	err := b.withPrimary(ctx, func() error { return b.conn.Delete(path.Join(b.base, relPath)) })
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
	err := b.withPrimary(ctx, func() error { return b.removeAllLocked(path.Join(b.base, relPath)) })
	if err != nil {
		Log.Add("ftp", DirErr, "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) removeAllLocked(fullPath string) error {
	entries, err := b.listLocked(fullPath)
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
	Log.Add("ftp", DirOut, fmt.Sprintf("RETR %s @%d", relPath, offset))
	fullPath := path.Join(b.base, relPath)
	if ex, release, ok := b.acquireExtra(ctx); ok {
		resp, err := retrFrom(ex.conn, fullPath, offset)
		if err != nil {
			release()
			Log.Add("ftp", DirErr, err.Error())
			return nil, err
		}
		return &ftpReader{rc: resp, stop: CancelCloser(ctx, ex.rawConn), done: release}, nil
	}
	b.mu.Lock()
	resp, err := retrFrom(b.conn, fullPath, offset)
	if err != nil {
		b.mu.Unlock()
		Log.Add("ftp", DirErr, err.Error())
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

func (r *ftpReader) Close() error {
	r.once.Do(func() {
		r.stop()
		r.err = r.shutdown()
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
	Log.Add("ftp", DirOut, cmd)
	if err := h.writer.PrintfLine(format, args...); err != nil {
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
