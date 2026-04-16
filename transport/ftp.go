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
	base     string
	conn     *ftp.ServerConn
	hasher   *ftpHasher
	display  string
	addr     string
	dialOpts []ftp.DialOption
	user     string
	pass     string
	scheme   string
	host     string
	port     string
	tlsCfg   *tls.Config
}

func NewFTPBackend(rawURL string, insecure bool) (*FTPBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if user == "" {
		user = "anonymous"
	}
	if pass == "" && user == "anonymous" {
		pass = "sc@"
	}

	var opts []ftp.DialOption
	opts = append(opts, ftp.DialWithTimeout(10*time.Second))

	tlsCfg := &tls.Config{ServerName: host, InsecureSkipVerify: insecure}

	switch scheme {
	case "ftps":
		if port == "" {
			port = "990"
		}
		opts = append(opts, ftp.DialWithTLS(tlsCfg))
	case "ftpes":
		if port == "" {
			port = "21"
		}
		opts = append(opts, ftp.DialWithExplicitTLS(tlsCfg))
	default:
		if port == "" {
			port = "21"
		}
	}

	addr := host + ":" + port
	conn, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("ftp dial %s: %v", addr, err)
	}

	if err := conn.Login(user, pass); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("ftp login %s@%s: %v", user, addr, err)
	}

	if err := conn.Type(ftp.TransferTypeBinary); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("ftp binary mode: %v", err)
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

	return &FTPBackend{
		base:     remotePath,
		conn:     conn,
		hasher:   hasher,
		display:  fmt.Sprintf("%s://%s@%s%s", scheme, user, displayHost, remotePath),
		addr:     addr,
		dialOpts: opts,
		user:     user,
		pass:     pass,
		scheme:   scheme,
		host:     host,
		port:     port,
		tlsCfg:   tlsCfg,
	}, nil
}

func (b *FTPBackend) BasePath() string { return b.display }

func (b *FTPBackend) Close() error {
	if b.hasher != nil {
		b.hasher.close()
	}
	return b.conn.Quit()
}

func (b *FTPBackend) connAlive() bool {
	ch := make(chan error, 1)
	go func() { ch <- b.conn.NoOp() }()
	select {
	case err := <-ch:
		return err == nil
	case <-time.After(3 * time.Second):
		return false
	}
}

func (b *FTPBackend) reconnect() error {
	b.conn.Quit()
	conn, err := ftp.Dial(b.addr, b.dialOpts...)
	if err != nil {
		return err
	}
	if err := conn.Login(b.user, b.pass); err != nil {
		conn.Quit()
		return err
	}
	conn.Type(ftp.TransferTypeBinary)
	b.conn = conn
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
	result, err := b.listDir(ctx, relDir)
	if err != nil && ctx.Err() == nil && !b.connAlive() {
		Log.Add("ftp", "ERR", "connection lost, reconnecting...")
		if rerr := b.reconnect(); rerr != nil {
			Log.Add("ftp", "ERR", "reconnect failed: "+rerr.Error())
			return nil, err
		}
		return b.listDir(ctx, relDir)
	}
	return result, err
}

func (b *FTPBackend) listDir(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := path.Join(b.base, relDir)
	Log.Add("ftp", ">>>", "LIST "+dir)
	type listResult struct {
		entries []*ftp.Entry
		err     error
	}
	ch := make(chan listResult, 1)
	go func() {
		e, err := b.conn.List(dir)
		ch <- listResult{e, err}
	}()
	var entries []*ftp.Entry
	var err error
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		entries, err = r.entries, r.err
	}
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
	err := b.conn.SetTime(path.Join(b.base, relPath), mtime)
	if err != nil {
		Log.Add("ftp", "ERR", "MFMT "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) CopyFrom(_ context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	Log.Add("ftp", ">>>", "STOR "+relPath)
	fullPath := path.Join(b.base, relPath)
	b.mkdirAll(path.Dir(fullPath))
	err := b.conn.Stor(fullPath, src)
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
	}
	return err
}

func (b *FTPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	err := b.conn.Rename(path.Join(b.base, oldRelPath), path.Join(b.base, newRelPath))
	if err != nil {
		Log.Add("ftp", "ERR", "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) Remove(_ context.Context, relPath string) error {
	err := b.conn.Delete(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("ftp", "ERR", "DELETE "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) RemoveAll(_ context.Context, relPath string) error {
	err := b.removeAll(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("ftp", "ERR", "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

func (b *FTPBackend) removeAll(fullPath string) error {
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
			if err := b.removeAll(child); err != nil {
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

func (b *FTPBackend) Open(_ context.Context, relPath string) (io.ReadCloser, error) {
	Log.Add("ftp", ">>>", "RETR "+relPath)
	resp, err := b.conn.Retr(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("ftp", "ERR", err.Error())
		return nil, err
	}
	return resp, nil
}

func (b *FTPBackend) mkdirAll(dir string) {
	if dir == "/" || dir == "." || dir == "" {
		return
	}
	b.conn.MakeDir(dir)
	if _, err := b.conn.List(dir); err != nil {
		b.mkdirAll(path.Dir(dir))
		b.conn.MakeDir(dir)
	}
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
