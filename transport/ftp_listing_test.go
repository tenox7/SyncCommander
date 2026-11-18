package transport

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeFTPD is a small FTP server that lists the way vsftpd does by
// default: no MLSD, LIST in ls -l form after a "total" line, and names
// starting with "." only for LIST -a (force_dot_files=NO). With
// dashAIsName it takes no options: it shows every name, and reads "-a dir"
// as a name, answering with an empty listing.
type fakeFTPD struct {
	ls    map[string][]string // folder -> ls -l lines, dot names too
	mlsd  map[string][]string // folder -> MLSD lines; set, the server offers MLSD
	files map[string]fakeFTPFile
	// store holds what STOR wrote, by path; SIZE, RNFR/RNTO and DELE work
	// on it. With noRenameOver, RNTO refuses a name in use, as some
	// servers do.
	store        map[string][]byte
	rmd          []string // the folders RMD removed; the listings stay
	noRenameOver bool
	dashAIsName  bool
	// hashes makes FEAT name XSHA256, which sums a file of store. A sum
	// of hangOn gets no reply; hangAsked hears of it. refuse(n) turns the
	// n-th connection away, the first being 1, as a server at its limit
	// does; featFails(n) closes it at its FEAT, as a server going away.
	// With dropOnSum every sum closes its connection with no reply.
	hashes bool
	// hashFeat is a HASH line for FEAT, as "HASH SHA-1*;SHA-256": HASH
	// sums a file of store with what OPTS HASH chose on its connection,
	// at first the one marked *. With hashRefuses OPTS HASH refuses every
	// choice; hashSays is the algorithm each HASH answers with, whatever
	// was chosen. sumReply, set, is what every sum answers in place of
	// the sum. utf8 makes FEAT name UTF8: a sum of a name beyond ASCII
	// is then found only after OPTS UTF8 ON, as on a server that starts
	// in another code page (L25).
	hashFeat    string
	hashRefuses bool
	hashSays    string
	sumReply    string
	utf8        bool
	// host is the address the server listens on, its data ports too;
	// 127.0.0.1 when empty. heard keeps each connection's command lines.
	host      string
	heard     map[int][]string
	dropOnSum bool
	hangOn    string
	hangAsked chan struct{}
	refuse    func(n int) bool
	featFails func(n int) bool
	// Stalls (L8). With storStall STOR takes its data connection and reads
	// nothing; with listStall LIST sends one line on it and then nothing,
	// the connection left open. hang names a command that gets no reply. A
	// file whose reply is "stall" is sent with its data connection left
	// open and no reply; "none" closes the data connection and sends no
	// reply. retrPace sends a file 1 KiB at a time, this long apart.
	// dataOpen hears the command of each data connection taken. listPace
	// sends a listing a line at a time, this long apart. dropOnce
	// names a command at which the server closes the connection, the first
	// time it comes, as a link that went down.
	dropOnce  string
	dropped   bool
	epsvPort  int // set, the data port EPSV names, one the fake does not serve
	storStall bool
	listStall bool
	hang      string
	retrPace  time.Duration
	listPace  time.Duration
	dataOpen  chan string
	stop      chan struct{} // closed by closeAll; a stall waits on it
	stopOnce  sync.Once
	// mdtm makes FEAT name MDTM: "MDTM path" answers the time in mtimes,
	// and "MDTM time path" sets it, as vsftpd's mdtm_write does. mfmt makes
	// it name MFMT, which sets it too. setTimes hears each command that
	// set a time.
	mdtm, mfmt bool
	// tlsCfg makes it an FTPS server: TLS from the first byte with
	// implicitTLS (ftps://), else after AUTH TLS (ftpes://). Every data
	// connection is TLS then, and dataTLS names each, in order, by its
	// command and whether it resumed a session: "LIST resumed", "STOR
	// new" (L18).
	tlsCfg      *tls.Config
	implicitTLS bool
	dataTLS     []string
	mtimes      map[string]time.Time
	setTimes    []string
	dataConns   []net.Conn
	mu          sync.Mutex
	lists       []string // LIST arguments, in order
	listConns   []int    // the connection each LIST came on
	logins      int      // PASS commands answered
	quits       []int    // the connections that said QUIT
	conns       []net.Conn
	listener    net.Listener
}

// fakeFTPFile is what RETR of a path sends: the body, then the final reply.
type fakeFTPFile struct {
	body, reply string
}

func lsLine(name string, dir bool) string {
	if dir {
		return "drwxr-xr-x    2 1000     1000         4096 Nov 16 10:00 " + name
	}
	return "-rw-r--r--    1 1000     1000            5 Nov 16 10:00 " + name
}

func (f *fakeFTPD) start(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(f.listenHost(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	f.stop = make(chan struct{})
	t.Cleanup(f.closeAll)
	f.listener = l
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			n := len(f.conns)
			refused := f.refuse != nil && f.refuse(n)
			f.mu.Unlock()
			if refused {
				io.WriteString(c, "421 There are too many connections from your internet address.\r\n")
				c.Close()
				continue
			}
			cfg := f.connTLS()
			if cfg != nil && f.implicitTLS {
				c = tls.Server(c, cfg)
			}
			go f.serve(c, n, cfg)
		}
	}()
	return l.Addr().String()
}

// listenHost is the address the server listens on.
func (f *fakeFTPD) listenHost() string {
	if f.host != "" {
		return f.host
	}
	return "127.0.0.1"
}

// heardOn returns the command lines the n-th connection sent.
func (f *fakeFTPD) heardOn(n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.heard[n])
}

// hashAlgos returns the algorithms of hashFeat and the one marked first.
func (f *fakeFTPD) hashAlgos() (algos []string, first string) {
	for _, a := range strings.Split(strings.TrimPrefix(f.hashFeat, "HASH "), ";") {
		name := strings.TrimSuffix(a, "*")
		algos = append(algos, name)
		if first == "" || name != a {
			first = name
		}
	}
	return algos, first
}

// fakeSum is the algo sum of body, algo named as HASH names it.
func fakeSum(algo string, body []byte) string {
	switch algo {
	case "SHA-1":
		return fmt.Sprintf("%x", sha1.Sum(body))
	case "MD5":
		return fmt.Sprintf("%x", md5.Sum(body))
	}
	return fmt.Sprintf("%x", sha256.Sum256(body))
}

// connTLS is the TLS config of one control connection and of its data
// connections; nil for plain FTP. Its session ticket keys are its own, so
// a data connection resumes the session of its own control connection or
// none: ProFTPD asks that of a data connection (L18).
func (f *fakeFTPD) connTLS() *tls.Config {
	if f.tlsCfg == nil {
		return nil
	}
	cfg := f.tlsCfg.Clone()
	var key [32]byte
	rand.Read(key[:])
	cfg.SetSessionTicketKeys([][32]byte{key})
	return cfg
}

// closeAll ends the server: its listener, every connection, every stall.
func (f *fakeFTPD) closeAll() {
	f.stopOnce.Do(func() { close(f.stop) })
	f.listener.Close()
	f.mu.Lock()
	conns := append(slices.Clone(f.conns), f.dataConns...)
	f.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// takeData accepts the data connection of cmd and keeps it for closeAll;
// with tlsCfg, the control connection's, it is TLS.
func (f *fakeFTPD) takeData(pasv net.Listener, cmd string, tlsCfg *tls.Config) (net.Conn, error) {
	dc, err := pasv.Accept()
	pasv.Close()
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		tc := tls.Server(dc, tlsCfg)
		if err := tc.Handshake(); err != nil {
			dc.Close()
			return nil, err
		}
		how := " new"
		if tc.ConnectionState().DidResume {
			how = " resumed"
		}
		f.mu.Lock()
		f.dataTLS = append(f.dataTLS, cmd+how)
		f.mu.Unlock()
		dc = tc
	}
	f.mu.Lock()
	f.dataConns = append(f.dataConns, dc)
	f.mu.Unlock()
	if f.dataOpen != nil {
		select {
		case f.dataOpen <- cmd:
		default:
		}
	}
	return dc, nil
}

// connCount is how many connections the server took or turned away.
func (f *fakeFTPD) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// kill ends the n-th connection the way a server ends an idle one.
func (f *fakeFTPD) kill(n int) {
	f.mu.Lock()
	c := f.conns[n-1]
	f.mu.Unlock()
	io.WriteString(c, "421 Timeout.\r\n")
	c.Close()
}

// quitFrom is whether the n-th connection said QUIT.
func (f *fakeFTPD) quitFrom(n int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.quits, n)
}

func (f *fakeFTPD) loginCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logins
}

func (f *fakeFTPD) listed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.lists)
}

func (f *fakeFTPD) serve(c net.Conn, n int, tlsCfg *tls.Config) {
	defer c.Close()
	r := bufio.NewReader(c)
	reply := func(s string) { io.WriteString(c, s+"\r\n") }
	reply("220 (fake vsFTPd)")
	var pasv net.Listener
	var rest int64
	var from string
	var algo string // what OPTS HASH chose here
	utf8On := false
	defer func() {
		if pasv != nil {
			pasv.Close()
		}
	}()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd, arg, _ := strings.Cut(strings.TrimRight(line, "\r\n"), " ")
		f.mu.Lock()
		hang := f.hang
		if f.heard == nil {
			f.heard = map[int][]string{}
		}
		f.heard[n] = append(f.heard[n], strings.TrimRight(line, "\r\n"))
		f.mu.Unlock()
		if hang != "" && strings.EqualFold(cmd, hang) {
			continue
		}
		if f.dropOnce != "" && strings.EqualFold(cmd, f.dropOnce) {
			f.mu.Lock()
			drop := !f.dropped
			f.dropped = true
			f.mu.Unlock()
			if drop {
				return
			}
		}
		switch strings.ToUpper(cmd) {
		case "USER":
			reply("331 Please specify the password.")
		case "PASS":
			f.mu.Lock()
			f.logins++
			f.mu.Unlock()
			reply("230 Login successful.")
		case "FEAT":
			if f.featFails != nil && f.featFails(n) {
				reply("421 Service not available, closing control connection.")
				return
			}
			feats := " EPSV\r\n PASV\r\n SIZE\r\n"
			if f.mlsd != nil {
				feats += " MLST type*;size*;modify*;\r\n"
			}
			if f.hashes {
				feats += " XSHA256\r\n"
			}
			if f.hashFeat != "" {
				feats += " " + f.hashFeat + "\r\n"
			}
			if f.utf8 {
				feats += " UTF8\r\n"
			}
			if f.mdtm {
				feats += " MDTM\r\n"
			}
			if f.mfmt {
				feats += " MFMT\r\n"
			}
			reply("211-Features:\r\n" + feats + "211 End")
		case "XSHA256":
			p := path.Clean(arg)
			if !f.hashes {
				reply("502 Command not implemented.")
				continue
			}
			f.mu.Lock()
			drop := f.dropOnSum
			f.mu.Unlock()
			if drop {
				return
			}
			if p == f.hangOn {
				if f.hangAsked != nil {
					f.hangAsked <- struct{}{}
				}
				continue
			}
			f.mu.Lock()
			body, ok := f.store[p]
			f.mu.Unlock()
			if !ok || f.utf8 && !utf8On && strings.ContainsFunc(p, func(r rune) bool { return r > 127 }) {
				reply("550 No such file.")
				continue
			}
			if f.sumReply != "" {
				reply("213 " + f.sumReply)
				continue
			}
			reply(fmt.Sprintf("213 %x", sha256.Sum256(body)))
		case "OPTS":
			opt, val, _ := strings.Cut(arg, " ")
			switch {
			case strings.EqualFold(opt, "UTF8") && f.utf8:
				utf8On = strings.EqualFold(val, "ON")
				reply("200 UTF8 set to " + val + ".")
			case strings.EqualFold(opt, "HASH") && f.hashFeat != "":
				algos, _ := f.hashAlgos()
				if f.hashRefuses || !slices.Contains(algos, strings.ToUpper(val)) {
					reply("501 Unknown algorithm, current selection not changed.")
					continue
				}
				algo = strings.ToUpper(val)
				reply("200 " + algo)
			default:
				reply("501 Option not understood.")
			}
		case "HASH":
			if f.hashFeat == "" {
				reply("502 Command not implemented.")
				continue
			}
			if algo == "" {
				_, algo = f.hashAlgos()
			}
			p := path.Clean(arg)
			f.mu.Lock()
			body, ok := f.store[p]
			f.mu.Unlock()
			if !ok || f.utf8 && !utf8On && strings.ContainsFunc(p, func(r rune) bool { return r > 127 }) {
				reply("550 No such file.")
				continue
			}
			says := algo
			if f.hashSays != "" {
				says = f.hashSays
			}
			sum := fakeSum(says, body)
			if f.sumReply != "" {
				sum = f.sumReply
			}
			reply(fmt.Sprintf("213 %s 0-%d %s %s", says, max(len(body)-1, 0), sum, arg))
		case "TYPE":
			reply("200 Switching to Binary mode.")
		case "PWD":
			reply(`257 "/" is the current directory`)
		case "CWD":
			if _, ok := f.ls[path.Clean(arg)]; ok {
				reply("250 Directory successfully changed.")
			} else {
				reply("550 Failed to change directory.")
			}
		case "EPSV":
			if f.epsvPort != 0 {
				reply(fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)", f.epsvPort))
				continue
			}
			if pasv, err = net.Listen("tcp", net.JoinHostPort(f.listenHost(), "0")); err != nil {
				reply("425 Cannot listen.")
				continue
			}
			reply(fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)", pasv.Addr().(*net.TCPAddr).Port))
		case "LIST":
			if pasv == nil {
				reply("425 Use PORT or PASV first.")
				continue
			}
			f.mu.Lock()
			f.lists = append(f.lists, arg)
			f.listConns = append(f.listConns, n)
			f.mu.Unlock()
			reply("150 Here comes the directory listing.")
			dc, err := f.takeData(pasv, "LIST", tlsCfg)
			pasv = nil
			if err != nil {
				continue
			}
			if f.listStall {
				io.WriteString(dc, lsLine("first", false)+"\r\n")
				<-f.stop
				dc.Close()
				return
			}
			for i, l := range f.list(arg) {
				if i > 0 && f.listPace > 0 {
					time.Sleep(f.listPace)
				}
				io.WriteString(dc, l+"\r\n")
			}
			dc.Close()
			reply("226 Directory send OK.")
		case "MLST":
			p := path.Clean(arg)
			facts, ok := f.mlst(p)
			if !ok {
				reply("550 No such file or directory.")
				continue
			}
			reply("250-Listing " + p + "\r\n " + facts + " " + p + "\r\n250 End")
		case "MLSD":
			lines, ok := f.mlsd[path.Clean(arg)]
			if !ok || pasv == nil {
				reply("550 Failed to list.")
				continue
			}
			reply("150 Here comes the listing.")
			dc, err := f.takeData(pasv, "MLSD", tlsCfg)
			pasv = nil
			if err != nil {
				continue
			}
			for _, l := range lines {
				io.WriteString(dc, l+"\r\n")
			}
			dc.Close()
			reply("226 Listing done.")
		case "RETR":
			file, ok := f.files[path.Clean(arg)]
			if !ok || pasv == nil {
				reply("550 Failed to open file.")
				continue
			}
			reply("150 Opening BINARY mode data connection.")
			dc, err := f.takeData(pasv, "RETR", tlsCfg)
			pasv = nil
			if err != nil {
				continue
			}
			for body := file.body; body != ""; {
				n := len(body)
				if f.retrPace > 0 {
					n = min(n, 1024)
				}
				io.WriteString(dc, body[:n])
				body = body[n:]
				if f.retrPace > 0 && body != "" {
					time.Sleep(f.retrPace)
				}
			}
			switch file.reply {
			case "stall":
				<-f.stop
				dc.Close()
				return
			case "none":
				dc.Close()
			default:
				dc.Close()
				reply(file.reply)
			}
		case "SIZE":
			f.mu.Lock()
			body, ok := f.store[path.Clean(arg)]
			f.mu.Unlock()
			if !ok {
				reply("550 Could not get file size.")
				continue
			}
			reply(fmt.Sprintf("213 %d", len(body)))
		case "REST":
			fmt.Sscan(arg, &rest)
			reply("350 Restart position accepted.")
		case "STOR":
			if pasv == nil || f.store == nil {
				reply("553 Could not create file.")
				continue
			}
			reply("150 Ok to send data.")
			dc, err := f.takeData(pasv, "STOR", tlsCfg)
			pasv = nil
			if err != nil {
				continue
			}
			if f.storStall {
				<-f.stop
				dc.Close()
				return
			}
			data, _ := io.ReadAll(dc)
			dc.Close()
			p := path.Clean(arg)
			f.mu.Lock()
			// Like vsftpd: write at the REST offset, past the end too,
			// which leaves a gap of zeros.
			old := f.store[p]
			if int64(len(old)) < rest {
				old = append(old, make([]byte, rest-int64(len(old)))...)
			}
			f.store[p] = append(old[:rest:rest], data...)
			f.mu.Unlock()
			rest = 0
			reply("226 Transfer complete.")
		case "RNFR":
			f.mu.Lock()
			_, ok := f.store[path.Clean(arg)]
			f.mu.Unlock()
			if !ok {
				reply("550 RNFR command failed.")
				continue
			}
			from = path.Clean(arg)
			reply("350 Ready for RNTO.")
		case "RNTO":
			to := path.Clean(arg)
			f.mu.Lock()
			_, taken := f.store[to]
			body, ok := f.store[from]
			if ok && !(taken && f.noRenameOver) {
				f.store[to] = body
				delete(f.store, from)
			}
			f.mu.Unlock()
			if !ok || taken && f.noRenameOver {
				reply("550 Rename failed.")
				continue
			}
			reply("250 Rename successful.")
		case "DELE":
			f.mu.Lock()
			_, ok := f.store[path.Clean(arg)]
			delete(f.store, path.Clean(arg))
			f.mu.Unlock()
			if !ok {
				reply("550 Delete operation failed.")
				continue
			}
			reply("250 Delete operation successful.")
		case "RMD":
			f.mu.Lock()
			f.rmd = append(f.rmd, path.Clean(arg))
			f.mu.Unlock()
			reply("250 Remove directory operation successful.")
		case "QUIT":
			f.mu.Lock()
			f.quits = append(f.quits, n)
			f.mu.Unlock()
			reply("221 Goodbye.")
			return
		case "NOOP":
			reply("200 NOOP ok.")
		case "AUTH":
			if tlsCfg == nil || f.implicitTLS {
				reply("502 Command not implemented.")
				continue
			}
			reply("234 Proceeding with negotiation.")
			c = tls.Server(c, tlsCfg)
			r = bufio.NewReader(c)
		case "PBSZ":
			reply("200 PBSZ set to 0.")
		case "PROT":
			reply("200 PROT now Private.")
		case "MDTM", "MFMT":
			verb := strings.ToUpper(cmd)
			if verb == "MDTM" && !f.mdtm || verb == "MFMT" && !f.mfmt {
				reply("502 Command not implemented.")
				continue
			}
			stamp, p, two := strings.Cut(arg, " ")
			if verb == "MFMT" || two && len(stamp) == 14 && strings.Trim(stamp, "0123456789") == "" {
				t, err := time.ParseInLocation("20060102150405", stamp, time.UTC)
				if err != nil {
					reply("501 Bad time.")
					continue
				}
				f.mu.Lock()
				if f.mtimes == nil {
					f.mtimes = map[string]time.Time{}
				}
				f.mtimes[path.Clean(p)] = t
				f.setTimes = append(f.setTimes, verb+" "+arg)
				f.mu.Unlock()
				reply("213 Modify=" + stamp + "; " + p)
				continue
			}
			f.mu.Lock()
			t, ok := f.mtimes[path.Clean(arg)]
			f.mu.Unlock()
			if !ok {
				reply("550 Could not get file modification time.")
				continue
			}
			reply("213 " + t.UTC().Format("20060102150405"))
		default:
			reply("502 Command not implemented.")
		}
	}
}

// list answers LIST arg: options start with "-", the folder follows.
// mlst returns the facts of p, an absolute path, from its folder's MLSD
// lines; the root is a cdir.
func (f *fakeFTPD) mlst(p string) (string, bool) {
	if p == "/" {
		return "type=cdir;modify=20260101000000;", true
	}
	dir, name := path.Split(p)
	for _, l := range f.mlsd[path.Clean(dir)] {
		if facts, n, ok := strings.Cut(l, " "); ok && n == name {
			return facts, true
		}
	}
	return "", false
}

func (f *fakeFTPD) list(arg string) []string {
	opts, dir := "", arg
	if strings.HasPrefix(arg, "-") && !f.dashAIsName {
		opts, dir, _ = strings.Cut(arg, " ")
	}
	lines, ok := f.ls[path.Clean(dir)]
	if !ok {
		return nil
	}
	out := []string{fmt.Sprintf("total %d", 4*len(lines))}
	for _, l := range lines {
		name := l[strings.LastIndexByte(l, ' ')+1:]
		if strings.HasPrefix(name, ".") && !strings.Contains(opts, "a") && !f.dashAIsName {
			continue
		}
		out = append(out, l)
	}
	return out
}

func openFakeFTP(t *testing.T, f *fakeFTPD, base string) *FTPBackend {
	t.Helper()
	b, err := NewFTPBackend(context.Background(), "ftp://u:p@"+f.start(t)+base, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// L11: an FTP server without MLSD that works like vsftpd lists names
// starting with "." only for LIST -a. sc asks with -a once one listing
// showed the server takes it, so .git and .htaccess are listed instead of
// read as missing, where a mirror would delete them on the other side.
func TestFTPListsDotNamesWithoutMLSD(t *testing.T) {
	dots := []string{lsLine(".", true), lsLine("..", true)}
	f := &fakeFTPD{ls: map[string][]string{
		"/srv":      append(dots, lsLine(".git", true), lsLine(".htaccess", false), lsLine("a.txt", false), lsLine("docs", true)),
		"/srv/docs": append(dots, lsLine(".hidden", false), lsLine("c.txt", false)),
		"/srv/.git": append(dots, lsLine("config", false)),
	}}
	b := openFakeFTP(t, f, "/srv")
	ctx := context.Background()
	for _, dir := range []string{"", "docs", ".git"} {
		want := map[string][]string{"": {".git", ".htaccess", "a.txt", "docs"}, "docs": {".hidden", "c.txt"}, ".git": {"config"}}[dir]
		if names, err := listNames(ctx, b, dir); err != nil || !slices.Equal(names, want) {
			t.Errorf("List(%q) = %q, %v; want %q", dir, names, err, want)
		}
	}
	if got, want := f.listed(), []string{"/srv", "-a /srv", "-a /srv/docs", "-a /srv/.git"}; !slices.Equal(got, want) {
		t.Errorf("LIST arguments %q, want %q", got, want)
	}
}

// L11: a server that takes no options shows every name with LIST and reads
// "-a /srv" as a name it does not have. sc sees the -a listing lose names
// and lists without -a from then on; nothing goes missing.
func TestFTPServerThatTakesNoOptions(t *testing.T) {
	f := &fakeFTPD{dashAIsName: true, ls: map[string][]string{
		"/srv":      {lsLine(".htaccess", false), lsLine("a.txt", false), lsLine("docs", true)},
		"/srv/docs": {lsLine("c.txt", false)},
	}}
	b := openFakeFTP(t, f, "/srv")
	ctx := context.Background()
	for _, dir := range []string{"", "docs"} {
		want := map[string][]string{"": {".htaccess", "a.txt", "docs"}, "docs": {"c.txt"}}[dir]
		if names, err := listNames(ctx, b, dir); err != nil || !slices.Equal(names, want) {
			t.Errorf("List(%q) = %q, %v; want %q", dir, names, err, want)
		}
	}
	if got, want := f.listed(), []string{"/srv", "-a /srv", "/srv/docs"}; !slices.Equal(got, want) {
		t.Errorf("LIST arguments %q, want %q", got, want)
	}
}

// L11: a listing line the FTP library cannot parse fails the listing
// instead of dropping the entry it stood for.
func TestFTPUnparsedListingLineFails(t *testing.T) {
	f := &fakeFTPD{ls: map[string][]string{
		"/srv": {lsLine("a.txt", false), "d????????? ? ? ? ? ? photos", lsLine("docs", true)},
	}}
	b := openFakeFTP(t, f, "/srv")
	names, err := listNames(context.Background(), b, "")
	if err == nil || !strings.Contains(err.Error(), "photos") {
		t.Errorf("List = %q, %v; want an error naming the photos line", names, err)
	}
}
