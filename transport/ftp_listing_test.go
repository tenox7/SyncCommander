package transport

import (
	"bufio"
	"context"
	"crypto/sha256"
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
	noRenameOver bool
	dashAIsName  bool
	// hashes makes FEAT name XSHA256, which sums a file of store. A sum
	// of hangOn gets no reply; hangAsked hears of it. refuse(n) turns the
	// n-th connection away, the first being 1, as a server at its limit
	// does; featFails(n) closes it at its FEAT, as a server going away.
	// With dropOnSum every sum closes its connection with no reply.
	hashes    bool
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
	// dataOpen hears the command of each data connection taken. dropOnce
	// names a command at which the server closes the connection, the first
	// time it comes, as a link that went down.
	dropOnce  string
	dropped   bool
	epsvPort  int // set, the data port EPSV names, one the fake does not serve
	storStall bool
	listStall bool
	hang      string
	retrPace  time.Duration
	dataOpen  chan string
	stop      chan struct{} // closed by closeAll; a stall waits on it
	stopOnce  sync.Once
	dataConns []net.Conn
	mu        sync.Mutex
	lists     []string // LIST arguments, in order
	listConns []int    // the connection each LIST came on
	logins    int      // PASS commands answered
	quits     []int    // the connections that said QUIT
	conns     []net.Conn
	listener  net.Listener
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
	l, err := net.Listen("tcp", "127.0.0.1:0")
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
			go f.serve(c, n)
		}
	}()
	return l.Addr().String()
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

// takeData accepts the data connection of cmd and keeps it for closeAll.
func (f *fakeFTPD) takeData(pasv net.Listener, cmd string) (net.Conn, error) {
	dc, err := pasv.Accept()
	pasv.Close()
	if err != nil {
		return nil, err
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

func (f *fakeFTPD) serve(c net.Conn, n int) {
	defer c.Close()
	r := bufio.NewReader(c)
	reply := func(s string) { io.WriteString(c, s+"\r\n") }
	reply("220 (fake vsFTPd)")
	var pasv net.Listener
	var rest int64
	var from string
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
			if !ok {
				reply("550 No such file.")
				continue
			}
			reply(fmt.Sprintf("213 %x", sha256.Sum256(body)))
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
			if pasv, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
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
			dc, err := f.takeData(pasv, "LIST")
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
			for _, l := range f.list(arg) {
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
			dc, err := f.takeData(pasv, "MLSD")
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
			dc, err := f.takeData(pasv, "RETR")
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
			dc, err := f.takeData(pasv, "STOR")
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
		case "QUIT":
			f.mu.Lock()
			f.quits = append(f.quits, n)
			f.mu.Unlock()
			reply("221 Goodbye.")
			return
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
