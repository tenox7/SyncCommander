package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
)

// fakeFTPD is a small FTP server that lists the way vsftpd does by
// default: no MLSD, LIST in ls -l form after a "total" line, and names
// starting with "." only for LIST -a (force_dot_files=NO). With
// dashAIsName it takes no options: it shows every name, and reads "-a dir"
// as a name, answering with an empty listing.
type fakeFTPD struct {
	ls          map[string][]string // folder -> ls -l lines, dot names too
	files       map[string]fakeFTPFile
	dashAIsName bool
	mu          sync.Mutex
	lists       []string // LIST arguments, in order
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
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return l.Addr().String()
}

func (f *fakeFTPD) listed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.lists)
}

func (f *fakeFTPD) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	reply := func(s string) { io.WriteString(c, s+"\r\n") }
	reply("220 (fake vsFTPd)")
	var pasv net.Listener
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
		switch strings.ToUpper(cmd) {
		case "USER":
			reply("331 Please specify the password.")
		case "PASS":
			reply("230 Login successful.")
		case "FEAT":
			reply("211-Features:\r\n EPSV\r\n PASV\r\n SIZE\r\n211 End")
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
			f.mu.Unlock()
			reply("150 Here comes the directory listing.")
			dc, err := pasv.Accept()
			pasv.Close()
			pasv = nil
			if err != nil {
				continue
			}
			for _, l := range f.list(arg) {
				io.WriteString(dc, l+"\r\n")
			}
			dc.Close()
			reply("226 Directory send OK.")
		case "RETR":
			file, ok := f.files[path.Clean(arg)]
			if !ok || pasv == nil {
				reply("550 Failed to open file.")
				continue
			}
			reply("150 Opening BINARY mode data connection.")
			dc, err := pasv.Accept()
			pasv.Close()
			pasv = nil
			if err != nil {
				continue
			}
			io.WriteString(dc, file.body)
			dc.Close()
			reply(file.reply)
		case "QUIT":
			reply("221 Goodbye.")
			return
		default:
			reply("502 Command not implemented.")
		}
	}
}

// list answers LIST arg: options start with "-", the folder follows.
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
	b, err := NewFTPBackend("ftp://u:p@"+f.start(t)+base, false, 1)
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
