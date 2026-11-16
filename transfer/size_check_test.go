package transfer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// serveShortFTP is a minimal FTP server with one file, big.bin, listed as
// listed bytes. A download sends sent bytes, closes the data connection
// cleanly, then answers verdict ("426" for aborted, "226" for complete).
func serveShortFTP(c net.Conn, listed, sent int, verdict string) {
	defer c.Close()
	r := bufio.NewReader(c)
	say := func(s string) { fmt.Fprintf(c, "%s\r\n", s) }
	say("220 fake ftp")
	var dl net.Listener
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd, _, _ := strings.Cut(strings.TrimRight(line, "\r\n"), " ")
		switch strings.ToUpper(cmd) {
		case "USER":
			say("331 password please")
		case "PASS":
			say("230 logged in")
		case "FEAT":
			c.Write([]byte("211-Features:\r\n MLST type*;size*;modify*;\r\n UTF8\r\n211 End\r\n"))
		case "OPTS", "TYPE", "NOOP":
			say("200 ok")
		case "PWD":
			say(`257 "/" is the current directory`)
		case "CWD":
			say("250 ok")
		case "EPSV":
			dl, _ = net.Listen("tcp", "127.0.0.1:0")
			say(fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)", dl.Addr().(*net.TCPAddr).Port))
		case "MLSD":
			say("150 here comes the listing")
			dc, _ := dl.Accept()
			fmt.Fprintf(dc, "type=file;size=%d;modify=20260101000000; big.bin\r\n", listed)
			dc.Close()
			dl.Close()
			say("226 listing done")
		case "REST":
			say("350 ok")
		case "RETR":
			say("150 opening data connection")
			dc, _ := dl.Accept()
			dc.Write(bytes.Repeat([]byte{'x'}, sent))
			dc.Close()
			dl.Close()
			if verdict == "426" {
				say("426 Connection closed; transfer aborted.")
			} else {
				say("226 Transfer complete.")
			}
		case "QUIT":
			say("221 bye")
			return
		default:
			say("502 not implemented")
		}
	}
}

// copyOK reports whether the log since mark says relPath was copied.
func copyOK(mark int, relPath string) bool {
	for _, l := range transport.Log.Lines()[mark:] {
		if strings.Contains(l, "COPY "+relPath+" OK") {
			return true
		}
	}
	return false
}

// L3, F14: an FTP server that gives up half way ends the data connection
// as for a whole file and says 426 only on the control connection, which sc
// read only in Close, whose error the copy dropped. Nothing compared the
// bytes with the listed size either: the short file was "COPY big.bin OK"
// and got the source's date. A server that says 226 after a short file is
// caught by the size alone.
func TestFTPShortDownloadFails(t *testing.T) {
	defer transport.SetMaxRetries(transport.MaxRetries())
	transport.SetMaxRetries(1)
	for _, verdict := range []string{"426", "226"} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go serveShortFTP(c, 1_000_000, 400_000, verdict)
			}
		}()
		src, err := transport.NewFTPBackend(context.Background(), "ftp://user:pw@"+ln.Addr().String()+"/", false, 1)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		s := &scenario{right: dir, lb: src, rb: transport.NewLocalBackend(dir)}
		s.sc = model.NewScanner(s.lb, s.rb, 1, 1, true)
		s.sc.Scan(context.Background(), scenarioOpts)
		mark := len(transport.Log.Lines())
		p := s.copy(t, "big.bin", true, false)
		if p.Failed.Load() != 1 || copyOK(mark, "big.bin") {
			t.Errorf("%s after 400000 of 1000000 bytes: failed=%d, logged as copied=%v; want a failed copy", verdict, p.Failed.Load(), copyOK(mark, "big.bin"))
		}
		if fi, err := os.Stat(filepath.Join(dir, "big.bin")); err == nil && fi.ModTime().Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("%s: the short file got the source's date", verdict)
		}
		transport.CloseBackend(src)
		ln.Close()
	}
}

// F14: a source that changed size after the scan gives another number of
// bytes than the listing, with no error. The copy took what it got, called
// it done and gave it the listed date, so the next compare could call the
// two sides equal. part.txt resumes the part a cut-off copy left (its bytes
// are the source's new first bytes), so the check runs on a resumed copy
// too. A failed copy never takes the file's name (L14).
func TestCopyOfSourceThatChangedSizeFails(t *testing.T) {
	defer transport.SetMaxRetries(transport.MaxRetries())
	transport.SetMaxRetries(1)
	for _, tc := range []struct {
		name, now string
	}{
		{"shrank", strings.Repeat("s", 500)},
		{"grew", strings.Repeat("g", 1500)},
	} {
		root := t.TempDir()
		left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
		writeFile(t, filepath.Join(left, "f.txt"), strings.Repeat("a", 1000))
		writeFile(t, filepath.Join(left, "part.txt"), strings.Repeat("b", 1000))
		writeFile(t, filepath.Join(right, model.PartPath("part.txt")), tc.now[:300])
		s := newScenario(t, left, right)
		for _, f := range []string{"f.txt", "part.txt"} {
			writeFile(t, filepath.Join(left, f), tc.now)
			mark := len(transport.Log.Lines())
			p := s.copy(t, f, true, false)
			if p.Failed.Load() != 1 || copyOK(mark, f) {
				t.Errorf("%s: source %s since the scan: failed=%d, logged as copied=%v; want a failed copy", f, tc.name, p.Failed.Load(), copyOK(mark, f))
			}
			if f == "part.txt" && !logged(mark, "COPY part.txt: resuming after") {
				t.Errorf("%s: the copy did not resume the part", f)
			}
			if onDisk(filepath.Join(right, f)) {
				t.Errorf("%s: source %s since the scan: the failed copy took the file's name", f, tc.name)
			}
		}
	}
}

// F14 on the rsync path: a direct send from a local file checks that the
// file still has the listed size.
func TestRsyncSendOfSourceThatChangedSizeFails(t *testing.T) {
	defer transport.SetMaxRetries(transport.MaxRetries())
	transport.SetMaxRetries(1)
	url, mod := startRsyncd(t)
	left := t.TempDir()
	writeFile(t, filepath.Join(left, "f.txt"), strings.Repeat("a", 1000))
	rb, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	s := &scenario{left: left, lb: transport.NewLocalBackend(left), rb: rb}
	s.sc = model.NewScanner(s.lb, s.rb, 1, 1, true)
	s.sc.Scan(context.Background(), scenarioOpts)
	writeFile(t, filepath.Join(left, "f.txt"), strings.Repeat("s", 500))
	mark := len(transport.Log.Lines())
	p := s.copy(t, "f.txt", true, false)
	if p.Failed.Load() != 1 || copyOK(mark, "f.txt") {
		t.Errorf("rsync send of a source that shrank since the scan: failed=%d, logged as copied=%v; want a failed copy (server holds %d bytes)", p.Failed.Load(), copyOK(mark, "f.txt"), fileSize(filepath.Join(mod, "f.txt")))
	}
}

func fileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		return -1
	}
	return fi.Size()
}
