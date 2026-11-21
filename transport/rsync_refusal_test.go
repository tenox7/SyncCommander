package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"sc/model"
)

// fakeRsyncd answers each connection as a daemon at login: its greeting,
// then, after the module line, reply. It counts the connections.
func fakeRsyncd(t *testing.T, reply string) (string, *atomic.Int64) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	conns := new(atomic.Int64)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			conns.Add(1)
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				io.WriteString(c, "@RSYNCD: 31.0\n")
				r.ReadString('\n') // the client's version
				r.ReadString('\n') // the module
				io.WriteString(c, reply+"\n")
				r.ReadString('\n') // an answer to a challenge
			}()
		}
	}()
	return l.Addr().String(), conns
}

// R18: a daemon's refusal at login read as a passing failure: each call
// was six connects, some 15 s, each one more refused login, which can get
// an address banned. A refusal is final now, whatever its reason, and the
// backend connects no more: the next call fails with no connect.
func TestRsyncLoginRefusalIsFinal(t *testing.T) {
	t.Setenv("RSYNC_PASSWORD", "")
	for _, reply := range []string{
		"@ERROR: Unknown module 'm'",
		"@ERROR: access denied to m from localhost (127.0.0.1)",
		"@ERROR: auth failed on module m",
		"@ERROR: chdir failed",
		"@ERROR: Unknown module 'max connections'", // a refusal, whatever the name
		"@RSYNCD: AUTHREQD abcdefghijklmnopqrstuv", // and no password to give
	} {
		t.Run(reply, func(t *testing.T) {
			addr, conns := fakeRsyncd(t, reply)
			url := "rsync://" + addr + "/m/"
			b := NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
			defer CloseBackend(b)
			for i := range 2 {
				finalAt(t, fmt.Sprintf("list %d", i+1), model.ErrAuth, func(ctx context.Context) error {
					_, err := b.List(ctx, "")
					return err
				})
			}
			if n := conns.Load(); n != 1 {
				t.Errorf("%d connects for two lists; want 1", n)
			}
		})
	}
}

// A URL with no module, which no try can use, was tried six times a call.
func TestRsyncURLWithNoModuleIsFinal(t *testing.T) {
	url := "rsync://127.0.0.1:1/"
	b := NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
	defer CloseBackend(b)
	finalAt(t, "list", fs.ErrInvalid, func(ctx context.Context) error {
		_, err := b.List(ctx, "")
		return err
	})
}

// A file gone since the scan: stock rsyncd says it is not there, then
// resets the connection, and its md4 was asked six times, some 15 s.
// Not there is a final answer now, for an md4 and a download alike.
func TestRsyncGoneFileIsNotThere(t *testing.T) {
	port, dir := startRsyncDaemon(t)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod/", port)
	b := NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
	defer CloseBackend(b)
	mark := len(Log.Lines())
	finalAt(t, "md4 of gone.txt", fs.ErrNotExist, func(ctx context.Context) error {
		_, err := b.Checksum(ctx, "gone.txt", "md4")
		return err
	})
	finalAt(t, "open gone.txt", fs.ErrNotExist, openOnce(b, "gone.txt"))
	finalAt(t, "open gone.txt at 2", fs.ErrNotExist, func(ctx context.Context) error {
		rc, err := b.(interface {
			OpenAt(context.Context, string, int64) (io.ReadCloser, error)
		}).OpenAt(ctx, "gone.txt", 2)
		if err == nil {
			rc.Close()
		}
		return err
	})
	if n := logCount(mark, " RETRY "); n != 0 {
		t.Errorf("%d tries again; want none", n)
	}
	if sum, err := b.Checksum(context.Background(), "a.txt", "md4"); err != nil || len(sum) != 32 {
		t.Errorf("md4 of a.txt = %q, %v; want its sum", sum, err)
	}
}

// "Not there" is read where the daemon gives its reason, at the end of
// its message: a file that could not be read, named "no such file or
// directory", was not there.
func TestRsyncNotThereReadsTheReason(t *testing.T) {
	for _, c := range []struct {
		msgs []string
		want bool
	}{
		{[]string{`[sender] link_stat "gone.txt" (in testmod) failed: No such file or directory (2)`}, true},
		{[]string{`link_stat "/m/gone.txt" failed: lstat /m/gone.txt: no such file or directory`}, true},
		{[]string{`[sender] send_files failed to open "no such file or directory" (in testmod): Permission denied (13)`}, false},
		{[]string{`[sender] link_stat "a" (in m) failed: No such file or directory (2)`, `[sender] read errors mapping "b" (in m): Input/output error (5)`}, false},
		{nil, false},
	} {
		if got := notThere(c.msgs); got != c.want {
			t.Errorf("notThere(%q) = %v; want %v", c.msgs, got, c.want)
		}
	}
}

// A copy stats each folder it makes on the daemon, which is not there yet.
// Stock rsyncd says so, then resets the connection, and each such STAT was
// an ERR line that opened the log. "Not there" is an answer, no error.
func TestRsyncStatOfAMissingFolderIsNoError(t *testing.T) {
	port, _ := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	mark := len(Log.Lines())
	for range 3 {
		if _, err := b.Stat(context.Background(), "d"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat of a missing folder: %v; want not there", err)
		}
	}
	if n := logCount(mark, " ERR "); n != 0 {
		t.Errorf("%d ERR lines for 3 stats of a missing folder; want none", n)
	}
}

// A run the caller stops, as a scan that is done stops its preload, is no
// error: it showed as err:1 at each start on an empty module.
func TestRsyncCanceledRunIsNoError(t *testing.T) {
	port, _ := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mark := len(Log.Lines())
	if _, err := b.runDaemon(ctx, "RLIST", []string{"-n", "-r", "-l"}, "testmod/", nil); err == nil {
		t.Fatal("a canceled run did not fail")
	}
	if n := logCount(mark, " ERR "); n != 0 {
		t.Errorf("%d ERR lines for a canceled run; want none", n)
	}
}
