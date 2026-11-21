package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jlaffaye/ftp"

	"sc/model"
)

// fullRsyncd is a daemon that takes maxConns logins at a time and turns
// the others away as rsyncd does past "max connections". A login it takes
// stays open until the client closes it.
type fullRsyncd struct {
	addr    string
	mu      sync.Mutex
	open    int
	most    int // the most logins open at once
	refused atomic.Int64
}

func newFullRsyncd(t *testing.T, maxConns int) *fullRsyncd {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	d := &fullRsyncd{addr: l.Addr().String()}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go d.serve(c, maxConns)
		}
	}()
	return d
}

func (d *fullRsyncd) serve(c net.Conn, maxConns int) {
	defer c.Close()
	r := bufio.NewReader(c)
	io.WriteString(c, "@RSYNCD: 31.0\n")
	r.ReadString('\n') // the client's version
	r.ReadString('\n') // the module
	d.mu.Lock()
	if d.open >= maxConns {
		d.mu.Unlock()
		d.refused.Add(1)
		fmt.Fprintf(c, "@ERROR: max connections (%d) reached -- try again later\n", maxConns)
		return
	}
	d.open++
	d.most = max(d.most, d.open)
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.open--
		d.mu.Unlock()
	}()
	io.WriteString(c, "@RSYNCD: OK\n")
	for {
		line, err := r.ReadString('\n')
		if err != nil || line == "\n" {
			break
		}
	}
	io.Copy(io.Discard, r) // until the client closes
}

func (d *fullRsyncd) mostOpen() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.most
}

// errLines counts the ERR and FAIL lines since mark: each opens the log.
func errLines(mark int) int { return logCount(mark, " ERR ") + logCount(mark, " FAIL ") }

// 8 parallel copies to a daemon that takes 2: 6 logins failed each try,
// and gave up after six, each with an ERR line that opened the log. Now
// each waits for a connection of sc's to close, and no more than the
// daemon took are opened at a time.
func TestRsyncMaxConnectionsWaitsForAPlace(t *testing.T) {
	old := closeLag
	closeLag = 20 * time.Millisecond
	defer func() { closeLag = old }()
	d := newFullRsyncd(t, 2)
	b, err := NewRsyncBackend("rsync://" + d.addr + "/m/")
	if err != nil {
		t.Fatal(err)
	}
	client, err := newRsyncClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	mark := len(Log.Lines())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var failed atomic.Int64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, _, err := b.login(ctx, client, "m/")
			if err != nil {
				failed.Add(1)
				t.Errorf("login: %v", err)
				return
			}
			time.Sleep(100 * time.Millisecond) // the copy
			conn.Close()
			b.gate.leave()
		}()
	}
	wg.Wait()
	if n := b.ConnLimit(); n < 1 || n > 2 {
		t.Errorf("ConnLimit %d; want what the daemon takes, 2, or less", n)
	}
	if n := d.mostOpen(); n > 2 {
		t.Errorf("%d logins open at once; the daemon takes 2", n)
	}
	if n := errLines(mark); n != 0 {
		t.Errorf("%d ERR or FAIL lines; want none: nothing failed\n%s", n, strings.Join(Log.Lines()[mark:], "\n"))
	}
	if logCount(mark, " WARN ") == 0 {
		t.Error("no WARN line says the daemon takes no more")
	}
	// Once the limit is known, the logins wait for one to close: not a
	// try for each of them against a full daemon.
	if n := d.refused.Load(); n > 12 {
		t.Errorf("%d logins turned away for 8 copies; want few", n)
	}
}

// A wait for a place is no stall: the copy's stall guard does not end it.
func TestRsyncWaitForAPlaceIsNoStall(t *testing.T) {
	oldTouch := touchEvery
	touchEvery = 50 * time.Millisecond
	defer func() { touchEvery = oldTouch }()
	g := &connGate{limit: 1}
	if _, err := g.enter(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(2500*time.Millisecond, g.leave)
	err := WithStallGuard(context.Background(), new(atomic.Int64), time.Second, func(ctx context.Context) error {
		_, err := g.enter(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("the wait for a place: %v; want it to end when the place is free", err)
	}
}

// A daemon full of other clients' connections, with none of sc's open, is
// tried again for serverFullWait, then the copy stops: no file gets
// through.
func TestRsyncFullServerIsTriedAgain(t *testing.T) {
	old := serverFullWait
	serverFullWait = time.Second
	defer func() { serverFullWait = old }()
	addr, conns := fakeRsyncd(t, "@ERROR: max connections (1) reached -- try again later")
	b, err := NewRsyncBackend("rsync://" + addr + "/m/")
	if err != nil {
		t.Fatal(err)
	}
	mark := len(Log.Lines())
	_, err = b.List(context.Background(), "")
	if !errors.Is(err, errServerFull) || !errors.Is(err, model.ErrTooMany) || !GivesUp(err) || errors.Is(err, model.ErrAuth) {
		t.Errorf("list: %v; want a server that took no connection (errServerFull), which stops a copy", err)
	}
	if n := conns.Load(); n < 2 {
		t.Errorf("%d connects; want it tried again", n)
	}
	if logCount(mark, " WARN ") == 0 {
		t.Error("no WARN line while it waits")
	}
}

// What a turn away brings the limit down to, and + past it.
func TestConnGateTurnedAway(t *testing.T) {
	for _, c := range []struct {
		name        string
		limit, open int // before; open counts the connect turned away
		stuck       bool
		seen, most  int
		want        int
		lowered     bool
	}{
		{"8 open, the daemon says 2", 0, 8, false, 0, 2, 2, true},
		{"one just closed, the daemon says 2", 2, 2, false, 2, 2, 2, false},
		{"stuck, the daemon says 2", 2, 2, true, 2, 2, 1, true},
		{"stuck, another brought it down since", 2, 2, true, 3, 2, 2, false},
		{"no number: wait", 0, 8, false, 0, 0, 0, false},
		{"no number, stuck", 0, 4, true, 0, 0, 3, true},
		{"none of sc's open, stuck", 0, 1, true, 0, 0, 1, true},
		{"never below one", 1, 1, true, 1, 1, 1, false},
		{"the daemon says fewer than known", 7, 3, false, 7, 2, 2, true},
	} {
		g := &connGate{limit: c.limit, open: c.open}
		limit, lowered, _ := g.turnedAway(c.stuck, c.seen, c.most)
		if limit != c.want || lowered != c.lowered {
			t.Errorf("%s: limit %d, lowered %v; want %d, %v", c.name, limit, lowered, c.want, c.lowered)
		}
	}
	g := &connGate{limit: 2}
	g.raise(2)
	if g.limitNow() != 2 {
		t.Fatal("SetParallel at the limit forgot it")
	}
	g.raise(3)
	if g.limitNow() != 0 {
		t.Fatal("+ past the limit kept it")
	}
}

// An extra the server turns away for having too many keeps the extras the
// pool has, with a WARN line, not an ERR: the copies come down to them.
func TestPoolTurnedAwayLowersTheLimit(t *testing.T) {
	var dialed atomic.Int64
	dial := func(context.Context) (int, error) {
		if dialed.Add(1) > 2 {
			return 0, model.WithKind(model.ErrTooMany, errors.New("421 Too many connections"))
		}
		return 1, nil
	}
	p := newConnPool("test", 0, 7, dial, func(int) {})
	p.turnedAway, p.base = tooManyFTP, 1
	mark := len(Log.Lines())
	var releases []func()
	for range 8 {
		_, rel := p.acquire(context.Background())
		releases = append(releases, rel)
	}
	if n := p.connLimit(); n != 3 {
		t.Errorf("connLimit %d; want the 2 extras and the primary", n)
	}
	if n := errLines(mark); n != 0 {
		t.Errorf("%d ERR lines; want none", n)
	}
	if logCount(mark, " WARN ") != 1 {
		t.Errorf("want one WARN line\n%s", strings.Join(Log.Lines()[mark:], "\n"))
	}
	if n := dialed.Load(); n != 3 {
		t.Errorf("%d dials; want no more after the one turned away", n)
	}
	for _, rel := range releases {
		rel()
	}
	p.resize(2) // the 3 copies the UI came down to
	if p.connLimit() != 3 {
		t.Error("a resize to the limit forgot it")
	}
	p.resize(7)
	if p.connLimit() != 0 {
		t.Error("+ kept the limit")
	}
	// An FTP pool the server took no extra for runs its copy on the
	// primary: 1 copy, 0 extras, and a resize to 1 dials none.
	q := newConnPool("test", 0, 2, func(context.Context) (int, error) {
		return 0, model.WithKind(model.ErrTooMany, errors.New("421 Too many connections"))
	}, func(int) {})
	q.turnedAway = tooManyFTP
	_, rel := q.acquire(context.Background())
	rel()
	q.resize(1)
	if n := q.connLimit(); n != 1 {
		t.Errorf("FTP pool with no extra, resized to 1: connLimit %d; want 1", n)
	}
}

// vsftpd, Pure-FTPd and FileZilla Server send 421 past their limits,
// ProFTPD a 530 that is no refused login: both are tried again.
func TestFTPTooMany(t *testing.T) {
	for _, tc := range []struct {
		code int
		msg  string
		want bool
	}{
		{ftp.StatusNotAvailable, "There are too many connected users, please try later.", true},
		{ftp.StatusNotAvailable, "Too many connections (2) from this IP", true},
		{ftp.StatusNotLoggedIn, "Sorry, the maximum number of clients (3) from your host are already connected.", true},
		{ftp.StatusNotLoggedIn, "Login incorrect.", false},
		{550, "maximum number of files", false},
	} {
		err := ftpTooMany(fmt.Errorf("ftp login: %w", &textproto.Error{Code: tc.code, Msg: tc.msg}))
		if got := errors.Is(err, model.ErrTooMany); got != tc.want {
			t.Errorf("%d %s: too many %v; want %v", tc.code, tc.msg, got, tc.want)
		}
		if tc.want && (isPermanentError(err) || GivesUp(err)) {
			t.Errorf("%d %s: final; want it tried again", tc.code, tc.msg)
		}
	}
}

// The same against stock rsyncd with "max connections = 2": 8 uploads at
// once all land, with no ERR line, and the backend says what it takes.
func TestRsyncdMaxConnectionsUploads(t *testing.T) {
	port, root := startRsyncDaemonConf(t, func(root string) string {
		return "[testmod]\n  path = " + filepath.Join(root, "module") + "\n  read only = no\n  max connections = 2\n  lock file = " + filepath.Join(root, "lock") + "\n"
	})
	url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port)
	b := NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
	defer CloseBackend(b)
	src := t.TempDir()
	mark := len(Log.Lines())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	if err := os.Mkdir(filepath.Join(root, "module", "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		name := fmt.Sprintf("d/f%d.txt", i)
		if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("f%d.txt", i)), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.(model.LocalSender).SendLocalFile(ctx, filepath.Join(src, filepath.Base(name)), name, model.PutParams{Mode: 0o644}); err != nil {
				t.Errorf("upload %s: %v", name, err)
			}
		}()
	}
	wg.Wait()
	for i := range 8 {
		name := fmt.Sprintf("d/f%d.txt", i)
		if got, err := os.ReadFile(filepath.Join(root, "module", name)); err != nil || string(got) != name {
			t.Errorf("%s on the daemon: %q, %v", name, got, err)
		}
	}
	if n := ConnLimit(b); n < 1 || n > 2 {
		t.Errorf("ConnLimit %d; want 2 or less", n)
	}
	// The daemon says how many it takes: 2 at once, not one step down at
	// a time from 8.
	if n := logCount(mark, " WARN ", "2 connection(s) at a time"); n != 1 {
		t.Errorf("%d WARN lines for 2 at a time; want one\n%s", n, strings.Join(Log.Lines()[mark:], "\n"))
	}
	if n := errLines(mark); n != 0 {
		t.Errorf("%d ERR or FAIL lines; want none\n%s", n, strings.Join(Log.Lines()[mark:], "\n"))
	}
}
