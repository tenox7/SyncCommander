package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// resetProxy passes connections on to target, and resets the next ones
// while resets is above zero, as stock rsyncd resets one when a path is
// not there. conns counts the connections it took.
type resetProxy struct {
	l      net.Listener
	target string
	resets atomic.Int32
	conns  atomic.Int64
}

func startResetProxy(t *testing.T, target string) *resetProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &resetProxy{l: l, target: target}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			p.conns.Add(1)
			if p.resets.Add(-1) >= 0 {
				c.(*net.TCPConn).SetLinger(0)
				c.Close()
				continue
			}
			p.resets.Store(0)
			go p.pass(c)
		}
	}()
	return p
}

func (p *resetProxy) pass(c net.Conn) {
	defer c.Close()
	d, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer d.Close()
	go func() {
		io.Copy(d, c)
		d.(*net.TCPConn).CloseWrite()
	}()
	io.Copy(c, d)
}

// R8: a call on a daemon that fails with its connection made the wrapper
// drop the backend, and the next call made a new one: the listing the
// preload had cached was gone, the next listing of a folder went to the
// daemon again (one per folder at 1M files), and the preload that was
// still running filled a cache no call read. Each call dials the daemon
// by itself: the call fails, the backend stays.
func TestRsyncFailedCallKeepsTheBackend(t *testing.T) {
	port, dir := startRsyncDaemon(t)
	if err := os.MkdirAll(filepath.Join(dir, "d", "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"d/a.txt", "d/sub/b.txt"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("abc"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	p := startResetProxy(t, fmt.Sprintf("127.0.0.1:%d", port))
	url := "rsync://" + p.l.Addr().String() + "/testmod/"
	var made atomic.Int32
	b := NewLazyBackend(url, func(context.Context) (model.Backend, error) {
		made.Add(1)
		return NewRsyncBackend(url)
	})
	defer CloseBackend(b)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := b.(model.RecursivePreloader).PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"", "d", "d/sub"} {
		if _, err := b.List(ctx, d); err != nil {
			t.Fatalf("list %q: %v", d, err)
		}
	}
	before := p.conns.Load()
	p.resets.Store(1)
	if _, err := b.Stat(ctx, "d/a.txt"); err != nil {
		t.Fatalf("stat d/a.txt, its first connection reset: %v; want it found on the next", err)
	}
	if got := p.conns.Load() - before; got != 2 {
		t.Fatalf("stat took %d connections; want 2, the reset one and the next", got)
	}
	if n := made.Load(); n != 1 {
		t.Errorf("the backend was made %d times; want once", n)
	}
	before = p.conns.Load()
	for _, d := range []string{"d", "d/sub"} {
		if _, err := b.List(ctx, d); err != nil {
			t.Fatalf("list %q: %v", d, err)
		}
	}
	if got := p.conns.Load() - before; got != 0 {
		t.Errorf("the folders the preload listed were listed again, %d connections; want none", got)
	}
}
