package transport

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

// S23: an extra whose connection ended went back to the pool and was lent
// out again, and the call on it failed. One that ends while idle is
// dropped when it is next taken, one that ends while lent out at its
// release, and the pool dials another.
func TestConnPoolDropsAnExtraWhoseConnectionEnded(t *testing.T) {
	type conn struct {
		id   int64
		dead atomic.Bool
	}
	var dialed, closed atomic.Int64
	dial := func(context.Context) (*conn, error) { return &conn{id: dialed.Add(1)}, nil }
	p := newConnPool[*conn]("test", nil, 1, dial, func(*conn) { closed.Add(1) })
	p.alive = func(c *conn) bool { return !c.dead.Load() }
	ctx := context.Background()

	c1, r1 := p.acquire(ctx)
	r1()
	c1.dead.Store(true) // it ends while idle
	mark := len(Log.Lines())
	c2, r2 := p.acquire(ctx)
	if c2 == nil || c2 == c1 {
		t.Fatalf("got extra %v, want a new one in place of the one that ended", c2)
	}
	if closed.Load() != 1 || dialed.Load() != 2 {
		t.Errorf("closed %d, dialed %d: want the dead one closed and one more dialed", closed.Load(), dialed.Load())
	}
	if !loggedSince(mark, "an extra connection is gone: dropped") {
		t.Error("no line says the extra was dropped")
	}
	c2.dead.Store(true) // it ends while lent out
	r2()
	if closed.Load() != 2 || len(p.idle) != 0 {
		t.Errorf("closed %d, %d idle: want the dead one closed at its release", closed.Load(), len(p.idle))
	}
	if c3, _ := p.acquire(ctx); c3 == nil || c3 == c2 || dialed.Load() != 3 {
		t.Errorf("got extra %v after %d dials, want a third one", c3, dialed.Load())
	}
}

// endConn ends c's connection, as a server that drops an idle one does,
// and waits until sc has seen it end.
func endConn(t *testing.T, c *ssh.Client) {
	t.Helper()
	c.Close()
	for deadline := time.Now().Add(5 * time.Second); sshClientAlive(c); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the connection is still alive after its close")
		}
	}
}

// S23: the backends' extras: one whose connection ended while it sat idle
// was lent out again, and the next transfer on it failed.
func TestSSHBackendsDropAnExtraWhoseConnectionEnded(t *testing.T) {
	ctx := context.Background()
	put := func(b model.Backend, name string) error {
		return b.CopyFrom(ctx, name, strings.NewReader("hello"), model.PutParams{Size: 5, Mode: 0o644})
	}
	t.Run("sftp", func(t *testing.T) {
		b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+startSFTPRequestServer(t, sftp.InMemHandler())+"/", true, 2)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close() })
		if err := put(b, "a.txt"); err != nil {
			t.Fatal(err)
		}
		if len(b.pool.idle) != 1 {
			t.Fatalf("%d idle extras after a copy, want 1", len(b.pool.idle))
		}
		endConn(t, b.pool.idle[0].ssh)
		if err := put(b, "b.txt"); err != nil {
			t.Errorf("a copy after an idle extra's connection ended: %v", err)
		}
		// The session ends and the connection stays, as OpenSSH's
		// ChannelTimeout leaves an sftp session that sat idle.
		if len(b.pool.idle) != 1 {
			t.Fatalf("%d idle extras after a copy, want 1", len(b.pool.idle))
		}
		c := b.pool.idle[0]
		c.sftp.Close()
		for deadline := time.Now().Add(5 * time.Second); !c.ended.Load(); time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the sftp session has not ended after its close")
			}
		}
		if !sshClientAlive(c.ssh) {
			t.Fatal("the connection ended with the session")
		}
		if err := put(b, "c.txt"); err != nil {
			t.Errorf("a copy after an idle extra's sftp session ended: %v", err)
		}
	})
	t.Run("scp", func(t *testing.T) {
		root := t.TempDir()
		ob, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 2)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ob.Close() })
		b := ob.(*SCPBackend)
		if err := put(b, "a.txt"); err != nil {
			t.Fatal(err)
		}
		if len(b.pool.idle) != 1 {
			t.Fatalf("%d idle extras after a copy, want 1", len(b.pool.idle))
		}
		endConn(t, b.pool.idle[0])
		if err := put(b, "b.txt"); err != nil {
			t.Errorf("a copy after an idle extra's connection ended: %v", err)
		}
	})
	t.Run("rsync+ssh", func(t *testing.T) {
		if _, err := exec.LookPath("rsync"); err != nil {
			t.Skip("no rsync binary")
		}
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello"), 0o644); err != nil {
			t.Fatal(err)
		}
		b, err := NewRsyncSSHBackend(ctx, "rsync+ssh://x:pw@"+startExecSSHServer(t)+root, true, 2)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close() })
		read := func() (string, error) {
			r, err := b.OpenAt(ctx, "a.txt", 1)
			if err != nil {
				return "", err
			}
			data, err := io.ReadAll(r)
			if cerr := r.Close(); err == nil {
				err = cerr
			}
			return string(data), err
		}
		if got, err := read(); err != nil || got != "ello" {
			t.Fatalf("read %q, %v", got, err)
		}
		if len(b.pool.idle) != 1 {
			t.Fatalf("%d idle extras after a read, want 1", len(b.pool.idle))
		}
		endConn(t, b.pool.idle[0])
		if got, err := read(); err != nil || got != "ello" {
			t.Errorf("a read after an idle extra's connection ended: %q, %v", got, err)
		}
	})
}
