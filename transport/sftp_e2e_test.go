package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// startSFTPServer runs an in-process sshd offering only the sftp subsystem:
// exec and shell are refused, so the find preload fails and List falls back
// to READDIR.
func startSFTPServer(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{NoClientAuth: true}
	cfg.AddHostKey(signer)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go serveSSHConn(nc, cfg)
		}
	}()
	return l.Addr().String()
}

func serveSSHConn(nc net.Conn, cfg *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(nc, cfg)
	if err != nil {
		return
	}
	defer conn.Close()
	go ssh.DiscardRequests(reqs)
	for ch := range chans {
		if ch.ChannelType() != "session" {
			ch.Reject(ssh.UnknownChannelType, "")
			continue
		}
		c, creqs, err := ch.Accept()
		if err != nil {
			continue
		}
		go serveSFTPSession(c, creqs)
	}
}

func serveSFTPSession(c ssh.Channel, reqs <-chan *ssh.Request) {
	defer c.Close()
	for req := range reqs {
		var sub struct{ Name string }
		ok := req.Type == "subsystem" && ssh.Unmarshal(req.Payload, &sub) == nil && sub.Name == "sftp"
		req.Reply(ok, nil)
		if !ok {
			continue
		}
		srv, err := sftp.NewServer(c)
		if err != nil {
			return
		}
		srv.Serve()
		return
	}
}

func TestSFTPBackendE2E(t *testing.T) {
	addr := startSFTPServer(t)
	root := t.TempDir()
	b, err := NewSFTPBackend("sftp://x:pw@"+addr+root, true, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()

	data := bytes.Repeat([]byte("sftp 255K packets\n"), 60000)
	if err := b.CopyFrom(ctx, "dir/a.bin", bytes.NewReader(data), 0644); err != nil {
		t.Fatal(err)
	}
	rc, err := b.Open(ctx, "dir/a.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("round trip: err=%v len=%d want %d", err, len(got), len(data))
	}

	if err := os.WriteFile(filepath.Join(root, "dir", "b.bin"), []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := b.Rename(ctx, "dir/a.bin", "dir/b.bin"); err != nil {
		t.Fatal("rename onto existing:", err)
	}
	mtime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := b.SetTimes(ctx, "dir/b.bin", mtime, mtime, time.Time{}); err != nil {
		t.Fatal(err)
	}
	b.PreloadRecursive(ctx, "")
	entries, err := b.List(ctx, "dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "b.bin" || entries[0].Size != int64(len(data)) || !entries[0].ModTime.Equal(mtime) {
		t.Fatalf("list after rename: %+v", entries)
	}
	if err := b.Remove(ctx, "dir/b.bin"); err != nil {
		t.Fatal(err)
	}
	if entries, _ = b.List(ctx, "dir"); len(entries) != 0 {
		t.Fatalf("remove left %+v", entries)
	}

	sb, err := OpenBackend("ssh://x:pw@"+addr+root, true, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(sb)
	if _, ok := sb.(*SFTPBackend); !ok {
		t.Fatalf("ssh:// opened %T, want *SFTPBackend", sb)
	}
}
