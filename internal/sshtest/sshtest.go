// Package sshtest runs an in-process ssh server for tests: every exec
// request runs with sh -c on this machine, so ssh backends drive the real
// find, perl, rsync and shell tools as on a remote host, and the sftp
// subsystem serves this machine's files.
package sshtest

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"os/exec"
	"testing"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Start serves until the test ends and returns the address. Without
// withSFTP it has no sftp subsystem, so sftp:// falls back to the shell
// backend, as on a server whose sshd has none.
func Start(t testing.TB, withSFTP bool) string {
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
			go func() {
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
					if err == nil {
						go serveSession(c, creqs, withSFTP)
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

func serveSession(c ssh.Channel, reqs <-chan *ssh.Request, withSFTP bool) {
	defer c.Close()
	for req := range reqs {
		var sub struct{ Name string }
		if withSFTP && req.Type == "subsystem" && ssh.Unmarshal(req.Payload, &sub) == nil && sub.Name == "sftp" {
			req.Reply(true, nil)
			if srv, err := sftp.NewServer(c); err == nil {
				srv.Serve()
			}
			return
		}
		var p struct{ Command string }
		if req.Type != "exec" || ssh.Unmarshal(req.Payload, &p) != nil {
			req.Reply(false, nil)
			continue
		}
		req.Reply(true, nil)
		cmd := exec.Command("sh", "-c", p.Command)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return
		}
		go func() { io.Copy(stdin, c); stdin.Close() }()
		cmd.Stdout, cmd.Stderr = c, c.Stderr()
		status := 0
		if err := cmd.Run(); err != nil {
			status = 1
			if ee, ok := err.(*exec.ExitError); ok {
				status = ee.ExitCode()
			}
		}
		c.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{uint32(status)}))
		return
	}
}
