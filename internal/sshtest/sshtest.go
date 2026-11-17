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

// Options set the server up as a real one can be.
type Options struct {
	// SFTP gives it the sftp subsystem.
	SFTP bool
	// Banner gives the text printed ahead of each command's output, as by
	// a login script that prints. A test can have it start later, as a
	// script that prints a warning only now and then.
	Banner func() string
	// OnlySFTP runs the sftp server for every command, as ForceCommand
	// internal-sftp does: with nothing on its input, a command prints
	// nothing and exits 0. The sftp subsystem works.
	OnlySFTP bool
}

// Start serves until the test ends and returns the address. Without
// withSFTP it has no sftp subsystem, so ssh:// and scp:// use the shell
// backend, as on a server whose sshd has none.
func Start(t testing.TB, withSFTP bool) string {
	t.Helper()
	return StartWith(t, Options{SFTP: withSFTP})
}

// StartWith is Start with the server set up as o says.
func StartWith(t testing.TB, o Options) string {
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
						go serveSession(c, creqs, o)
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

func serveSession(c ssh.Channel, reqs <-chan *ssh.Request, o Options) {
	defer c.Close()
	for req := range reqs {
		var sub struct{ Name string }
		if (o.SFTP || o.OnlySFTP) && req.Type == "subsystem" && ssh.Unmarshal(req.Payload, &sub) == nil && sub.Name == "sftp" {
			req.Reply(true, nil)
			serveSFTP(c)
			return
		}
		var p struct{ Command string }
		if req.Type != "exec" || ssh.Unmarshal(req.Payload, &p) != nil {
			req.Reply(false, nil)
			continue
		}
		req.Reply(true, nil)
		if o.OnlySFTP {
			serveSFTP(c)
			c.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
			return
		}
		if o.Banner != nil {
			io.WriteString(c, o.Banner())
		}
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

func serveSFTP(c ssh.Channel) {
	if srv, err := sftp.NewServer(c); err == nil {
		srv.Serve()
	}
}
