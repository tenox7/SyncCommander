package transport

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// startSilentServer accepts TCP connections and never answers, as an
// ssh -L forward whose far end is dead does.
func startSilentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

// S12: the ssh handshake had no deadline and no ctx. Through an ssh -L
// forward whose far end is dead the TCP connect works and the handshake
// waits for good, holding the pane's dial lock: every operation on that
// side queued behind it. A connect now ends with the ctx of the call that
// dials.
func TestSSHConnectToSilentServerEnds(t *testing.T) {
	addr := startSilentServer(t)
	for _, scheme := range []string{"sftp", "ssh", "rsync+ssh"} {
		b, err := TryOpenBackend(scheme+"://x:pw@"+addr+"/data", true, 1)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		done := make(chan error, 1)
		go func() {
			_, err := b.List(ctx, "")
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("%s: List through a server that never answers: no error", scheme)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s: List through a server that never answers still waits 10 s after its ctx ended", scheme)
		}
		cancel()
		CloseBackend(b)
	}
}

// The handshake also ends on its own, for a dial whose ctx has no end (a
// pool's extra, a connect for a scan), and its deadline does not outlive it.
func TestSSHHandshakeTimesOut(t *testing.T) {
	defer func(d time.Duration) { sshHandshakeTimeout = d }(sshHandshakeTimeout)
	sshHandshakeTimeout = 300 * time.Millisecond

	addr := startSilentServer(t)
	done := make(chan error, 1)
	go func() {
		_, err := dialSSH(context.Background(), "ssh://x:pw@"+addr+"/", true)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("handshake with a server that never answers: no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handshake with a server that never answers still waits after 10 s")
	}

	conn, err := dialSSH(context.Background(), "ssh://x:pw@"+startExecSSHServer(t)+"/", true)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.client.Close()
	time.Sleep(3 * sshHandshakeTimeout)
	s := &sshShell{client: conn.client, proto: "ssh"}
	if out, err := s.run(context.Background(), "echo ok"); err != nil || out != "ok\n" {
		t.Errorf("a command after the handshake deadline passed: %q, %v", out, err)
	}
}
