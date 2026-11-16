package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
)

// silentServer accepts connections and never says a word.
func silentServer(t *testing.T) string {
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

// D1: the first connect of an ftp://, webdav://, restic:// or rclone://
// location did not take the context. A cancel, as when the user leaves a
// pane that is still connecting, did not stop it: it went on against a
// server that never answers until its own timeout, 15 s for WebDAV and
// restic, minutes for an rclone remote. sftp:// took it already.
func TestOpenStopsWhenCancelled(t *testing.T) {
	for name, url := range map[string]func(addr string) string{
		"ftp":    func(a string) string { return "ftp://u:p@" + a + "/" },
		"webdav": func(a string) string { return "webdav://u:p@" + a + "/" },
		"restic": func(a string) string { return "restic://u:p@" + a + "/" },
		"rclone": func(a string) string { return "rclone://:http,url='http://" + a + "/':dir" },
		"sftp":   func(a string) string { return "sftp://u:p@" + a + "/" },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				b, err := openBackend(ctx, url(silentServer(t)), true, 1)
				if err == nil {
					CloseBackend(b)
				}
				done <- err
			}()
			time.Sleep(300 * time.Millisecond)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("the open returned %v; want it to say it was cancelled", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the open went on 3 s after it was cancelled")
			}
		})
	}
}

// The reconnecting wrapper connects under the context of the call that
// needed the connection, which ends with that call. What the connect made
// must outlive it: the FTP control socket and its later data sockets, a
// remote that keeps its construction context (rclone's s3 and smb). FTP
// reconnects when a connection turns out dead, which would hide a socket
// the end of the context closed: no new login may happen either.
func TestOpenOutlivesItsContext(t *testing.T) {
	ftpd := &fakeFTPD{ls: map[string][]string{"/": {lsLine("a.txt", false)}}}
	for name, url := range map[string]func(t *testing.T) string{
		"ftp":    func(t *testing.T) string { return "ftp://u:p@" + ftpd.start(t) + "/" },
		"rclone": func(t *testing.T) string { registerKeptCtxRemote(); return "rclone://:scctxtest:" + t.TempDir() },
		"sftp":   func(t *testing.T) string { return "sftp://x:pw@" + startExecSFTPServer(t) + t.TempDir() },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			b, err := openBackend(ctx, url(t), true, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer CloseBackend(b)
			logins := ftpd.loginCount()
			cancel()
			time.Sleep(50 * time.Millisecond)
			for i := range 2 {
				if _, err := b.List(context.Background(), ""); err != nil {
					t.Fatalf("list %d after the connect's context ended: %v", i+1, err)
				}
			}
			if n := ftpd.loginCount() - logins; n != 0 {
				t.Errorf("%d new logins after the connect's context ended; the connection should have lived on", n)
			}
		})
	}
}

var keptCtxOnce sync.Once

// keptCtxFs is an rclone remote that keeps the context it was made with, as
// s3 does for the connections it makes later, and fails to list once that
// context has ended.
type keptCtxFs struct {
	fs.Fs
	made context.Context
}

func (f *keptCtxFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if err := f.made.Err(); err != nil {
		return nil, fmt.Errorf("the context the remote was made with: %w", err)
	}
	return f.Fs.List(ctx, dir)
}

// registerKeptCtxRemote adds keptCtxFs to rclone as :scctxtest:PATH, a
// local folder.
func registerKeptCtxRemote() {
	keptCtxOnce.Do(func() {
		rcloneSetup()
		fs.Register(&fs.RegInfo{
			Name: "scctxtest",
			NewFs: func(ctx context.Context, _, root string, _ configmap.Mapper) (fs.Fs, error) {
				f, err := fs.NewFs(ctx, root)
				if err != nil {
					return nil, err
				}
				return &keptCtxFs{Fs: f, made: ctx}, nil
			},
		})
	})
}
