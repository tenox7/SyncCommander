package transport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"

	"sc/model"
)

// D1: Close was not in model.Backend. It was found by type assertion, so a
// wrapper that embeds the interface, the usual way to decorate one, hid the
// Close of the backend it wraps: closing the wrapper closed nothing.
func TestCloseReachesThroughAWrapper(t *testing.T) {
	inner := &connStub{}
	CloseBackend(struct{ model.Backend }{inner})
	if !inner.closed.Load() {
		t.Error("closing a wrapper that embeds model.Backend left the backend it wraps open")
	}

	connected := &connStub{}
	lazy := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) { return connected, nil })
	if _, err := lazy.List(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	CloseBackend(lazy)
	if !connected.closed.Load() {
		t.Error("closing the reconnecting wrapper left its connection open")
	}
}

// shutFs is an rclone remote that counts its Shutdown calls.
type shutFs struct {
	fs.Fs
	shutdowns atomic.Int64
}

func (f *shutFs) Shutdown(context.Context) error {
	f.shutdowns.Add(1)
	return nil
}

// rclone keeps work for a remote until its Shutdown: dropbox commits the
// uploads it batched, smb closes its sessions. rclone's cache of remotes
// calls it when it lets one go; sc makes its remotes without that cache,
// and had no Close for them, so nothing called it.
func TestRcloneCloseShutsTheRemoteDown(t *testing.T) {
	rcloneSetup()
	inner, err := fs.NewFs(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &shutFs{Fs: inner}
	b := &RcloneBackend{url: "rclone://test", f: f, feat: f.Features(), objs: newRcObjCache(rcloneObjCacheLimit), listCache: newListCache()}
	CloseBackend(b)
	if n := f.shutdowns.Load(); n != 1 {
		t.Fatalf("Close shut the remote down %d times, want once", n)
	}
	CloseBackend(b)
	if n := f.shutdowns.Load(); n != 1 {
		t.Errorf("a second Close shut the remote down again (%d times)", n)
	}
}

// A second Close does no harm: a pane closes the backends it replaces, and
// the program closes the ones it started with on exit.
func TestCloseTwiceDoesNoHarm(t *testing.T) {
	opened := func(t *testing.T, url string, insecure bool) model.Backend {
		t.Helper()
		b, err := OpenBackend(url, insecure, 1)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for name, open := range map[string]func(t *testing.T) model.Backend{
		"local": func(t *testing.T) model.Backend { return NewLocalBackend(t.TempDir()) },
		"fake":  func(t *testing.T) model.Backend { return opened(t, "fake://small", false) },
		"rsync": func(t *testing.T) model.Backend { u, _ := startGorsyncd(t); return opened(t, u, false) },
		"sftp": func(t *testing.T) model.Backend {
			return opened(t, "sftp://x:pw@"+startExecSFTPServer(t)+t.TempDir(), true)
		},
		"scp": func(t *testing.T) model.Backend {
			return opened(t, "scp://x:pw@"+startExecSSHServer(t)+t.TempDir(), true)
		},
		"rsync+ssh": func(t *testing.T) model.Backend {
			return opened(t, "rsync+ssh://x:pw@"+startExecSSHServer(t)+t.TempDir(), true)
		},
		"ftp": func(t *testing.T) model.Backend {
			return openFakeFTP(t, &fakeFTPD{ls: map[string][]string{"/": {lsLine("a.txt", false)}}}, "/")
		},
		"webdav": func(t *testing.T) model.Backend {
			port, _ := startRcloneWebDAV(t, t.TempDir())
			return opened(t, fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), false)
		},
		"rclone": func(t *testing.T) model.Backend { return opened(t, "rclone://"+t.TempDir(), false) },
	} {
		t.Run(name, func(t *testing.T) {
			b := open(t)
			if _, err := b.List(context.Background(), ""); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				b.Close()
				b.Close()
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("a second Close does not return")
			}
		})
	}
}
