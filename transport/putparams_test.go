package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// D1: a put is told the file's size and mtime in model.PutParams. They
// travelled in the context, where a caller that left them out compiled and
// got a file stamped with the time of its upload, sent without its length.
// Each case puts with a context that holds nothing.

var putMtime = time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)

// hidden hides a reader's type, so net/http cannot learn its length by
// itself, as with every reader the copy engine passes.
type hidden struct{ io.Reader }

// checkPutStamped puts a 12-byte file and an empty one under up/ on b, and
// checks that root, where b keeps its files, holds them stamped with the
// mtime the put was told.
func checkPutStamped(t *testing.T, b model.Backend, root string) {
	t.Helper()
	for name, body := range map[string]string{"f.txt": "twelve bytes", "empty.txt": ""} {
		p := model.PutParams{Size: int64(len(body)), Mode: 0o644, ModTime: putMtime}
		if err := b.CopyFrom(context.Background(), "up/"+name, hidden{strings.NewReader(body)}, p); err != nil {
			t.Fatalf("CopyFrom %s: %v", name, err)
		}
		fi, err := os.Stat(filepath.Join(root, "up", name))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != int64(len(body)) || !fi.ModTime().Equal(putMtime) {
			t.Errorf("%s: the server holds %d bytes stamped %v; want %d stamped %v, the mtime the put was told",
				name, fi.Size(), fi.ModTime().UTC(), len(body), putMtime)
		}
	}
}

// rsync -t sends the mtime of the file it stages, rclone stores it with
// the object, and WebDAV sends it as X-OC-Mtime.
func TestPutStoresTheMtimeItIsTold(t *testing.T) {
	t.Run("rsyncd", func(t *testing.T) {
		port, mod := startRsyncDaemon(t)
		b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
		if err != nil {
			t.Fatal(err)
		}
		checkPutStamped(t, b, mod)
	})
	t.Run("gorsyncd", func(t *testing.T) {
		u, mod := startGorsyncd(t)
		b, err := NewRsyncBackend(u)
		if err != nil {
			t.Fatal(err)
		}
		checkPutStamped(t, b, mod)
	})
	t.Run("rsync+ssh", func(t *testing.T) {
		mod := t.TempDir()
		b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+startExecSSHServer(t)+mod, true, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		checkPutStamped(t, b, mod)
	})
	t.Run("rclone", func(t *testing.T) {
		root := t.TempDir()
		b, err := NewRcloneBackend(context.Background(), "rclone://"+root, false)
		if err != nil {
			t.Fatal(err)
		}
		checkPutStamped(t, b, root)
	})
	t.Run("webdav", func(t *testing.T) {
		root := t.TempDir()
		port, _ := startRcloneWebDAV(t, root)
		b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), false, 4)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		checkPutStamped(t, b, root)
	})
}

// lengths forwards to a server and keeps the Content-Length of each PUT and
// POST by path: -1 for a body sent without one (chunked).
type lengths struct {
	mu  sync.Mutex
	got map[string]int64
}

func (l *lengths) of(path string) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, ok := l.got[path]
	return n, ok
}

// startLengths puts lengths in front of the server at port and returns the
// host:port to reach it by.
func startLengths(t *testing.T, port int) (*lengths, string) {
	t.Helper()
	target, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	l := &lengths{got: map[string]int64{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			l.mu.Lock()
			l.got[r.URL.Path] = r.ContentLength
			l.mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return l, strings.TrimPrefix(srv.URL, "http://")
}

// A WebDAV PUT and a restic POST carry the size they are told as their
// Content-Length: some servers refuse a chunked upload, and restic's want
// the length.
func TestPutSendsTheSizeItIsTold(t *testing.T) {
	body := "twelve bytes"
	p := model.PutParams{Size: int64(len(body)), Mode: 0o644, ModTime: putMtime}
	t.Run("webdav", func(t *testing.T) {
		port, _ := startRcloneWebDAV(t, t.TempDir())
		l, addr := startLengths(t, port)
		b, err := NewWebDAVBackend(context.Background(), "webdav://u:p@"+addr+"/", false, 4)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		if err := b.CopyFrom(context.Background(), "up/f.txt", hidden{strings.NewReader(body)}, p); err != nil {
			t.Fatal(err)
		}
		if n, ok := l.of("/up/f.txt"); !ok || n != int64(len(body)) {
			t.Errorf("the PUT carried Content-Length %d (seen %v); want %d, the size it was told", n, ok, len(body))
		}
	})
	t.Run("restic", func(t *testing.T) {
		port, _ := startRcloneRestic(t, t.TempDir())
		l, addr := startLengths(t, port)
		var b *ResticBackend
		var err error
		for range 20 {
			if b, err = NewResticBackend(context.Background(), "restic://u:p@"+addr+"/", false, 8); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("NewResticBackend: %v", err)
		}
		hash := sha256hex([]byte(body))
		if err := b.CopyFrom(context.Background(), "data/"+hash, hidden{strings.NewReader(body)}, p); err != nil {
			t.Fatal(err)
		}
		if n, ok := l.of("/data/" + hash); !ok || n != int64(len(body)) {
			t.Errorf("the POST carried Content-Length %d (seen %v); want %d, the size it was told", n, ok, len(body))
		}
	})
}
