package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// pausingProxy forwards to the WebDAV server on port. It holds back the
// answer to a Depth: infinity PROPFIND after the response for href until
// release; paused is closed once it holds it back.
func pausingProxy(t *testing.T, port int, href string) (proxyPort int, paused <-chan struct{}, release func()) {
	t.Helper()
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	held, free := make(chan struct{}), make(chan struct{})
	var heldOnce, freeOnce sync.Once
	release = func() { freeOnce.Do(func() { close(free) }) }
	rp := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(target) }}
	rp.ModifyResponse = func(resp *http.Response) error {
		if resp.Request.Method != "PROPFIND" || !strings.EqualFold(resp.Request.Header.Get("Depth"), "infinity") {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		cut := -1
		if i := bytes.Index(body, []byte("<D:href>"+href+"</D:href>")); i >= 0 {
			if j := bytes.Index(body[i:], []byte("</D:response>")); j >= 0 {
				cut = i + j + len("</D:response>")
			}
		}
		if cut < 0 {
			t.Errorf("the deep answer has no response for %s:\n%s", href, body)
			cut = len(body)
		}
		pr, pw := io.Pipe()
		go func() {
			pw.Write(body[:cut])
			heldOnce.Do(func() { close(held) })
			<-free
			pw.Write(body[cut:])
			pw.Close()
		}()
		resp.Body, resp.ContentLength = pr, -1
		resp.Header.Del("Content-Length")
		return nil
	}
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	t.Cleanup(release) // runs first: Close waits for the held answer
	return srv.Listener.Addr().(*net.TCPAddr).Port, held, release
}

// W-H2, with real rclone serve webdav: a copy into a/ while the scan's deep
// listing is still coming. The listing had sent a/ as a child of the base;
// the write dropped the base's listing, and what came after it (b/ and c/)
// was kept as the whole base. After the run List("") gave [b c]: the tree
// showed a/ on one side only, and a mirror the other way deleted it.
func TestWebDAVWriteDuringPreloadIsListedLive(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"a/a1.txt", "b/b1.txt", "c/c1.txt"} {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	port, _ := startRcloneWebDAV(t, root)
	proxy, paused, release := pausingProxy(t, port, "/b/")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := NewWebDAVBackend(ctx, fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", proxy), false, 4)
	if err != nil {
		t.Fatal(err)
	}
	b.PreloadRecursive(ctx, "")
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("the deep listing never came")
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		if _, hit, _, _ := b.listCache.lookup("a"); hit {
			break // a/ came in full, and with it a/ as a child of the base
		}
		if time.Now().After(deadline) {
			t.Fatal("a/ never came in full")
		}
	}
	if err := b.CopyFrom(ctx, "a/new.txt", strings.NewReader("new"), model.PutParams{Size: 3, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	release()
	waitDone(t, b.listCache)
	for dir, want := range map[string][]string{"": {"a", "b", "c"}, "a": {"a1.txt", "new.txt"}, "c": {"c1.txt"}} {
		entries, err := b.List(ctx, dir)
		if err != nil {
			t.Errorf("List(%q) after the run: %v", dir, err)
			continue
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name)
		}
		slices.Sort(names)
		if !slices.Equal(names, want) {
			t.Errorf("List(%q) after the run = %q, want %q", dir, names, want)
		}
	}
}
