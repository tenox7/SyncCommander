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
	"strconv"
	"strings"
	"sync"
	"testing"
)

// depthProxy forwards to the WebDAV server on port and records the path of
// every Depth: 1 PROPFIND. With oneLevel it also answers Depth: infinity
// with one level, as sabre/dav does by default.
func depthProxy(t *testing.T, port int, oneLevel bool) (proxyPort int, depth1 func() []string) {
	t.Helper()
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		switch depth := r.In.Header.Get("Depth"); {
		case r.In.Method == "PROPFIND" && depth == "1":
			mu.Lock()
			paths = append(paths, r.In.URL.Path)
			mu.Unlock()
		case oneLevel && strings.EqualFold(depth, "infinity"):
			r.Out.Header.Set("Depth", "1")
		}
	}})
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().(*net.TCPAddr).Port, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(paths)
	}
}

// W-C1: an answer to Depth: infinity does not say how deep the server went.
// A folder with no children in it is listed again on its own, never taken
// as empty. Folders that came with children are served from the preload.
func TestWebDAVPreloadListsChildlessFoldersAgain(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"docs/plan.txt", "docs/report.txt", "deep/x/y.txt", "top.txt"} {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	port, _ := startRcloneWebDAV(t, root)
	want := map[string][]string{"": {"deep", "docs", "empty", "top.txt"}, "docs": {"plan.txt", "report.txt"}, "deep": {"x"}, "deep/x": {"y.txt"}, "empty": nil}
	for name, tc := range map[string]struct {
		oneLevel bool
		live     []string
	}{
		"server sends the whole tree": {false, []string{"/empty/"}},
		"server sends one level":      {true, []string{"/deep/", "/deep/x/", "/docs/", "/empty/"}},
	} {
		t.Run(name, func(t *testing.T) {
			pp, depth1 := depthProxy(t, port, tc.oneLevel)
			b, err := NewWebDAVBackend(fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", pp), false, 4)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			b.PreloadRecursive(ctx, "")
			for _, dir := range []string{"", "docs", "deep", "deep/x", "empty"} {
				entries, err := b.List(ctx, dir)
				var names []string
				for _, e := range entries {
					names = append(names, e.Name)
				}
				slices.Sort(names)
				if err != nil || !slices.Equal(names, want[dir]) {
					t.Errorf("List(%q) = %q, %v; want %q", dir, names, err, want[dir])
				}
			}
			live := depth1()
			slices.Sort(live)
			if !slices.Equal(live, tc.live) {
				t.Errorf("listed live: %q, want %q", live, tc.live)
			}
		})
	}
}

// walkAbortProxy forwards to the WebDAV server on port. The answer to a
// PROPFIND with the given Depth is cut after keep responses and ended the
// way golang.org/x/net/webdav ends one whose walk failed part way: it
// closes the multistatus, then writes the status text after it.
func walkAbortProxy(t *testing.T, port int, depth string, keep int) int {
	t.Helper()
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ModifyResponse = func(resp *http.Response) error {
		if resp.Request.Method != "PROPFIND" || !strings.EqualFold(resp.Request.Header.Get("Depth"), depth) {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		const end = "</D:response>"
		cut := 0
		for range keep {
			i := bytes.Index(body[cut:], []byte(end))
			if i < 0 {
				t.Errorf("the answer has fewer than %d responses", keep)
				break
			}
			cut += i + len(end)
		}
		body = append(body[:cut:cut], "</D:multistatus>Internal Server Error"...)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength, resp.TransferEncoding = int64(len(body)), nil
		resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().(*net.TCPAddr).Port
}

// W-H1, with real rclone serve webdav: a deep answer cut short the way
// x/net/webdav cuts it is refused, so every folder is listed on its own and
// nothing is missing. A one-level answer cut the same way fails that
// folder's listing.
func TestWebDAVWalkAbortIsNotAListing(t *testing.T) {
	root := t.TempDir()
	var big []string
	for i := range 10 {
		big = append(big, fmt.Sprintf("f%05d", i))
	}
	files := []string{"top/zz-after/last.txt", "top/zz-file.txt"}
	for _, f := range big {
		files = append(files, "top/big/"+f)
	}
	for _, p := range files {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	port, _ := startRcloneWebDAV(t, root)
	ctx := context.Background()

	t.Run("deep listing", func(t *testing.T) {
		// "/", "/top/", "/top/big/" and f00000 to f00004: the walk failed in big/.
		b, err := NewWebDAVBackend(fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", walkAbortProxy(t, port, "infinity", 8)), false, 4)
		if err != nil {
			t.Fatal(err)
		}
		mark := len(Log.Lines())
		b.PreloadRecursive(ctx, "")
		for dir, want := range map[string][]string{"top": {"big", "zz-after", "zz-file.txt"}, "top/big": big, "top/zz-after": {"last.txt"}} {
			if names, err := listNames(ctx, b, dir); err != nil || !slices.Equal(names, want) {
				t.Errorf("List(%q) = %q, %v; want %q", dir, names, err, want)
			}
		}
		warned := false
		for _, line := range Log.Lines()[mark:] {
			warned = warned || strings.Contains(line, "RPROPFIND") && strings.Contains(line, "Internal Server Error")
		}
		if !warned {
			t.Error("the log does not say the deep listing was cut short")
		}
	})
	t.Run("one level", func(t *testing.T) {
		// "/top/" itself and "/top/big/" only.
		b, err := NewWebDAVBackend(fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", walkAbortProxy(t, port, "1", 2)), false, 4)
		if err != nil {
			t.Fatal(err)
		}
		if names, err := listNames(ctx, b, "top"); err == nil {
			t.Errorf("List(top) of a cut answer = %q and no error", names)
		}
	})
}
