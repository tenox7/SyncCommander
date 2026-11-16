package transport

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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
