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
			b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", pp), false, 4)
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
		b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", walkAbortProxy(t, port, "infinity", 8)), false, 4)
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
		b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", walkAbortProxy(t, port, "1", 2)), false, 4)
		if err != nil {
			t.Fatal(err)
		}
		if names, err := listNames(ctx, b, "top"); err == nil {
			t.Errorf("List(top) of a cut answer = %q and no error", names)
		}
	})
}

// rewriteProxy forwards to the WebDAV server on port with prefix taken off
// the path, as a reverse proxy that publishes the server under prefix does.
// The server's hrefs then lack the prefix.
func rewriteProxy(t *testing.T, port int, prefix string) int {
	t.Helper()
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	srv := httptest.NewServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		r.Out.URL.Path, r.Out.URL.RawPath = strings.TrimPrefix(r.In.URL.Path, prefix), ""
	}})
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().(*net.TCPAddr).Port
}

// W-H5, with real rclone serve webdav: behind a proxy that publishes it
// under /dav/, every href lacks /dav/. Connecting fails and says why; the
// remote side used to read as empty.
func TestWebDAVProxyRewriteFailsToConnect(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	port, _ := startRcloneWebDAV(t, root)
	b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/dav/", rewriteProxy(t, port, "/dav")), false, 4)
	if err == nil {
		names, err := listNames(context.Background(), b, "")
		t.Fatalf("connected; List = %q, %v", names, err)
	}
	if !strings.Contains(err.Error(), "not under /dav") {
		t.Errorf("err = %v, want it to say the server's paths are not under /dav", err)
	}
}

// cannedDAV answers PROPFIND from answers, keyed by Depth and path, with a
// multistatus holding that body. Anything else is 404, Depth: infinity 403.
func cannedDAV(t *testing.T, answers map[string]string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Depth") == "infinity" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		body, ok := answers[r.Header.Get("Depth")+" "+r.URL.Path]
		if r.Method != "PROPFIND" || !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		io.WriteString(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">`+body+`</d:multistatus>`)
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

func davEntry(href string, dir bool, status string) string {
	rt := "<d:resourcetype/>"
	if dir {
		rt = "<d:resourcetype><d:collection/></d:resourcetype>"
	}
	return `<d:response><d:href>` + href + `</d:href><d:propstat><d:prop><d:getcontentlength>3</d:getcontentlength>` +
		rt + `</d:prop><d:status>HTTP/1.1 ` + status + `</d:status></d:propstat></d:response>`
}

// W-H5: a one-level answer with an entry that cannot be mapped fails the
// listing instead of dropping the entry; a raw "#" stays in the name; a
// folder that is now a file, or an answer that does not include the folder,
// is not an empty folder.
func TestWebDAVListingNeverDropsAnEntry(t *testing.T) {
	self := davEntry("/dav/", true, "200 OK")
	for _, tc := range []struct {
		name, dir, body string
		want            []string // nil: the listing must fail
	}{
		{"an entry named by another path", "", self + davEntry("/dav/a.txt", false, "200 OK") + davEntry("/internal/files/b.txt", false, "200 OK") + davEntry("/dav/c/", true, "200 OK"), nil},
		{"an entry with only a 403 propstat", "", self + davEntry("/dav/a.txt", false, "200 OK") + davEntry("/dav/secret/", true, "403 Forbidden"), nil},
		{"a raw # in a name", "", self + davEntry("/dav/a#b.txt", false, "200 OK"), []string{"a#b.txt"}},
		{"a folder that is a file now", "x", davEntry("/dav/x", false, "200 OK"), nil},
		{"an answer without the folder", "", "", nil},
		{"entries deeper than asked", "", self + davEntry("/dav/c/", true, "200 OK") + davEntry("/dav/c/d.txt", false, "200 OK"), []string{"c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := "/dav/"
			if tc.dir != "" {
				path += tc.dir + "/"
			}
			addr := cannedDAV(t, map[string]string{"0 /dav/": self, "1 " + path: tc.body})
			b, err := NewWebDAVBackend(context.Background(), "webdav://"+addr+"/dav/", false, 1)
			if err != nil {
				t.Fatal(err)
			}
			b.PreloadRecursive(context.Background(), "")
			names, err := listNames(context.Background(), b, tc.dir)
			switch {
			case tc.want == nil && err == nil:
				t.Errorf("List(%q) = %q and no error", tc.dir, names)
			case tc.want != nil && (err != nil || !slices.Equal(names, tc.want)):
				t.Errorf("List(%q) = %q, %v; want %q", tc.dir, names, err, tc.want)
			}
		})
	}
}

// W-H5: connecting checks the server's answer about the base itself.
func TestWebDAVConnectChecksTheBase(t *testing.T) {
	for name, self := range map[string]string{
		"the server names the base in another case": davEntry("/DAV/", true, "200 OK"),
		"the base is a file":                        davEntry("/dav", false, "200 OK"),
		"the answer is empty":                       "",
	} {
		addr := cannedDAV(t, map[string]string{"0 /dav/": self})
		if _, err := NewWebDAVBackend(context.Background(), "webdav://"+addr+"/dav/", false, 1); err == nil {
			t.Errorf("%s: connected", name)
		}
	}
}
