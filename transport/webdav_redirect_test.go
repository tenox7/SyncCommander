package transport

import (
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

// apacheProxy forwards to the WebDAV server on port, serving root, and
// answers as Apache's mod_dir does: 301 to the path with a slash, for any
// method on a folder's path without one. The Location names the host the
// request named, or, with canonical set, that host (UseCanonicalName On:
// the name Apache calls itself, which a client that came through ssh -L may
// not reach). moved.txt is sent elsewhere with a 302. seen returns the
// requests since its last call, as "METHOD path".
func apacheProxy(t *testing.T, port int, root, canonical string) (proxyPort int, seen func() []string) {
	t.Helper()
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	rp := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		r.Out.Host = r.In.Host // a MOVE's Destination names this host
	}}
	var mu sync.Mutex
	var reqs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqs = append(reqs, r.Method+" "+r.URL.Path)
		mu.Unlock()
		p := r.URL.Path
		if p == "/moved.txt" {
			http.Redirect(w, r, "/elsewhere.txt", http.StatusFound)
			return
		}
		if !strings.HasSuffix(p, "/") {
			if fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); err == nil && fi.IsDir() {
				host := r.Host
				if canonical != "" {
					host = canonical
				}
				w.Header().Set("Location", "http://"+host+p+"/")
				w.WriteHeader(http.StatusMovedPermanently)
				return
			}
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().(*net.TCPAddr).Port, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := reqs
		reqs = nil
		return out
	}
}

func gets(reqs []string) []string {
	var out []string
	for _, r := range reqs {
		if strings.HasPrefix(r, "GET ") {
			out = append(out, r)
		}
	}
	return out
}

// W-H3: Go's client sends a request that got a 301, 302 or 303 again as a
// GET, and sc took the GET's 200 for the answer to the MOVE, PUT or DELETE.
// Apache answers 301 to any method on a folder's path without the slash:
// a folder was not moved or deleted, and said so with nil. Such a request
// is now sent again at the slash, with its own method, on the host sc was
// given; a PUT or GET there fails (the name is a folder), as does any other
// redirect. No GET is sent that sc did not mean.
func TestWebDAVRedirectIsNotFollowedAsAGet(t *testing.T) {
	t.Run("Location on the host asked", func(t *testing.T) { redirectedWebDAV(t, "") })
	// A closed port: the old client failed there, before it sent the GET.
	t.Run("Location on Apache's own name", func(t *testing.T) { redirectedWebDAV(t, "127.0.0.1:1") })
}

func redirectedWebDAV(t *testing.T, canonical string) {
	root := t.TempDir()
	for _, d := range []string{"d1", "d2/sub", "d3", "d4", "d5", "d6", "d7"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"d1/x.txt", "d2/sub/y.txt", "elsewhere.txt"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	port, _ := startRcloneWebDAV(t, root)
	proxy, seen := apacheProxy(t, port, root, canonical)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	b, err := NewWebDAVBackend(ctx, fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", proxy), false, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	exists := func(rel string) bool {
		_, err := os.Stat(filepath.Join(root, rel))
		return err == nil
	}
	for _, c := range []struct {
		name string
		do   func() error
		ok   bool        // the call should work
		then func() bool // and leave this true
	}{
		{"MOVE of a folder", func() error { return b.Rename(ctx, "d1", "e1") }, true,
			func() bool { return exists("e1/x.txt") && !exists("d1") }},
		{"DELETE of a folder and all in it", func() error { return b.RemoveAll(ctx, "d2") }, true,
			func() bool { return !exists("d2") }},
		{"DELETE of an empty folder", func() error { return b.Remove(ctx, "d3") }, true,
			func() bool { return !exists("d3") }},
		{"PROPFIND of a folder", func() error {
			e, err := b.Stat(ctx, "d7")
			if err == nil && !e.IsDir {
				err = fmt.Errorf("Stat(d7) = %+v, not a folder", e)
			}
			return err
		}, true, func() bool { return true }},
		{"PUT onto a folder's name", func() error {
			return b.CopyFrom(ctx, "d5", strings.NewReader("data"), model.PutParams{Size: 4, Mode: 0o644})
		}, false, func() bool { fi, err := os.Stat(filepath.Join(root, "d5")); return err == nil && fi.IsDir() }},
		{"GET of a folder's name", func() error {
			rc, err := b.Open(ctx, "d6")
			if err == nil {
				body, _ := io.ReadAll(rc)
				rc.Close()
				return fmt.Errorf("read %d bytes: %.60q", len(body), body)
			}
			return nil
		}, true, func() bool { return true }},
		{"PUT redirected elsewhere", func() error {
			return b.CopyFrom(ctx, "moved.txt", strings.NewReader("data"), model.PutParams{Size: 4, Mode: 0o644})
		}, false, func() bool { return !exists("moved.txt") }},
		{"DELETE redirected elsewhere", func() error { return b.Remove(ctx, "moved.txt") }, false,
			func() bool { return exists("elsewhere.txt") }},
	} {
		seen()
		err := c.do()
		reqs := seen()
		if c.ok && err != nil {
			t.Errorf("%s: %v (requests %q)", c.name, err, reqs)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: no error (requests %q)", c.name, reqs)
		}
		if !c.then() {
			t.Errorf("%s: the server's files are not as they should be (requests %q)", c.name, reqs)
		}
		if g := gets(reqs); len(g) > 0 && !(c.name == "GET of a folder's name" && slices.Equal(g, []string{"GET /d6"})) {
			t.Errorf("%s: sc sent %q", c.name, g)
		}
	}
	seen()
	b.SetTimes(ctx, "d4", time.Unix(1e9, 0), time.Time{}, time.Time{})
	if reqs := seen(); len(gets(reqs)) > 0 || !slices.Contains(reqs, "PROPPATCH /d4/") {
		t.Errorf("PROPPATCH of a folder: requests %q, want it sent again at /d4/ and no GET", reqs)
	}
}

// W-H3, for every HTTP backend (restic's too): no redirect is followed. A
// redirect to the same path with a slash is a folder; a method that may name
// one is sent once more there, with its body.
func TestHTTPBaseFollowsNoRedirect(t *testing.T) {
	var mu sync.Mutex
	var reqs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqs = append(reqs, strings.TrimSpace(r.Method+" "+r.URL.Path+" "+string(body)))
		mu.Unlock()
		switch r.URL.Path {
		case "/f":
			http.Redirect(w, r, "/g", http.StatusSeeOther)
		case "/d":
			http.Redirect(w, r, "/d/", http.StatusPermanentRedirect)
		case "/s":
			http.Redirect(w, r, "/s/", http.StatusSeeOther)
		case "/loop":
			http.Redirect(w, r, "/loop/", http.StatusMovedPermanently)
		case "/loop/":
			// Set by hand: http.Redirect would clean the path.
			w.Header().Set("Location", "/loop//")
			w.WriteHeader(http.StatusMovedPermanently)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	host, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	h, err := newHTTPBase("restic", false, host, port, "", "", "", false, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, c := range []struct {
		method, path, body string
		dir                bool
		ok                 bool
		msg                string // in the error
		want               []string
	}{
		{"POST", "f", "data", false, false, "to /g", []string{"POST /f data"}},
		{"DELETE", "f", "", false, false, "to /g", []string{"DELETE /f"}},
		{"PROPFIND", "d", "<propfind/>", false, true, "", []string{"PROPFIND /d <propfind/>", "PROPFIND /d/ <propfind/>"}},
		{"DELETE", "d", "", false, true, "", []string{"DELETE /d", "DELETE /d/"}},
		{"GET", "d", "", false, false, "is a folder", []string{"GET /d"}},
		{"PUT", "d", "data", false, false, "is a folder", []string{"PUT /d data"}},
		{"DELETE", "loop", "", false, false, "follows no redirect", []string{"DELETE /loop", "DELETE /loop/"}},
		{"PROPFIND", "loop", "", true, false, "follows no redirect", []string{"PROPFIND /loop/"}},
		// 303 says where the answer is, not that the path is a folder.
		{"DELETE", "s", "", false, false, "follows no redirect", []string{"DELETE /s"}},
	} {
		mu.Lock()
		reqs = nil
		mu.Unlock()
		var body io.Reader
		if c.body != "" {
			body = strings.NewReader(c.body)
		}
		resp, err := h.do(ctx, c.method, h.urlFor(c.path, c.dir), body, nil)
		if err == nil {
			drainClose(resp.Body)
			if resp.StatusCode/100 != 2 {
				err = fmt.Errorf("%s", resp.Status)
			}
		}
		mu.Lock()
		got := reqs
		mu.Unlock()
		if (err == nil) != c.ok || !slices.Equal(got, c.want) || err != nil && !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s /%s: %v; requests %q, want %q (ok %v, %q)", c.method, c.path, err, got, c.want, c.ok, c.msg)
		}
	}
}
