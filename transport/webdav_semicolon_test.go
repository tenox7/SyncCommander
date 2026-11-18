package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"sc/model"
)

// servletPaths hands next each request with its path as a servlet
// container (Tomcat, Jetty) reads it: every name cut at its first ;, where
// a path parameter starts (;jsessionid=...), before it is unescaped. The
// path in a MOVE's Destination alike.
func servletPaths(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _, _ := strings.Cut(r.RequestURI, "?")
		r.URL.Path, r.URL.RawPath = cutPathParams(raw), ""
		if d := r.Header.Get("Destination"); d != "" {
			if u, err := url.Parse(d); err == nil {
				u.Path, u.RawPath = cutPathParams(u.EscapedPath()), ""
				r.Header.Set("Destination", u.String())
			}
		}
		next.ServeHTTP(w, r)
	})
}

// cutPathParams is the escaped path raw, each name cut at its first ; and
// then unescaped.
func cutPathParams(raw string) string {
	names := strings.Split(raw, "/")
	for i, n := range names {
		n, _, _ = strings.Cut(n, ";")
		if u, err := url.PathUnescape(n); err == nil {
			n = u
		}
		names[i] = n
	}
	return strings.Join(names, "/")
}

// W-L3: Go escapes a URL's path as a whole and leaves ; as it is, and a
// servlet container cuts a name at its ;. A PUT of x;y overwrote x, a
// MOVE of it took x's name, and a DELETE of it removed x. Now each name
// is escaped as a name, so x;y is x;y on every server.
func TestWebDAVNameWithASemicolonIsThatName(t *testing.T) {
	ctx := context.Background()
	s := newWDTree()
	srv := httptest.NewServer(servletPaths(s))
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	put := func(name, body string) {
		t.Helper()
		if err := b.CopyFrom(ctx, name, strings.NewReader(body), model.PutParams{Size: int64(len(body))}); err != nil {
			t.Fatalf("PUT %s: %v", name, err)
		}
	}
	put("x", "keep")
	put("x;y", "other")
	if a, c := s.holds("x"), s.holds("x;y"); a != "keep" || c != "other" {
		t.Fatalf("after a PUT of x;y, x holds %q (want keep), x;y holds %q (want other)", a, c)
	}
	if e, err := b.Stat(ctx, "x;y"); err != nil || e.Size != int64(len("other")) {
		t.Errorf("Stat of x;y: size %d, %v; want %d", e.Size, err, len("other"))
	}
	if err := b.Mkdir(ctx, "d;e", 0o755); err != nil {
		t.Fatal(err)
	}
	put("d;e/f", "in")
	if got := s.holds("d;e/f"); got != "in" {
		t.Errorf("d;e/f holds %q, want in", got)
	}
	if err := b.Rename(ctx, "x;y", "z;w"); err != nil {
		t.Fatal(err)
	}
	if a, c := s.holds("x"), s.holds("z;w"); a != "keep" || c != "other" {
		t.Errorf("after a MOVE of x;y to z;w, x holds %q (want keep), z;w holds %q (want other)", a, c)
	}
	if err := b.Remove(ctx, "z;w"); err != nil {
		t.Fatal(err)
	}
	if a, c := s.holds("x"), s.holds("z;w"); a != "keep" || c != "" {
		t.Errorf("after a DELETE of z;w, x holds %q (want keep), z;w holds %q (want it gone)", a, c)
	}
}
