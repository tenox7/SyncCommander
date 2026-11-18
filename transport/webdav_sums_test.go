package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"sc/model"
)

// W-M3: a newer listing without a checksum, or with other ones, left the
// cached one. Another client rewrote a file of the same size, and
// Checksum gave the old MD5 with no request: a false "same". Now each
// listing's sums replace what an older one said.
func TestWebDAVNewerListingReplacesTheSums(t *testing.T) {
	var mu sync.Mutex
	sums := map[string]string{"f.txt": "MD5:" + strings.Repeat("a", 32), "g.txt": "MD5:" + strings.Repeat("b", 32)}
	var requests atomic.Int32
	file := func(name string) string {
		cs := ""
		if s := sums[name]; s != "" {
			cs = "<oc:checksums><oc:checksum>" + s + "</oc:checksum></oc:checksums>"
		}
		return `<d:response><d:href>/` + name + `</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:getcontentlength>3</d:getcontentlength><d:resourcetype/>` + cs + `</d:prop></d:propstat></d:response>`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PROPFIND" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		requests.Add(1)
		mu.Lock()
		defer mu.Unlock()
		body := ""
		switch name := strings.Trim(r.URL.Path, "/"); {
		case name != "":
			body = file(name)
		case r.Header.Get("Depth") == "infinity":
			w.WriteHeader(http.StatusForbidden)
			return
		default:
			body = `<d:response><d:href>/</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
				`<d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>`
			if r.Header.Get("Depth") == "1" {
				body += file("f.txt") + file("g.txt")
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">` + body + `</d:multistatus>`))
	}))
	defer srv.Close()
	b, err := NewWebDAVBackend(context.Background(), "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Checksum(ctx, "f.txt", "md5"); err != nil || got != strings.Repeat("a", 32) {
		t.Fatalf("Checksum before = %q, %v", got, err)
	}

	// Another client rewrites both: f.txt now lists with no checksum,
	// g.txt with a SHA1 only.
	mu.Lock()
	sums["f.txt"], sums["g.txt"] = "", "SHA1:"+strings.Repeat("c", 40)
	mu.Unlock()
	if _, err := b.List(model.WithFreshListing(ctx), ""); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"f.txt", "g.txt"} {
		if got, err := b.Checksum(ctx, f, "md5"); err == nil {
			t.Errorf("%s: md5 %q, from before it was rewritten; want no md5", f, got)
		}
	}
	if got, err := b.Checksum(ctx, "g.txt", "sha1"); err != nil || got != strings.Repeat("c", 40) {
		t.Errorf("g.txt: sha1 %q, %v; want the newer listing's", got, err)
	}
}
