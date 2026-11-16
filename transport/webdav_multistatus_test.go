package transport

import (
	"strings"
	"testing"
)

// W-H1: a PROPFIND answer is taken as complete only when it is one
// multistatus element with nothing after it but white space.
func TestReadMultistatusRefusesWhatFollowsTheRoot(t *testing.T) {
	resp := func(href string) string {
		return `<D:response><D:href>` + href + `</D:href><D:propstat><D:prop><D:resourcetype/></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`
	}
	ms := `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<D:multistatus xmlns:D="DAV:">` + resp("/a/") + resp("/a/b") + `</D:multistatus>`
	for _, tc := range []struct {
		name, body string
		ok         bool
	}{
		{"the whole answer", ms, true},
		{"white space after it", ms + "\r\n\n", true},
		{"a comment after it", ms + "<!-- done -->\n", true},
		{"x/net/webdav after its walk failed", ms + "Internal Server Error", false},
		{"another element after it", ms + `<D:multistatus xmlns:D="DAV:"></D:multistatus>`, false},
		{"cut off inside", strings.TrimSuffix(ms, "</D:multistatus>"), false},
		{"an HTML page", "<html><body>Sign in</body></html>", false},
		{"nothing", "", false},
	} {
		n := 0
		err := readMultistatus(strings.NewReader(tc.body), func(wdResponse) error { n++; return nil })
		switch {
		case tc.ok && (err != nil || n != 2):
			t.Errorf("%s: %d responses, %v; want 2 and no error", tc.name, n, err)
		case !tc.ok && err == nil:
			t.Errorf("%s: accepted with %d responses", tc.name, n)
		}
	}
}

// W-H5: an href maps to a path under the base or is an error; a raw "#" or
// "?" is part of the name, not a fragment or a query.
func TestWebDAVHrefToRel(t *testing.T) {
	for _, tc := range []struct {
		base, href, want string
		ok               bool
	}{
		{"dav", "/dav/", "", true},
		{"dav", "/dav", "", true},
		{"dav", "/dav/a/b.txt", "a/b.txt", true},
		{"dav", "http://host:8080/dav/a%20b.txt", "a b.txt", true},
		{"dav", "/dav/a#b.txt", "a#b.txt", true},
		{"dav", "/dav/what?.txt", "what?.txt", true},
		{"dav", "/dav/100%.txt", "100%.txt", true},
		{"dav", "/dav//a/./b", "a/b", true},
		{"dav//sub/", "/dav/sub/x", "x", true},
		{"", "/a/b", "a/b", true},
		{"", "https://host/", "", true},
		{"dav", "/internal/files/a.txt", "", false},
		{"dav", "/DAV/a.txt", "", false},
		{"dav", "/davx/a.txt", "", false},
		{"dav", "/dav/../etc/passwd", "", false},
		{"dav", "a.txt", "", false},
	} {
		b := &WebDAVBackend{httpBase: &httpBase{base: tc.base}}
		got, err := b.hrefToRel(tc.href)
		if tc.ok && (err != nil || got != tc.want) || !tc.ok && err == nil {
			t.Errorf("base %q: hrefToRel(%q) = %q, %v; want %q ok=%v", tc.base, tc.href, got, err, tc.want, tc.ok)
		}
	}
}
