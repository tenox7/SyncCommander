package transport

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

// sumTree is a WebDAV server of a tree that does not change, whose files
// may carry checksums (oc:checksums); it counts the PROPFINDs it answers.
type sumTree struct {
	dirs  []string          // folders under the base
	files map[string]string // file -> its checksum ("MD5:..."), or ""
	asked atomic.Int32
}

func (s *sumTree) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PROPFIND" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.asked.Add(1)
	p, depth := strings.Trim(r.URL.Path, "/"), r.Header.Get("Depth")
	if p != "" && !slices.Contains(s.dirs, p) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	shown := func(q string) bool { // p itself, or under it no deeper than asked
		if q == p {
			return true
		}
		rest, ok := q, p == ""
		if !ok {
			rest, ok = strings.CutPrefix(q, p+"/")
		}
		return ok && depth != "0" && (depth == "infinity" || !strings.Contains(rest, "/"))
	}
	var out strings.Builder
	for _, d := range append([]string{""}, s.dirs...) {
		if shown(d) {
			fmt.Fprintf(&out, `<d:response><d:href>%s</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>`+
				`<d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>`, strings.TrimSuffix("/"+d, "/")+"/")
		}
	}
	for _, f := range slices.Sorted(maps.Keys(s.files)) {
		if !shown(f) {
			continue
		}
		cs := ""
		if sum := s.files[f]; sum != "" {
			cs = "<oc:checksums><oc:checksum>" + sum + "</oc:checksum></oc:checksums>"
		}
		fmt.Fprintf(&out, `<d:response><d:href>/%s</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>`+
			`<d:getcontentlength>3</d:getcontentlength><d:resourcetype/>%s</d:prop></d:propstat></d:response>`, f, cs)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusMultiStatus)
	fmt.Fprintf(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">%s</d:multistatus>`, out.String())
}

// W-L3: the checksum probe listed up to four folders again, live, after
// a scan that had listed them: a big folder's whole listing, again, to
// learn which sums the server gives. Now the listings so far say, with no
// request, and a probe lists through the listing cache.
func TestWebDAVChecksumProbeTakesWhatListingsBrought(t *testing.T) {
	ctx := context.Background()
	newDAV := func(t *testing.T, s *sumTree) *WebDAVBackend {
		t.Helper()
		srv := httptest.NewServer(s)
		t.Cleanup(srv.Close)
		b, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	t.Run("after a listing", func(t *testing.T) {
		s := &sumTree{files: map[string]string{"f.txt": "MD5:" + strings.Repeat("a", 32)}}
		b := newDAV(t, s)
		if _, err := b.List(ctx, ""); err != nil {
			t.Fatal(err)
		}
		s.asked.Store(0)
		if algos, err := b.ChecksumAlgos(ctx); err != nil || !slices.Equal(algos, []string{"md5"}) {
			t.Errorf("algos %q, %v; want [md5]", algos, err)
		}
		if n := s.asked.Load(); n != 0 {
			t.Errorf("%d PROPFINDs after a listing that carried the sums; want none", n)
		}
	})
	t.Run("a top level of folders", func(t *testing.T) {
		s := &sumTree{dirs: []string{"a"}, files: map[string]string{"a/f": "SHA1:" + strings.Repeat("c", 40)}}
		if algos, err := newDAV(t, s).ChecksumAlgos(ctx); err != nil || !slices.Equal(algos, []string{"sha1"}) {
			t.Errorf("algos %q, %v; want [sha1]", algos, err)
		}
	})
	t.Run("after a deep listing with no sums", func(t *testing.T) {
		s := &sumTree{dirs: []string{"a", "a/b"}, files: map[string]string{"a/b/f": ""}}
		b := newDAV(t, s)
		if err := b.PreloadRecursive(ctx, ""); err != nil {
			t.Fatal(err)
		}
		for _, d := range []string{"", "a", "a/b"} {
			if _, err := b.List(ctx, d); err != nil {
				t.Fatal(err)
			}
		}
		s.asked.Store(0)
		if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) != 0 {
			t.Errorf("algos %q, %v; want none", algos, err)
		}
		if n := s.asked.Load(); n != 0 {
			t.Errorf("%d PROPFINDs after a deep listing; want none", n)
		}
	})
}
