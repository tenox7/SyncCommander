package transport

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"sc/model"
)

// sumsDAV is a WebDAV server of a tree held in memory, whose files carry
// their MD5 (oc:checksums). It answers PROPFIND at depth 0, 1 and, unless
// finite, infinity, with a folder's entries before those under it; and
// DELETE, MOVE and MKCOL. A path has no slash at either end; a folder's
// content is "/".
type sumsDAV struct {
	mu     sync.Mutex
	tree   map[string]string
	finite bool
	asked  map[string]int // PROPFINDs of each path
}

func (s *sumsDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.Trim(r.URL.Path, "/")
	_, there := s.tree[p]
	there = there || p == ""
	under := func(p string) []string {
		var out []string
		for k := range s.tree {
			if p == "" || strings.HasPrefix(k, p+"/") {
				out = append(out, k)
			}
		}
		// A folder's entries come before those under it.
		slices.SortFunc(out, func(a, b string) int {
			return strings.Compare(strings.ReplaceAll(a, "/", "\x00"), strings.ReplaceAll(b, "/", "\x00"))
		})
		return out
	}
	switch r.Method {
	case "PROPFIND":
		if s.asked == nil {
			s.asked = map[string]int{}
		}
		s.asked[p]++
		depth := r.Header.Get("Depth")
		switch {
		case depth == "infinity" && s.finite:
			w.WriteHeader(http.StatusForbidden)
			return
		case !there:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body := s.response(p)
		if depth != "0" && (p == "" || s.tree[p] == "/") {
			for _, k := range under(p) {
				if depth == "1" && parentDir(k) != p {
					continue
				}
				body += s.response(k)
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		fmt.Fprintf(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">%s</d:multistatus>`, body)
	case "DELETE":
		if !there {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		for _, k := range under(p) {
			delete(s.tree, k)
		}
		delete(s.tree, p)
		w.WriteHeader(http.StatusNoContent)
	case "MOVE":
		u, err := url.Parse(r.Header.Get("Destination"))
		if err != nil || !there {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		to := strings.Trim(u.Path, "/")
		if _, taken := s.tree[to]; taken {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		for _, k := range append(under(p), p) {
			s.tree[to+strings.TrimPrefix(k, p)] = s.tree[k]
			delete(s.tree, k)
		}
		w.WriteHeader(http.StatusCreated)
	case "MKCOL":
		if there {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.tree[p] = "/"
		w.WriteHeader(http.StatusCreated)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *sumsDAV) response(p string) string {
	prop := `<d:resourcetype><d:collection/></d:resourcetype>`
	href := "/" + p + "/"
	if content := s.tree[p]; p != "" && content != "/" {
		sum := md5.Sum([]byte(content))
		prop = fmt.Sprintf(`<d:getcontentlength>%d</d:getcontentlength><d:resourcetype/>`+
			`<oc:checksums><oc:checksum>MD5:%s</oc:checksum></oc:checksums>`, len(content), hex.EncodeToString(sum[:]))
		href = "/" + p
	}
	if p == "" {
		href = "/"
	}
	return `<d:response><d:href>` + href + `</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
		prop + `</d:prop></d:propstat></d:response>`
}

func (s *sumsDAV) set(p, content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if content == "" {
		delete(s.tree, p)
		return
	}
	s.tree[p] = content
}

func (s *sumsDAV) askedFor(p string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.asked[p]
}

func newSumsDAVBackend(t *testing.T, s *sumsDAV) *WebDAVBackend {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(context.Background(), "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 8)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// held is how many files the cache holds an answer about.
func (c *wdSumCache) held() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, f := range c.folders {
		n += f.sums.len()
		if f.none != nil {
			n += f.none.len()
		}
	}
	return n
}

// sumsHeld reports, for each path, whether the cache holds its MD5.
func sumsHeld(b *WebDAVBackend, paths ...string) map[string]bool {
	out := map[string]bool{}
	for _, p := range paths {
		_, ok, _ := b.sums.get(p, "md5")
		out[p] = ok
	}
	return out
}

func checkHeld(t *testing.T, when string, b *WebDAVBackend, want map[string]bool) {
	t.Helper()
	var paths []string
	for p := range want {
		paths = append(paths, p)
	}
	for p, ok := range sumsHeld(b, paths...) {
		if ok != want[p] {
			t.Errorf("%s: the sum of %s held = %v, want %v", when, p, ok, want[p])
		}
	}
}

func sumsTree() map[string]string {
	return map[string]string{
		"top.txt": "top", "d": "/", "d/f.txt": "one", "d/s": "/", "d/s/g.txt": "two",
		"e": "/", "e/h.txt": "three",
	}
}

// W-M8: the sums of the files under a folder sc deleted or moved whole
// stayed in memory for the session. They go with their folder now; the
// rest stay.
func TestWebDAVSumsGoWithAFolderDeletedOrMoved(t *testing.T) {
	dav := &sumsDAV{tree: sumsTree(), finite: true}
	b := newSumsDAVBackend(t, dav)
	ctx := context.Background()
	for _, dir := range []string{"", "d", "d/s", "e"} {
		if _, err := b.List(ctx, dir); err != nil {
			t.Fatal(err)
		}
	}
	checkHeld(t, "listed", b, map[string]bool{"top.txt": true, "d/f.txt": true, "d/s/g.txt": true, "e/h.txt": true})
	if err := b.RemoveAll(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	checkHeld(t, "after a delete of d", b, map[string]bool{"top.txt": true, "d/f.txt": false, "d/s/g.txt": false, "e/h.txt": true})
	if err := b.Rename(ctx, "e", "e2"); err != nil {
		t.Fatal(err)
	}
	checkHeld(t, "after a move of e", b, map[string]bool{"top.txt": true, "e/h.txt": false})
	if n := b.sums.held(); n != 1 {
		t.Errorf("the cache holds answers about %d files; want 1, top.txt", n)
	}
}

// W-M8: a file or a folder another client deleted kept its sums, and the
// files under that folder theirs, for the session. A listing that no
// longer gives them drops them. A folder that is still there keeps the
// sums under it, with no request: it is not listed again.
func TestWebDAVSumsGoWhenAListingNoLongerGivesThem(t *testing.T) {
	dav := &sumsDAV{tree: sumsTree(), finite: true}
	b := newSumsDAVBackend(t, dav)
	ctx := context.Background()
	for _, dir := range []string{"", "d", "d/s", "e"} {
		if _, err := b.List(ctx, dir); err != nil {
			t.Fatal(err)
		}
	}
	dav.set("d/f.txt", "")
	dav.set("d/s/g.txt", "")
	dav.set("d/s", "")
	if _, err := b.List(model.WithFreshListing(ctx), "d"); err != nil {
		t.Fatal(err)
	}
	checkHeld(t, "d listed again", b, map[string]bool{"top.txt": true, "d/f.txt": false, "d/s/g.txt": false, "e/h.txt": true})

	dav.set("top.txt", "")
	if _, err := b.List(model.WithFreshListing(ctx), ""); err != nil {
		t.Fatal(err)
	}
	checkHeld(t, "the base listed again", b, map[string]bool{"top.txt": false, "e/h.txt": true})
	if _, err := b.Checksum(ctx, "e/h.txt", "md5"); err != nil {
		t.Fatal(err)
	}
	if n := dav.askedFor("e/h.txt"); n != 0 {
		t.Errorf("e/h.txt was asked for %d times; its folder is still there, so its sum is held", n)
	}
}

// W-M8: a recursive listing drops the sums of what it no longer gives: a
// folder in a folder it listed whole, and in the scope.
func TestWebDAVPreloadDropsTheSumsItNoLongerGives(t *testing.T) {
	dav := &sumsDAV{tree: sumsTree()}
	b := newSumsDAVBackend(t, dav)
	ctx := context.Background()
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	checkHeld(t, "preloaded", b, map[string]bool{"top.txt": true, "d/f.txt": true, "d/s/g.txt": true, "e/h.txt": true})
	dav.set("d/f.txt", "")
	dav.set("d/s/g.txt", "")
	dav.set("d/s", "")
	dav.set("d/new.txt", "new")
	dav.set("e/h.txt", "")
	dav.set("e", "")
	if err := b.PreloadRecursive(model.WithFreshListing(ctx), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	checkHeld(t, "preloaded again", b, map[string]bool{"top.txt": true, "d/new.txt": true, "d/f.txt": false, "d/s/g.txt": false, "e/h.txt": false})
	if n := b.sums.held(); n != 2 {
		t.Errorf("the cache holds answers about %d files; want 2", n)
	}
}

// A fewMap gives what it was given, below, at and above fewMax entries.
func TestFewMap(t *testing.T) {
	var m fewMap[int]
	for n := 1; n <= 3*fewMax; n++ {
		m.set(fmt.Sprint(n), n)
		m.set(fmt.Sprint(n), -n) // a second set replaces
		if m.len() != n {
			t.Fatalf("after %d sets len = %d", n, m.len())
		}
		for i := 1; i <= n; i++ {
			if v, ok := m.get(fmt.Sprint(i)); !ok || v != -i {
				t.Fatalf("after %d sets get(%d) = %d, %v", n, i, v, ok)
			}
		}
	}
	m.del("2")
	m.del("nothing")
	seen := 0
	m.each(func(name string, v int) {
		seen++
		if name == "2" {
			t.Error("2 is there after its del")
		}
	})
	if seen != 3*fewMax-1 || m.len() != seen {
		t.Errorf("each gave %d entries, len %d; want %d", seen, m.len(), 3*fewMax-1)
	}
	var few fewMap[int]
	few.set("a", 1)
	few.set("b", 2)
	few.del("a")
	if _, ok := few.get("a"); ok || few.len() != 1 {
		t.Errorf("a short list kept a after its del, len %d", few.len())
	}
}

// The sums by folder take little room in folders of two files too: a Go
// map for each such folder took some 450 bytes a file (W-M8).
func TestWebDAVSumsInSmallFoldersTakeLittleRoom(t *testing.T) {
	const files = 100_000
	names := make([]string, files)
	for i := range names {
		names[i] = fmt.Sprintf("d%03d/s%05d/f%06d.txt", (i/2)%100, i/2, i)
	}
	md5s, sha1s := strings.Repeat("a", 32), strings.Repeat("b", 40)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	c := newWDSumCache()
	for _, n := range names {
		c.put(0, n, false, map[string]string{"md5": md5s, "sha1": sha1s})
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / files
	runtime.KeepAlive(c)
	runtime.KeepAlive(names)
	t.Logf("%d bytes a file", per)
	if per > 280 {
		t.Errorf("the sums take %d bytes a file; want 280 at most", per)
	}
}
