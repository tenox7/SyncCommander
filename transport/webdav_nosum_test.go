package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// davSums is a WebDAV server of files in its base, each with the
// oc:checksums text in sums ("" for none). It refuses Depth: infinity and
// counts the PROPFINDs of each path.
type davSums struct {
	mu    sync.Mutex
	sums  map[string]string
	asked map[string]int
}

func openDavSums(t *testing.T, sums map[string]string) (*davSums, string) {
	t.Helper()
	d := &davSums{sums: sums, asked: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(d.serve))
	t.Cleanup(srv.Close)
	return d, "webdav://" + strings.TrimPrefix(srv.URL, "http://") + "/"
}

func (d *davSums) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PROPFIND" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	name := strings.Trim(r.URL.Path, "/")
	d.asked[name]++
	file := func(n string) string {
		cs := ""
		if s := d.sums[n]; s != "" {
			cs = "<oc:checksums><oc:checksum>" + s + "</oc:checksum></oc:checksums>"
		}
		return `<d:response><d:href>/` + n + `</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:getcontentlength>3</d:getcontentlength><d:resourcetype/>` + cs + `</d:prop></d:propstat></d:response>`
	}
	body := ""
	switch _, ok := d.sums[name]; {
	case name != "" && !ok:
		w.WriteHeader(http.StatusNotFound)
		return
	case name != "":
		body = file(name)
	case r.Header.Get("Depth") == "infinity":
		w.WriteHeader(http.StatusForbidden)
		return
	default:
		body = `<d:response><d:href>/</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>`
		if r.Header.Get("Depth") == "1" {
			names := make([]string, 0, len(d.sums))
			for n := range d.sums {
				names = append(names, n)
			}
			slices.Sort(names)
			for _, n := range names {
				body += file(n)
			}
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusMultiStatus)
	w.Write([]byte(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">` + body + `</d:multistatus>`))
}

func (d *davSums) count(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.asked[name]
}

// W-M8: a file an answer gave no sum of the algo asked is asked no more.
// Each was a PROPFIND; a file no listing showed is one, once. None is a
// final answer, and the log says so once. A sum of the wrong length, or
// no hex, is none.
func TestWebDAVFileWithNoSumIsAskedNoMore(t *testing.T) {
	a32, c40 := strings.Repeat("a", 32), strings.Repeat("c", 40)
	d, url := openDavSums(t, map[string]string{
		"f.txt":     "MD5:" + a32,
		"g.txt":     "",
		"h.txt":     "SHA1:" + c40,
		"short.txt": "MD5:abc",
		"long.txt":  "MD5:" + strings.Repeat("d", 64),
		"zzz.txt":   "MD5:" + strings.Repeat("z", 32),
	})
	ctx := context.Background()
	b, err := NewWebDAVBackend(ctx, url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.sums["late.txt"] = "" // no listing showed it
	d.mu.Unlock()
	mark := len(Log.Lines())
	for _, c := range []struct{ name, algo, want string }{
		{"f.txt", "md5", a32},
		{"g.txt", "md5", ""},
		{"h.txt", "md5", ""},
		{"h.txt", "sha1", c40},
		{"short.txt", "md5", ""},
		{"long.txt", "md5", ""},
		{"zzz.txt", "md5", ""},
		{"late.txt", "md5", ""},
		{"late.txt", "sha1", ""},
	} {
		got, err := b.Checksum(ctx, c.name, c.algo)
		switch {
		case c.want != "" && (err != nil || got != c.want):
			t.Errorf("%s: %s = %q, %v; want %q", c.name, c.algo, got, err, c.want)
		case c.want == "" && err == nil:
			t.Errorf("%s: %s = %q; want none", c.name, c.algo, got)
		case c.want == "" && !errors.Is(err, ErrUnsupported):
			t.Errorf("%s: %s: %v; want a final answer (ErrUnsupported)", c.name, c.algo, err)
		}
	}
	for _, name := range []string{"f.txt", "g.txt", "h.txt", "short.txt", "long.txt", "zzz.txt", "late.txt"} {
		want := 0
		if name == "late.txt" {
			want = 1
		}
		if got := d.count(name); got != want {
			t.Errorf("%s asked %d times on its own; want %d", name, got, want)
		}
	}
	warns := 0
	for _, l := range Log.Lines()[mark:] {
		if strings.Contains(l, " webdav WARN ") && strings.Contains(l, "no md5 checksum") {
			warns++
		}
	}
	if warns != 1 {
		t.Errorf("%d warnings of a file with no sum; want 1", warns)
	}
	b.sums.invalidate("g.txt") // a write: what the answer said goes
	if _, err := b.Checksum(ctx, "g.txt", "md5"); err == nil || d.count("g.txt") != 1 {
		t.Errorf("after a write g.txt gave %v and was asked %d times; want none, asked once", err, d.count("g.txt"))
	}
}

// Through the wrapper every remote goes through, a file with no sum is
// one ask: the error was one a retry took for passing, and each such file
// cost six PROPFINDs and some 15 s of waits (W-M8).
func TestWebDAVNoSumIsAFinalAnswer(t *testing.T) {
	d, url := openDavSums(t, map[string]string{"g.txt": ""})
	b := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) { return NewWebDAVBackend(ctx, url, false, 1) })
	defer CloseBackend(b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t0 := time.Now()
	_, err := b.Checksum(ctx, "g.txt", "md5")
	if took := time.Since(t0); err == nil || ctx.Err() != nil || took > 2*time.Second {
		t.Errorf("Checksum = %v after %v; want no sum, at once", err, took.Round(time.Millisecond))
	}
	if n := d.count("g.txt"); n != 1 {
		t.Errorf("g.txt asked %d times; want 1", n)
	}
}

// The sums of 100k files take some 100 bytes a file, at most 280 here
// as the map's room goes. A map of strings for each took some 370.
func TestWebDAVSumsTakeLittleRoom(t *testing.T) {
	const files = 100_000
	names := make([]string, files)
	for i := range names {
		names[i] = fmt.Sprintf("d%03d/f%06d.txt", i%100, i)
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
