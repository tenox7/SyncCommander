package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// davMtimes is a WebDAV server in memory that keeps an mtime as one kind
// of server does: header takes X-OC-Mtime on a PUT, accepted answers it
// "accepted", patch takes a PROPPATCH of DAV:lastmodified. A time it does
// not take is the upload's.
type davMtimes struct {
	header, accepted, patch bool

	mu          sync.Mutex
	files       map[string]davFile
	proppatches int
	stats       int // PROPFIND Depth 0 of a file
}

type davFile struct {
	size  int
	mtime time.Time
}

var davUploadTime = time.Date(2031, 1, 2, 3, 4, 5, 0, time.UTC)

var davLastmodRE = regexp.MustCompile(`<d:lastmodified>(\d+)</d:lastmodified>`)

func (d *davMtimes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	name := strings.Trim(r.URL.Path, "/")
	resp := func(href string, f davFile, dir bool) string {
		rt, size := "<d:resourcetype/>", fmt.Sprintf("<d:getcontentlength>%d</d:getcontentlength>", f.size)
		if dir {
			rt, size = "<d:resourcetype><d:collection/></d:resourcetype>", ""
		}
		return `<d:response><d:href>` + href + `</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:getlastmodified>` + f.mtime.UTC().Format(http.TimeFormat) + `</d:getlastmodified>` + size + rt +
			`</d:prop></d:propstat></d:response>`
	}
	ms := func(body string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		io.WriteString(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">`+body+`</d:multistatus>`)
	}
	switch r.Method {
	case "PUT":
		body, _ := io.ReadAll(r.Body)
		f := davFile{size: len(body), mtime: davUploadTime}
		if s := r.Header.Get("X-OC-Mtime"); s != "" && d.header {
			n, _ := strconv.ParseInt(s, 10, 64)
			f.mtime = time.Unix(n, 0)
			if d.accepted {
				w.Header().Set("X-OC-Mtime", "accepted")
			}
		}
		d.files[name] = f
		w.WriteHeader(http.StatusCreated)
	case "PROPPATCH":
		d.proppatches++
		body, _ := io.ReadAll(r.Body)
		f, ok := d.files[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if m := davLastmodRE.FindSubmatch(body); m != nil && d.patch {
			n, _ := strconv.ParseInt(string(m[1]), 10, 64)
			f.mtime = time.Unix(n, 0)
			d.files[name] = f
		}
		// Taken or not, the answer says it was: as Apache's mod_dav does
		// for a property it stores and does not apply.
		ms(`<d:response><d:href>/` + name + `</d:href><d:propstat><d:prop><d:lastmodified/></d:prop>` +
			`<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`)
	case "PROPFIND":
		switch depth := r.Header.Get("Depth"); {
		case depth == "infinity":
			w.WriteHeader(http.StatusForbidden)
		case name == "":
			out := resp("/", davFile{mtime: davUploadTime}, true)
			if depth == "1" {
				for n, f := range d.files {
					out += resp("/"+n, f, false)
				}
			}
			ms(out)
		default:
			f, ok := d.files[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			d.stats++
			ms(resp("/"+name, f, false))
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (d *davMtimes) counts() (proppatches, stats int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.proppatches, d.stats
}

func openDavMtimes(t *testing.T, d *davMtimes) *WebDAVBackend {
	t.Helper()
	d.files = map[string]davFile{}
	srv := httptest.NewServer(d)
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(context.Background(), "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 4)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var davMtime = time.Date(2026, 11, 18, 12, 34, 56, 0, time.UTC)

// davCopy puts each file there as a copy does, in name order: a PUT with
// the time, then SetTimes. It returns what each SetTimes said.
func davCopy(t *testing.T, b *WebDAVBackend, names ...string) map[string]error {
	t.Helper()
	ctx := context.Background()
	errs := map[string]error{}
	for _, name := range slices.Sorted(slices.Values(names)) {
		if err := b.CopyFrom(ctx, name, strings.NewReader("abc"), model.PutParams{Size: 3, ModTime: davMtime}); err != nil {
			t.Fatal(err)
		}
		errs[name] = b.SetTimes(ctx, name, davMtime, davMtime, time.Time{})
	}
	return errs
}

func davListedTimes(t *testing.T, b *WebDAVBackend) map[string]time.Time {
	t.Helper()
	entries, err := b.List(model.WithFreshListing(context.Background()), "")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]time.Time{}
	for _, e := range entries {
		out[e.Name] = e.ModTime
	}
	return out
}

// W-M4: the mtime went out twice for every file, in X-OC-Mtime and in a
// PROPPATCH, though ownCloud and Nextcloud answer "accepted" to the first.
// Now a server that says so gets no PROPPATCH.
func TestWebDAVNoPROPPATCHWhenTheServerTookTheTime(t *testing.T) {
	d := &davMtimes{header: true, accepted: true, patch: true}
	b := openDavMtimes(t, d)
	for name, err := range davCopy(t, b, "a.txt", "b.txt") {
		if err != nil {
			t.Errorf("SetTimes %s: %v", name, err)
		}
	}
	if p, s := d.counts(); p != 0 || s != 0 {
		t.Errorf("%d PROPPATCH and %d checks; want none of either", p, s)
	}
	for name, mt := range davListedTimes(t, b) {
		if !mt.Equal(davMtime) {
			t.Errorf("%s lists %v, want %v", name, mt, davMtime)
		}
	}
}

// W-M4: rclone serve webdav takes X-OC-Mtime and says nothing. One check
// sees the time, and no file gets a PROPPATCH.
func TestWebDAVChecksOnceThatTheHeaderTookTheTime(t *testing.T) {
	d := &davMtimes{header: true}
	b := openDavMtimes(t, d)
	for name, err := range davCopy(t, b, "a.txt", "b.txt", "c.txt") {
		if err != nil {
			t.Errorf("SetTimes %s: %v", name, err)
		}
	}
	if p, s := d.counts(); p != 0 || s != 1 {
		t.Errorf("%d PROPPATCH and %d checks; want no PROPPATCH, one check", p, s)
	}
}

// A server that takes only the PROPPATCH keeps getting it, and no file's
// time is taken for lost.
func TestWebDAVServerThatTakesOnlyPROPPATCH(t *testing.T) {
	d := &davMtimes{patch: true}
	b := openDavMtimes(t, d)
	for name, err := range davCopy(t, b, "a.txt", "b.txt") {
		if err != nil {
			t.Errorf("SetTimes %s: %v", name, err)
		}
	}
	if p, _ := d.counts(); p != 2 {
		t.Errorf("%d PROPPATCH, want one per file", p)
	}
	for name, mt := range davListedTimes(t, b) {
		if !mt.Equal(davMtime) {
			t.Errorf("%s lists %v, want %v", name, mt, davMtime)
		}
	}
}

// W-M4: a server that answers the PROPPATCH and keeps neither time lists
// the upload's time, and every file copied there was "different" after the
// copy. Now one check finds that: SetTimes says the server keeps no time,
// sends nothing more, and the compare leaves its mtimes out.
func TestWebDAVServerThatKeepsNoMtime(t *testing.T) {
	d := &davMtimes{}
	b := openDavMtimes(t, d)
	errs := davCopy(t, b, "a.txt", "b.txt")
	for name, err := range errs {
		if !errors.Is(err, ErrUnsupported) {
			t.Errorf("SetTimes %s = %v, want ErrUnsupported: the server keeps no time", name, err)
		}
	}
	if p, s := d.counts(); p != 1 || s != 2 {
		t.Errorf("%d PROPPATCH and %d checks; want 1 and 2, for the first file only", p, s)
	}
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt"} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("abc"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, davMtime, davMtime); err != nil {
			t.Fatal(err)
		}
	}
	opts := model.CompareOpts{Size: true, ModTime: true}
	s := model.NewScanner(NewLocalBackend(dir), b, 1, 1, true)
	s.Scan(context.Background(), opts)
	s.ReadTree(func(root *model.TreeNode) {
		for _, name := range []string{"a.txt", "b.txt"} {
			if n := childNamed(root, name); n == nil || model.NodeStatus(n, &opts) != model.AttrEqual {
				t.Errorf("%s is not equal after its copy", name)
			}
		}
	})
}

// W-M4: a restic object keeps no time, and listed none: zero, so every
// object was "different" from a copy anywhere else, by mtime. Now the
// compare leaves its mtime out.
func TestResticObjectsCompareWithoutMtime(t *testing.T) {
	right, _ := openFakeRestic(t, map[string]string{"config": "cfg", "keys/" + keyA: "key"})
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "keys"), 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "keys", keyA)
	if err := os.WriteFile(p, []byte("key"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts := model.CompareOpts{Size: true, ModTime: true}
	s := model.NewScanner(NewLocalBackend(dir), right, 1, 1, true)
	s.Scan(context.Background(), opts)
	s.ReadTree(func(root *model.TreeNode) {
		keys := childNamed(root, "keys")
		if keys == nil {
			t.Error("keys is not in the tree")
			return
		}
		if n := childNamed(keys, keyA); n == nil || model.NodeStatus(n, &opts) != model.AttrEqual {
			t.Errorf("keys/%s is not equal to its copy", keyA)
		}
	})
}
