package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// webTime is the mtime of every file of a webTree: to the second, as
// Last-Modified gives it.
var webTime = time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)

// webTree makes files (path → content) under a new folder, with webTime,
// and gives the folder. A path that ends with / is an empty folder.
func webTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if strings.HasSuffix(p, "/") {
			if err := os.MkdirAll(full, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(full, webTime, webTime); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// fakeWeb is a web server that lists folders: Go's own file server, the
// requests it gets kept, and some answered with a status set for them.
type fakeWeb struct {
	h       http.Handler
	mu      sync.Mutex
	reqs    []string          // "METHOD /path"
	accepts []string          // the Accept of each GET of a folder
	status  map[string]int    // "METHOD /path" or "METHOD *" → the status to answer
	pages   map[string]string // "/path/" → a page served for it instead
}

func newFakeWeb(t *testing.T, root string) (*fakeWeb, string) {
	t.Helper()
	f := &fakeWeb{h: http.FileServer(http.Dir(root)), status: map[string]int{}, pages: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, "http://" + srv.Listener.Addr().String()
}

func (f *fakeWeb) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	f.mu.Lock()
	f.reqs = append(f.reqs, key)
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/") {
		f.accepts = append(f.accepts, r.Header.Get("Accept"))
	}
	code := f.status[key]
	if code == 0 {
		code = f.status[r.Method+" *"]
	}
	page, ok := f.pages[r.URL.Path]
	f.mu.Unlock()
	switch {
	case code != 0:
		http.Error(w, http.StatusText(code), code)
	case ok:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, page)
	default:
		f.h.ServeHTTP(w, r)
	}
}

func (f *fakeWeb) set(req string, code int) {
	f.mu.Lock()
	f.status[req] = code
	f.mu.Unlock()
}

func (f *fakeWeb) page(path, page string) {
	f.mu.Lock()
	f.pages[path] = page
	f.mu.Unlock()
}

// taken gives the requests since its last call.
func (f *fakeWeb) taken() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.reqs
	f.reqs = nil
	return out
}

func countPrefix(reqs []string, prefix string) int {
	n := 0
	for _, r := range reqs {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

func openWeb(t *testing.T, url string) *HTTPBackend {
	t.Helper()
	b, err := NewHTTPBackend(context.Background(), url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// rowsOf gives entries as "name size" for files, "name/" for folders and
// "name -> target" for links.
func rowsOf(entries []model.FileEntry) []string {
	var out []string
	for _, e := range entries {
		target, link := e.Target()
		switch {
		case e.IsDir:
			out = append(out, e.RelPath+"/")
		case link:
			out = append(out, e.RelPath+" -> "+target)
		default:
			out = append(out, fmt.Sprintf("%s %d", e.RelPath, e.Size))
		}
	}
	slices.Sort(out)
	return out
}

func wantRows(t *testing.T, what string, entries []model.FileEntry, err error, want ...string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	slices.Sort(want)
	if got := rowsOf(entries); !slices.Equal(got, want) {
		t.Errorf("%s: %q, want %q", what, got, want)
	}
}

var webFiles = map[string]string{
	"a.txt":            "hello",
	"my file.bin":      "0123456789",
	"docs/b.txt":       "bb",
	"docs/sub/c.bin":   "ccc",
	"docs/sub/d e.txt": "",
	"empty/":           "",
}

// A folder is its page's entries, each file with the size and time its
// HEAD gives, exactly.
func TestHTTPListsFolders(t *testing.T) {
	_, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url+"/")
	ctx := context.Background()
	e, err := b.List(ctx, "")
	wantRows(t, "root", e, err, "a.txt 5", "my file.bin 10", "docs/", "empty/")
	for _, f := range e {
		if !f.IsDir && (!f.ModTime.Equal(webTime) || f.TimeRes != model.TimeSecond || f.Mode != 0o644) {
			t.Errorf("%s: %v (%v, %v), want %v to the second", f.RelPath, f.ModTime, f.TimeRes, f.Mode, webTime)
		}
	}
	e, err = b.List(ctx, "docs/sub")
	wantRows(t, "docs/sub", e, err, "docs/sub/c.bin 3", "docs/sub/d e.txt 0")
	e, err = b.List(ctx, "empty")
	wantRows(t, "empty", e, err)
	if _, err := b.List(ctx, "nothere"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a folder that is not there: %v, want fs.ErrNotExist", err)
	}
	st, err := b.Stat(ctx, "docs/sub/c.bin")
	if err != nil || st.Size != 3 || st.IsDir || !st.ModTime.Equal(webTime) || st.RelPath != "docs/sub/c.bin" {
		t.Errorf("Stat of a file = %+v, %v", st, err)
	}
	if st, err := b.Stat(ctx, "docs"); err != nil || !st.IsDir {
		t.Errorf("Stat of a folder = %+v, %v", st, err)
	}
	if _, err := b.Stat(ctx, "nothere.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat of nothing: %v, want fs.ErrNotExist", err)
	}
}

// A recursive listing gets each folder's page once and each file's HEAD
// once; the scan's listings then come from the cache.
func TestHTTPPreloadListsEachFolderOnce(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	web.taken()
	ctx := context.Background()
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for dir, want := range map[string][]string{
		"":         {"a.txt 5", "my file.bin 10", "docs/", "empty/"},
		"docs":     {"docs/b.txt 2", "docs/sub/"},
		"docs/sub": {"docs/sub/c.bin 3", "docs/sub/d e.txt 0"},
		"empty":    nil,
	} {
		e, err := b.List(ctx, dir)
		wantRows(t, dir, e, err, want...)
	}
	reqs := web.taken()
	if gets, heads := countPrefix(reqs, "GET "), countPrefix(reqs, "HEAD "); gets != 4 || heads != 5 {
		t.Errorf("%d GETs and %d HEADs, want 4 pages and 5 files: %q", gets, heads, reqs)
	}
}

// A link on the server back up to a folder above (loop -> ., up -> ..) is
// a folder whose listing is that folder's: it is listed as a link to it,
// and a recursive listing ends rather than go round for ever. Below each
// link two pages are read: its own, and the one that shows it leads back
// up.
func TestHTTPLinkLoop(t *testing.T) {
	root := webTree(t, webFiles)
	if err := os.Symlink(".", filepath.Join(root, "loop")); err != nil {
		t.Skip("no symbolic links here:", err)
	}
	if err := os.Symlink("..", filepath.Join(root, "docs", "up")); err != nil {
		t.Fatal(err)
	}
	web, url := newFakeWeb(t, root)
	b := openWeb(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b.PreloadRecursive(ctx, "")
	e, err := b.List(ctx, "")
	wantRows(t, "root", e, err, "a.txt 5", "my file.bin 10", "docs/", "empty/", "loop -> .")
	e, err = b.List(ctx, "docs")
	wantRows(t, "docs", e, err, "docs/b.txt 2", "docs/sub/", "docs/up -> ..")
	waitFor(t, func() bool { _, _, _, done := b.listCache.lookup(""); return done })
	var below []string
	for _, r := range web.taken() {
		if strings.Contains(r, "/loop/") || strings.Contains(r, "/up/") {
			below = append(below, r)
		}
	}
	slices.Sort(below)
	if want := []string{"GET /docs/up/", "GET /docs/up/docs/", "GET /loop/", "GET /loop/loop/"}; !slices.Equal(below, want) {
		t.Errorf("below the links: %q, want %q", below, want)
	}
}

// Folders that each hold one folder of one name and nothing else, x/x/x,
// have equal pages, as a link back up has: they are folders all the same.
func TestHTTPNestedNamesAreNoLoop(t *testing.T) {
	_, url := newFakeWeb(t, webTree(t, map[string]string{"x/x/x/": "", "a.txt": "a"}))
	b := openWeb(t, url)
	ctx := context.Background()
	b.PreloadRecursive(ctx, "")
	for dir, want := range map[string][]string{"": {"a.txt 1", "x/"}, "x": {"x/x/"}, "x/x": {"x/x/x/"}, "x/x/x": nil} {
		e, err := b.List(ctx, dir)
		wantRows(t, dir, e, err, want...)
	}
}

// A scan lists the folder it opens, then has the tree under it listed
// recursively: that listing takes the folder's listing as it was made, and
// the pages that listing read. Each page and each HEAD is sent once.
func TestHTTPScanListsEachFolderOnce(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	web.taken()
	ctx := context.Background()
	e, err := b.List(ctx, "")
	wantRows(t, "root", e, err, "a.txt 5", "my file.bin 10", "docs/", "empty/")
	b.PreloadRecursive(ctx, "")
	for _, dir := range []string{"docs", "docs/sub", "empty"} {
		if _, err := b.List(ctx, dir); err != nil {
			t.Fatal(err)
		}
	}
	reqs := web.taken()
	if gets, heads := countPrefix(reqs, "GET "), countPrefix(reqs, "HEAD "); gets != 4 || heads != 5 {
		t.Errorf("%d GETs and %d HEADs, want 4 pages and 5 files: %q", gets, heads, reqs)
	}
}

// The listing of a folder keeps the pages of its folders for their own
// listings. A rescan shows what the server holds now: its fresh listing
// takes none of them.
func TestHTTPRescanReadsThePageAgain(t *testing.T) {
	root := webTree(t, webFiles)
	web, url := newFakeWeb(t, root)
	b := openWeb(t, url)
	ctx := context.Background()
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	web.taken()
	e, err := b.List(ctx, "docs")
	wantRows(t, "docs", e, err, "docs/b.txt 2", "docs/sub/")
	if n := countExact(web.taken(), "GET /docs/"); n != 0 {
		t.Errorf("the page of docs read %d more times, want the one the root's listing read", n)
	}

	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "new.txt"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err = b.List(model.WithFreshListing(ctx), "docs")
	wantRows(t, "docs, fresh", e, err, "docs/b.txt 2", "docs/new.txt 3", "docs/sub/")
}

func countExact(reqs []string, req string) int {
	n := 0
	for _, r := range reqs {
		if r == req {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A server that refuses HEAD gives sizes and times to a GET of one byte,
// and is sent no HEAD after it.
func TestHTTPHeadRefused(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	web.set("HEAD *", http.StatusMethodNotAllowed)
	e, err := b.List(context.Background(), "")
	wantRows(t, "root", e, err, "a.txt 5", "my file.bin 10", "docs/", "empty/")
	web.taken() // the HEADs in flight when the first 405 came
	e, err = b.List(context.Background(), "docs/sub")
	wantRows(t, "docs/sub", e, err, "docs/sub/c.bin 3", "docs/sub/d e.txt 0")
	for _, f := range e {
		if !f.ModTime.Equal(webTime) {
			t.Errorf("%s: %v, want %v", f.RelPath, f.ModTime, webTime)
		}
	}
	if heads := countPrefix(web.taken(), "HEAD "); heads != 0 {
		t.Errorf("%d HEADs after the first 405, want none", heads)
	}
}

// A file the page links that is not there is left out; a file the server
// will not show fails the folder's listing, final: it is there, and a
// mirror from a listing without it would delete it on the other side.
func TestHTTPLinkedFileGoneOrForbidden(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	web.page("/docs/", `<pre><a href="b.txt">b.txt</a>
<a href="ghost.txt">ghost.txt</a>
</pre>`)
	e, err := b.List(context.Background(), "docs")
	wantRows(t, "docs", e, err, "docs/b.txt 2")
	web.set("HEAD /docs/b.txt", http.StatusForbidden)
	if _, err := b.List(context.Background(), "docs"); !errors.Is(err, fs.ErrPermission) || !isPermanentError(err) {
		t.Errorf("a file the server will not show: %v, want a final fs.ErrPermission", err)
	}
}

// A page that links a folder without its slash: the HEAD's redirect to the
// slash says it is a folder.
func TestHTTPFolderLinkWithoutSlash(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	web.page("/", `<ul><li><a href="docs">docs</a></li><li><a href="a.txt">a.txt</a></li></ul>`)
	e, err := b.List(context.Background(), "")
	wantRows(t, "root", e, err, "docs/", "a.txt 5")
}

// sc writes nothing to a web server: every write fails at once, final,
// stops a copy, and sends no request.
func TestHTTPIsReadOnly(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	web.taken()
	ctx := context.Background()
	for what, err := range map[string]error{
		"CopyFrom":  b.CopyFrom(ctx, "x.txt", strings.NewReader("x"), model.PutParams{Size: 1}),
		"Mkdir":     b.Mkdir(ctx, "newdir", 0o755),
		"Remove":    b.Remove(ctx, "a.txt"),
		"RemoveAll": b.RemoveAll(ctx, "docs"),
		"Rename":    b.Rename(ctx, "a.txt", "b.txt"),
		"SetTimes":  b.SetTimes(ctx, "a.txt", time.Now(), time.Time{}, time.Time{}),
	} {
		if !errors.Is(err, model.ErrReadOnly) || !GivesUp(err) || !isPermanentError(err) {
			t.Errorf("%s: %v, want a final model.ErrReadOnly that stops a copy", what, err)
		}
	}
	if reqs := web.taken(); len(reqs) != 0 {
		t.Errorf("writes sent %q", reqs)
	}
	if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) != 0 {
		t.Errorf("ChecksumAlgos = %v, %v; want none", algos, err)
	}
	if _, err := b.Checksum(ctx, "a.txt", "md5"); !errors.Is(err, ErrUnsupported) {
		t.Errorf("Checksum: %v, want a final none", err)
	}
}

func TestHTTPOpen(t *testing.T) {
	_, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	ctx := context.Background()
	for _, tc := range []struct {
		rel  string
		off  int64
		want string
	}{{"my file.bin", 0, "0123456789"}, {"my file.bin", 4, "456789"}, {"docs/sub/c.bin", 1, "cc"}} {
		rc, err := b.OpenAt(ctx, tc.rel, tc.off)
		if err != nil {
			t.Fatalf("%s @%d: %v", tc.rel, tc.off, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || string(got) != tc.want {
			t.Errorf("%s @%d = %q, %v; want %q", tc.rel, tc.off, got, err, tc.want)
		}
	}
	if _, err := b.Open(ctx, "nothere"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open of nothing: %v, want fs.ErrNotExist", err)
	}
}

// jsonWeb answers a folder's listing as nginx does with autoindex_format
// json, or as Caddy does, when the request accepts JSON.
type jsonWeb struct {
	*fakeWeb
	caddy bool
	other bool // a JSON that is no listing
}

func (j *jsonWeb) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/") || !strings.Contains(r.Header.Get("Accept"), "application/json") {
		j.fakeWeb.ServeHTTP(w, r)
		return
	}
	j.mu.Lock()
	j.reqs = append(j.reqs, r.Method+" "+r.URL.Path)
	j.accepts = append(j.accepts, r.Header.Get("Accept"))
	j.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if j.other {
		io.WriteString(w, `{"files":[]}`)
		return
	}
	mtime := webTime.Format(http.TimeFormat)
	switch r.URL.Path {
	case "/":
		if j.caddy {
			fmt.Fprintf(w, `[{"name":"docs/","size":4096,"url":"./docs/","mod_time":%q,"is_dir":true,"is_symlink":false},`+
				`{"name":"a.txt","size":5,"url":"./a.txt","mod_time":%q,"is_dir":false,"is_symlink":false}]`,
				webTime.Format(time.RFC3339Nano), webTime.Format(time.RFC3339Nano))
			return
		}
		fmt.Fprintf(w, "[\n{ \"name\":\"docs\", \"type\":\"directory\", \"mtime\":%q },\n{ \"name\":\"a.txt\", \"type\":\"file\", \"mtime\":%q, \"size\":5 }\n]\n", mtime, mtime)
	default:
		io.WriteString(w, "[\n]\n")
	}
}

// A JSON listing gives exact sizes and times: no HEAD is sent.
func TestHTTPJSONListingSendsNoHead(t *testing.T) {
	for _, caddy := range []bool{false, true} {
		web, _ := newFakeWeb(t, webTree(t, webFiles))
		j := &jsonWeb{fakeWeb: web, caddy: caddy}
		srv := httptest.NewServer(j)
		t.Cleanup(srv.Close)
		b := openWeb(t, srv.URL)
		web.taken()
		e, err := b.List(context.Background(), "")
		wantRows(t, fmt.Sprintf("caddy=%v", caddy), e, err, "a.txt 5", "docs/")
		for _, f := range e {
			if !f.IsDir && !f.ModTime.Equal(webTime) {
				t.Errorf("caddy=%v: %s: %v, want %v", caddy, f.RelPath, f.ModTime, webTime)
			}
		}
		if heads := countPrefix(web.taken(), "HEAD "); heads != 0 {
			t.Errorf("caddy=%v: %d HEADs, want none", caddy, heads)
		}
	}
}

// A server whose JSON is no listing sc reads is asked for its pages, then
// and from then on.
func TestHTTPOtherJSONAsksForPages(t *testing.T) {
	web, _ := newFakeWeb(t, webTree(t, webFiles))
	j := &jsonWeb{fakeWeb: web, other: true}
	srv := httptest.NewServer(j)
	t.Cleanup(srv.Close)
	b := openWeb(t, srv.URL)
	ctx := context.Background()
	e, err := b.List(ctx, "")
	wantRows(t, "root", e, err, "a.txt 5", "my file.bin 10", "docs/", "empty/")
	e, err = b.List(ctx, "docs")
	wantRows(t, "docs", e, err, "docs/b.txt 2", "docs/sub/")
	web.mu.Lock()
	accepts := web.accepts
	web.mu.Unlock()
	if len(accepts) < 2 || accepts[0] != acceptListing || slices.ContainsFunc(accepts[1:], func(a string) bool { return a != acceptHTML }) {
		t.Errorf("Accept of each page: %q, want JSON asked for first, then HTML alone", accepts)
	}
}

// The open checks the base: a path that is not there, a login refused, a
// file, or a redirect elsewhere fail it.
func TestHTTPOpenChecksTheBase(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	ctx := context.Background()
	if _, err := NewHTTPBackend(ctx, url+"/nothere/", false, 1); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a path that is not there: %v, want fs.ErrNotExist", err)
	}
	if _, err := NewHTTPBackend(ctx, url+"/a.txt", false, 1); err == nil {
		t.Error("a file opened as a folder")
	}
	web.set("HEAD /docs/", http.StatusUnauthorized)
	if _, err := NewHTTPBackend(ctx, url+"/docs", false, 1); !errors.Is(err, model.ErrAuth) {
		t.Errorf("a login refused: %v, want model.ErrAuth", err)
	}
	web.set("HEAD /docs/", http.StatusMovedPermanently)
	if _, err := NewHTTPBackend(ctx, url+"/docs", false, 1); err == nil {
		t.Error("a redirect elsewhere was followed")
	}
}

// A URL as a browser shows it: its path escaped, with a query or a
// fragment, which are left out.
func TestHTTPURLAsABrowserShowsIt(t *testing.T) {
	_, url := newFakeWeb(t, webTree(t, map[string]string{"my files/x.txt": "x", "100%/y.txt": "yy"}))
	b := openWeb(t, url+"/my%20files/?C=M;O=A#top")
	e, err := b.List(context.Background(), "")
	wantRows(t, "my files", e, err, "x.txt 1")
	if got := b.BasePath(); !strings.HasSuffix(got, "/my files") {
		t.Errorf("BasePath = %q", got)
	}
	b = openWeb(t, url+"/100%25/")
	e, err = b.List(context.Background(), "")
	wantRows(t, "100%", e, err, "y.txt 2")
}

// A page of no stated length that its connection cut short could look
// whole: without its end tag it is an error another try can mend.
func TestHTTPCutPageIsNoListing(t *testing.T) {
	defer func(n int) { httpListTries = n }(httpListTries)
	httpListTries = 0
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	var cut sync.Mutex
	cutting := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cut.Lock()
		on := cutting
		cut.Unlock()
		if !on || r.Method != http.MethodGet || r.URL.Path != "/docs/" {
			web.ServeHTTP(w, r)
			return
		}
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n<pre>\n<a href=\"b.txt\">b.txt</a>\n")
		buf.Flush()
	}))
	t.Cleanup(srv.Close)
	b = openWeb(t, srv.URL)
	_, err := b.List(context.Background(), "docs")
	if !errors.Is(err, io.ErrUnexpectedEOF) || isPermanentError(err) {
		t.Errorf("a page cut short: %v, want an error another try can mend", err)
	}
	cut.Lock()
	cutting = false
	cut.Unlock()
	e, err := b.List(context.Background(), "docs")
	wantRows(t, "docs", e, err, "docs/b.txt 2", "docs/sub/")
}

// One HEAD that the server fails once is sent again, not the whole folder.
func TestHTTPRequestTriedAgain(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b := openWeb(t, url)
	flaky := &flakyOnce{h: web, path: "/docs/b.txt"}
	srv := httptest.NewServer(flaky)
	t.Cleanup(srv.Close)
	b = openWeb(t, srv.URL)
	web.taken()
	e, err := b.List(context.Background(), "docs")
	wantRows(t, "docs", e, err, "docs/b.txt 2", "docs/sub/")
	reqs := web.taken()
	if gets := countExact(reqs, "GET /docs/"); gets != 1 {
		t.Errorf("the page asked for %d times, want once: %q", gets, reqs)
	}
}

// flakyOnce answers the first request of path with a 503.
type flakyOnce struct {
	h    http.Handler
	path string
	mu   sync.Mutex
	done bool
}

func (f *flakyOnce) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	fail := !f.done && r.URL.Path == f.path
	f.done = f.done || fail
	f.mu.Unlock()
	if fail {
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	f.h.ServeHTTP(w, r)
}

// http:// opens through the reconnecting wrapper, which keeps the
// backend's own idle deadline.
func TestHTTPOpensLazily(t *testing.T) {
	web, url := newFakeWeb(t, webTree(t, webFiles))
	b, err := TryOpenBackend(url+"/docs/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if len(web.taken()) != 0 {
		t.Error("the open connected before the first call")
	}
	if lm, ok := b.(model.LivenessManaged); !ok || !lm.ManagesLiveness() {
		t.Error("http:// leaves its liveness to the scanner's cap")
	}
	e, err := b.List(context.Background(), "")
	wantRows(t, "docs", e, err, "b.txt 2", "sub/")
}
