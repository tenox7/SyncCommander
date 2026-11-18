package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"sc/model"
)

// wdTree is a WebDAV server holding folders and files as RFC 4918 has
// them: a MKCOL of a folder that is there answers 405, of one whose parent
// is not 409; a PUT into a folder that is not there answers 409. bad405
// makes a MKCOL of a folder answer 405 and make nothing, so many times, as
// x/net/webdav answers any Mkdir that failed; mkcolFail makes it answer
// that code. conflict makes a PUT of a file answer 409 anyway. finite refuses Depth: infinity, so each folder
// is listed on its own.
type wdTree struct {
	mu        sync.Mutex
	folders   map[string]bool
	files     map[string]string
	bad405    map[string]int
	mkcolFail map[string]int
	mkcols    map[string]int
	puts      map[string]int
	conflict  map[string]bool
	finite    bool
}

func newWDTree(folders ...string) *wdTree {
	s := &wdTree{folders: map[string]bool{"": true}, files: map[string]string{}, bad405: map[string]int{}, mkcolFail: map[string]int{}, mkcols: map[string]int{}, puts: map[string]int{}, conflict: map[string]bool{}}
	for _, f := range folders {
		s.folders[f] = true
	}
	return s
}

func (s *wdTree) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.Trim(r.URL.Path, "/")
	_, isFile := s.files[p]
	switch r.Method {
	case "PROPFIND":
		s.propfind(w, p, r.Header.Get("Depth"))
	case "MKCOL":
		s.mkcols[p]++
		switch {
		case s.bad405[p] > 0:
			s.bad405[p]--
			w.WriteHeader(http.StatusMethodNotAllowed)
		case s.mkcolFail[p] != 0:
			w.WriteHeader(s.mkcolFail[p])
		case s.folders[p] || isFile:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case !s.folders[parentDir(p)]:
			w.WriteHeader(http.StatusConflict)
		default:
			s.folders[p] = true
			w.WriteHeader(http.StatusCreated)
		}
	case "PUT":
		s.puts[p]++
		if s.conflict[p] || !s.folders[parentDir(p)] {
			w.WriteHeader(http.StatusConflict)
			return
		}
		s.files[p] = string(body)
		w.WriteHeader(http.StatusCreated)
	case "DELETE":
		s.drop(p)
		w.WriteHeader(http.StatusNoContent)
	case "MOVE":
		u, err := url.Parse(r.Header.Get("Destination"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		dst := strings.Trim(u.Path, "/")
		if _, taken := s.files[dst]; taken || s.folders[dst] {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		moved := func(q string) (string, bool) {
			if q == p {
				return dst, true
			}
			rest, ok := strings.CutPrefix(q, p+"/")
			return dst + "/" + rest, ok
		}
		for q := range s.folders {
			if to, ok := moved(q); ok {
				delete(s.folders, q)
				s.folders[to] = true
			}
		}
		for q, v := range s.files {
			if to, ok := moved(q); ok {
				delete(s.files, q)
				s.files[to] = v
			}
		}
		w.WriteHeader(http.StatusCreated)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// drop takes p and all under it away, as a DELETE does, or a hand behind
// sc's back. s.mu is held.
func (s *wdTree) drop(p string) {
	for q := range s.folders {
		if q == p || strings.HasPrefix(q, p+"/") {
			delete(s.folders, q)
		}
	}
	for q := range s.files {
		if q == p || strings.HasPrefix(q, p+"/") {
			delete(s.files, q)
		}
	}
}

func (s *wdTree) propfind(w http.ResponseWriter, p, depth string) {
	_, isFile := s.files[p]
	switch {
	case !s.folders[p] && !isFile:
		w.WriteHeader(http.StatusNotFound)
		return
	case depth == "infinity" && s.finite:
		w.WriteHeader(http.StatusForbidden)
		return
	}
	var out strings.Builder
	out.WriteString(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">`)
	add := func(q string, dir bool) {
		href, prop := "/"+q, fmt.Sprintf("<d:resourcetype/><d:getcontentlength>%d</d:getcontentlength>", len(s.files[q]))
		if dir {
			href, prop = strings.TrimSuffix("/"+q, "/")+"/", "<d:resourcetype><d:collection/></d:resourcetype>"
		}
		fmt.Fprintf(&out, `<d:response><d:href>%s</d:href><d:propstat><d:prop>%s</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`, href, prop)
	}
	add(p, !isFile)
	under := func(q string) bool {
		if q == p || isFile || depth == "0" {
			return false
		}
		rest := q
		if p != "" {
			var ok bool
			if rest, ok = strings.CutPrefix(q, p+"/"); !ok {
				return false
			}
		}
		return depth == "infinity" || !strings.Contains(rest, "/")
	}
	for q := range s.folders {
		if under(q) {
			add(q, true)
		}
	}
	for q := range s.files {
		if under(q) {
			add(q, false)
		}
	}
	out.WriteString(`</d:multistatus>`)
	w.WriteHeader(http.StatusMultiStatus)
	w.Write([]byte(out.String()))
}

// sent gives the MKCOLs of dir and the PUTs of file since the last call,
// and forgets them.
func (s *wdTree) sent(dir, file string) (mkcols, puts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mkcols, puts = s.mkcols[dir], s.puts[file]
	s.mkcols, s.puts = map[string]int{}, map[string]int{}
	return mkcols, puts
}

func (s *wdTree) holds(file string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.files[file]
}

func newWDTreeBackend(t *testing.T, s *wdTree) *WebDAVBackend {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(context.Background(), "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// wdPut copies body to relPath under the retry, as a copy does: the source
// is read again for each try.
func wdPut(ctx context.Context, b *WebDAVBackend, relPath, body string) error {
	return Retry(ctx, "webdav", "COPY "+relPath, func(ctx context.Context) error {
		return b.CopyFrom(ctx, relPath, strings.NewReader(body), model.PutParams{Size: int64(len(body))})
	})
}

// W-M5: a MKCOL answered 405, as x/net/webdav answers any Mkdir that
// failed, and sc took the folder for there for the session: every file
// into it failed with 409, and no MKCOL was sent again. Now a PUT that
// finds its folder not there makes it again, and the copy is tried again.
func TestWebDAVPutMakesAFolderTakenForThereAgain(t *testing.T) {
	ctx := context.Background()
	defer SetMaxRetries(MaxRetries())
	SetMaxRetries(2)
	s := newWDTree()
	s.bad405["d"] = 1
	b := newWDTreeBackend(t, s)
	if err := wdPut(ctx, b, "d/a", "A"); err != nil {
		t.Fatalf("d/a: %v", err)
	}
	if mk, pa := s.sent("d", "d/a"); mk != 2 || pa != 2 {
		t.Errorf("d/a: MKCOL d %d times, PUT %d times; want 2 and 2", mk, pa)
	}
	if err := wdPut(ctx, b, "d/b", "B"); err != nil {
		t.Fatalf("d/b: %v", err)
	}
	if mk, pb := s.sent("d", "d/b"); mk != 0 || pb != 1 {
		t.Errorf("d/b: MKCOL d %d times, PUT %d times; want 0 and 1", mk, pb)
	}
	for f, want := range map[string]string{"d/a": "A", "d/b": "B"} {
		if got := s.holds(f); got != want {
			t.Errorf("%s holds %q, want %q", f, got, want)
		}
	}
}

// W-M5: folders sc made, and then deleted by another hand: the memo kept
// them, and every file into them failed. Now they are made again, the
// folder above too.
func TestWebDAVPutMakesFoldersDeletedBehindItsBackAgain(t *testing.T) {
	ctx := context.Background()
	defer SetMaxRetries(MaxRetries())
	SetMaxRetries(2)
	s := newWDTree()
	b := newWDTreeBackend(t, s)
	if err := wdPut(ctx, b, "d/e/a", "A"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.drop("d")
	s.mu.Unlock()
	if err := wdPut(ctx, b, "d/e/b", "B"); err != nil {
		t.Fatalf("d/e/b after d was deleted: %v", err)
	}
	if got := s.holds("d/e/b"); got != "B" {
		t.Errorf("d/e/b holds %q, want B", got)
	}
}

// W-M5: a server that answers 405 to every MKCOL of a folder and makes
// none: the PUT's 409 stands, after one MKCOL more and no PUT more. sc
// never tries again and again.
func TestWebDAVPutIntoAFolderTheServerCannotMake(t *testing.T) {
	ctx := context.Background()
	defer SetMaxRetries(MaxRetries())
	SetMaxRetries(2)
	s := newWDTree()
	s.bad405["d"] = 1000
	b := newWDTreeBackend(t, s)
	err := wdPut(ctx, b, "d/a", "A")
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("a PUT into a folder that cannot be made: %v, want its 409", err)
	}
	if mk, pa := s.sent("d", "d/a"); mk != 2 || pa != 1 {
		t.Errorf("MKCOL d %d times, PUT %d times; want 2 and 1", mk, pa)
	}
	// A 409 for a file in the base: no folder to make, the answer stands.
	s.mu.Lock()
	s.conflict["top"] = true
	s.mu.Unlock()
	if err := wdPut(ctx, b, "top", "T"); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("a PUT into the base answered 409: %v, want its 409", err)
	}
	if _, pt := s.sent("", "top"); pt != 1 {
		t.Errorf("a PUT into the base answered 409 was sent %d times, want 1", pt)
	}
}

// W-M5: a folder that is to be made again and that the server refuses:
// the copy fails, with the server's reason for the folder, read by its
// code.
func TestWebDAVPutWhoseFolderCannotBeMadeAgain(t *testing.T) {
	ctx := context.Background()
	defer SetMaxRetries(MaxRetries())
	SetMaxRetries(2)
	s := newWDTree()
	b := newWDTreeBackend(t, s)
	if err := wdPut(ctx, b, "d/a", "A"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.drop("d")
	s.mkcolFail["d"] = http.StatusInsufficientStorage
	s.mu.Unlock()
	s.sent("", "")
	if err := wdPut(ctx, b, "d/b", "B"); !errors.Is(err, model.ErrNoSpace) {
		t.Fatalf("a PUT whose folder cannot be made again: %v, want the MKCOL's 507", err)
	}
	if _, pb := s.sent("", "d/b"); pb != 1 {
		t.Errorf("PUT d/b %d times, want 1", pb)
	}
}

// W-M5: the memo of folders that are there knew only the folders sc made:
// each folder a listing showed cost a MKCOL, and any delete or rename
// emptied it. Now listings fill it, deep or one level at a time, and a
// delete or a rename forgets only its own folders.
func TestWebDAVFolderMemoFromListings(t *testing.T) {
	for _, finite := range []bool{false, true} {
		t.Run(fmt.Sprintf("one level at a time=%v", finite), func(t *testing.T) {
			ctx := context.Background()
			s := newWDTree("d", "d/e", "f", "f/k", "g")
			s.files["d/x"] = "X"
			s.finite = finite
			b := newWDTreeBackend(t, s)
			if finite {
				for _, dir := range []string{"", "d", "f"} {
					if _, err := b.List(ctx, dir); err != nil {
						t.Fatal(err)
					}
				}
			} else if err := b.runRecursiveList(ctx, "", func(string, []model.FileEntry) {}, func(string) {}); err != nil {
				t.Fatal(err)
			}
			s.sent("", "")
			if err := wdPut(ctx, b, "d/e/y", "Y"); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			n := len(s.mkcols)
			s.mu.Unlock()
			if n != 0 {
				t.Errorf("a PUT into folders a listing showed sent MKCOLs: %v", s.mkcols)
			}
			if err := b.Remove(ctx, "d/x"); err != nil {
				t.Fatal(err)
			}
			if err := b.Rename(ctx, "g", "h"); err != nil {
				t.Fatal(err)
			}
			if err := wdPut(ctx, b, "d/z", "Z"); err != nil {
				t.Fatal(err)
			}
			if mk, _ := s.sent("d", ""); mk != 0 {
				t.Errorf("after a delete and a rename elsewhere, MKCOL d %d times; want 0", mk)
			}
			if err := b.RemoveAll(ctx, "f"); err != nil {
				t.Fatal(err)
			}
			if err := wdPut(ctx, b, "f/k/z", "Z"); err != nil {
				t.Fatal(err)
			}
			if mk, pz := s.sent("f", "f/k/z"); mk != 1 || pz != 1 {
				t.Errorf("into a folder deleted with its parent: MKCOL f %d times, PUT %d times; want 1 and 1", mk, pz)
			}
		})
	}
}
