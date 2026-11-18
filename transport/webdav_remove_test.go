package transport

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// W-L3: a DELETE takes a collection with all it holds (RFC 4918), and sc
// removes with Remove what it saw as a file. A file that is a folder on
// the server now went with its whole subtree, where every other backend
// refuses. Now a folder that holds anything is refused, final, and stays;
// an empty one goes, as rmdir takes it; a file goes; one gone already is
// no failure, and the listing forgets it; a look that fails sends no
// DELETE.
func TestWebDAVRemoveTakesNoFolderThatHoldsAnything(t *testing.T) {
	ctx := context.Background()
	s := newWDTree("x", "x/sub", "e", "y")
	s.files["x/sub/f"] = "keep"
	s.files["y/f"] = "keep"
	s.files["f"] = "go"
	s.files["g"] = "stay"
	s.files["gone"] = "soon"
	var deletes atomic.Int32
	var mu sync.Mutex
	failPath, failDepth := "", "" // a PROPFIND that answers 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fail := r.Method == "PROPFIND" && strings.Trim(r.URL.Path, "/") == failPath && r.Header.Get("Depth") == failDepth
		mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method == "DELETE" {
			deletes.Add(1)
		}
		s.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	failing := func(path, depth string) {
		mu.Lock()
		failPath, failDepth = path, depth
		mu.Unlock()
		deletes.Store(0)
	}

	err = b.Remove(ctx, "x")
	if !errors.Is(err, fs.ErrExist) {
		t.Errorf("Remove of a folder that holds a folder: %v; want it refused (fs.ErrExist)", err)
	}
	if got := s.holds("x/sub/f"); got != "keep" || deletes.Load() != 0 {
		t.Errorf("Remove of a folder: %d DELETEs; x/sub/f holds %q, want keep", deletes.Load(), got)
	}
	if err := b.Remove(ctx, "e"); err != nil {
		t.Errorf("Remove of an empty folder: %v", err)
	}
	s.mu.Lock()
	e := s.folders["e"]
	s.mu.Unlock()
	if e {
		t.Error("Remove of an empty folder left it there")
	}
	if err := b.Remove(ctx, "f"); err != nil || s.holds("f") != "" {
		t.Errorf("Remove of a file: %v; it holds %q", err, s.holds("f"))
	}

	failing("g", "0")
	if err := b.Remove(ctx, "g"); err == nil || deletes.Load() != 0 || s.holds("g") != "stay" {
		t.Errorf("Remove when the look fails: %v, %d DELETEs, g holds %q; want a failure and no DELETE", err, deletes.Load(), s.holds("g"))
	}
	failing("y", "1")
	if err := b.Remove(ctx, "y"); err == nil || deletes.Load() != 0 || s.holds("y/f") != "keep" {
		t.Errorf("Remove when the folder's listing fails: %v, %d DELETEs, y/f holds %q; want a failure and no DELETE", err, deletes.Load(), s.holds("y/f"))
	}
	failing("", "")

	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	delete(s.files, "gone") // another client removes it
	s.mu.Unlock()
	if err := b.Remove(ctx, "gone"); err != nil {
		t.Errorf("Remove of a file gone already: %v; want no failure", err)
	}
	entries, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "gone" {
			t.Error("the listing still shows a file Remove found gone")
		}
	}
}
