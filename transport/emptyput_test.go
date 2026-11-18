package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"sc/model"
)

// lengthWanted answers a PUT or POST whose length it is not told with 411,
// as servers that want a length do, when strict; it hands every other
// request to next and keeps the lengths it was told.
type lengthWanted struct {
	next    http.Handler
	strict  bool
	mu      sync.Mutex
	lengths map[string]int64
}

func (l *lengthWanted) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "PUT" || r.Method == "POST" {
		l.mu.Lock()
		l.lengths[strings.Trim(r.URL.Path, "/")] = r.ContentLength
		strict := l.strict
		l.mu.Unlock()
		if strict && r.ContentLength < 0 {
			http.Error(w, "length wanted", http.StatusLengthRequired)
			return
		}
	}
	l.next.ServeHTTP(w, r)
}

// length is the length the request for p said, and whether one came.
func (l *lengthWanted) length(p string) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, ok := l.lengths[p]
	return n, ok
}

// onlyReader hides what src is: http.NewRequest knows the length of a
// strings.Reader, never of a file or a stream from another backend.
type onlyReader struct{ io.Reader }

// W-M6: an empty file, its size given as 0 (not known), went chunked: Go
// takes a body of length 0 that is not http.NoBody for one of a length not
// known. A server that wants a length answered 411. Now an empty body goes
// with Content-Length: 0, and one not empty of a size not known goes whole.
func TestEmptyFileGoesWithItsLength(t *testing.T) {
	ctx := context.Background()
	s := newWDTree()
	lw := &lengthWanted{next: s, strict: true, lengths: map[string]int64{}}
	srv := httptest.NewServer(lw)
	t.Cleanup(srv.Close)
	dav, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := dav.CopyFrom(ctx, "empty", onlyReader{strings.NewReader("")}, model.PutParams{}); err != nil {
		t.Fatalf("webdav: an empty file: %v", err)
	}
	if n, ok := lw.length("empty"); !ok || n != 0 {
		t.Errorf("webdav: an empty file went with length %d (sent: %v), want 0", n, ok)
	}
	s.mu.Lock()
	_, there := s.files["empty"]
	s.mu.Unlock()
	if !there {
		t.Error("webdav: the empty file is not there")
	}
	if err := dav.CopyFrom(ctx, "abc", onlyReader{strings.NewReader("abc")}, model.PutParams{Size: 3}); err != nil {
		t.Fatalf("webdav: a file of a size known: %v", err)
	}
	if n, _ := lw.length("abc"); n != 3 {
		t.Errorf("webdav: a file of 3 bytes went with length %d", n)
	}
	lw.mu.Lock()
	lw.strict = false
	lw.mu.Unlock()
	if err := dav.CopyFrom(ctx, "hello", onlyReader{strings.NewReader("hello")}, model.PutParams{}); err != nil {
		t.Fatalf("webdav: a file of a size not known: %v", err)
	}
	if got := s.holds("hello"); got != "hello" {
		t.Errorf("webdav: a file of a size not known holds %q, want hello", got)
	}

	f := &fakeRestic{objects: map[string]string{"config": "cfg"}, lists: map[string]int{}, fail: map[string]int{}}
	rl := &lengthWanted{next: f, strict: true, lengths: map[string]int64{}}
	rsrv := httptest.NewServer(rl)
	t.Cleanup(rsrv.Close)
	rst, err := NewResticBackend(ctx, "restic://u:p@"+rsrv.Listener.Addr().String()+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rst.Close() })
	if err := rst.CopyFrom(ctx, "locks/"+keyB, onlyReader{strings.NewReader("")}, model.PutParams{}); err != nil {
		t.Fatalf("restic: an empty object: %v", err)
	}
	if n, ok := rl.length("locks/" + keyB); !ok || n != 0 {
		t.Errorf("restic: an empty object went with length %d (sent: %v), want 0", n, ok)
	}
	if err := rst.CopyFrom(ctx, "keys/"+keyB, onlyReader{strings.NewReader("abc")}, model.PutParams{Size: 3}); err != nil {
		t.Fatalf("restic: an object of a size known: %v", err)
	}
	if n, _ := rl.length("keys/" + keyB); n != 3 {
		t.Errorf("restic: an object of 3 bytes went with length %d", n)
	}
}
