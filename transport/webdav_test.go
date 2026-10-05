package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"
)

const wdCollectionBody = `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response><d:href>/</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>`

// wdDeleteServer answers PROPFIND for the base, and for every other path
// either finds it (keep) or not, so a delete can be checked afterwards.
// DELETE answers status with body and records the Depth header it saw.
type wdDeleteServer struct {
	status  int
	body    string
	keep    bool
	mu      sync.Mutex
	depths  []string
	deletes int
}

func (s *wdDeleteServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "PROPFIND":
		if r.URL.Path == "/" || s.keep {
			w.WriteHeader(http.StatusMultiStatus)
			w.Write([]byte(wdCollectionBody))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case "DELETE":
		s.mu.Lock()
		s.depths = append(s.depths, r.Header.Get("Depth"))
		s.deletes++
		s.mu.Unlock()
		w.WriteHeader(s.status)
		w.Write([]byte(s.body))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func newWDDeleteBackend(t *testing.T, s *wdDeleteServer) *WebDAVBackend {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend("webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A collection delete is Depth: infinity by protocol; a plain file delete
// carries no Depth. The recursive form is checked afterwards and passes when
// the path is gone.
func TestWebDAVDeleteDepthAndVerify(t *testing.T) {
	s := &wdDeleteServer{status: http.StatusNoContent}
	b := newWDDeleteBackend(t, s)
	ctx := context.Background()
	if err := b.RemoveAll(ctx, "dir"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := b.Remove(ctx, "f"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := strings.Join(s.depths, ","); got != "infinity," {
		t.Errorf("Depth headers = %q, want infinity for the collection and none for the file", got)
	}
}

// 207 is a 2xx class answer that means some member survived: it must fail,
// naming the member.
func TestWebDAVDeleteMultiStatusIsFailure(t *testing.T) {
	s := &wdDeleteServer{status: http.StatusMultiStatus, body: `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response><d:href>/dir/locked.txt</d:href><d:status>HTTP/1.1 423 Locked</d:status></d:response></d:multistatus>`}
	b := newWDDeleteBackend(t, s)
	err := b.RemoveAll(context.Background(), "dir")
	if err == nil || !strings.Contains(err.Error(), "locked.txt") || !strings.Contains(err.Error(), "423") {
		t.Fatalf("RemoveAll = %v, want the surviving member reported", err)
	}
}

// A server that reports success but still lists the collection afterwards
// did not delete it.
func TestWebDAVRemoveAllDetectsSurvivingCollection(t *testing.T) {
	s := &wdDeleteServer{status: http.StatusNoContent, keep: true}
	b := newWDDeleteBackend(t, s)
	err := b.RemoveAll(context.Background(), "dir")
	if err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Fatalf("RemoveAll = %v, want a still-exists failure", err)
	}
	// A single file delete trusts the status: no read-back.
	if err := b.Remove(context.Background(), "f"); err != nil {
		t.Fatalf("Remove = %v", err)
	}
}

func TestWebDAVRemoveRefusesBase(t *testing.T) {
	s := &wdDeleteServer{status: http.StatusNoContent}
	b := newWDDeleteBackend(t, s)
	for _, in := range []string{"", ".", "/"} {
		if err := b.RemoveAll(context.Background(), in); err == nil {
			t.Errorf("RemoveAll(%q) succeeded", in)
		}
		if err := b.Remove(context.Background(), in); err == nil {
			t.Errorf("Remove(%q) succeeded", in)
		}
	}
	if s.deletes != 0 {
		t.Fatalf("%d DELETE request(s) reached the server", s.deletes)
	}
}

// wdMkcolServer mimics the Go webdav lock system behind rclone: a MKCOL for
// a path whose MKCOL is still in flight answers 423 Locked. PUT accepts.
type wdMkcolServer struct {
	mu       sync.Mutex
	inflight map[string]bool
	mkcols   map[string]int
	locked   int
}

func (s *wdMkcolServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "MKCOL":
		p := path.Clean(r.URL.Path)
		s.mu.Lock()
		s.mkcols[p]++
		if s.inflight[p] {
			s.locked++
			s.mu.Unlock()
			w.WriteHeader(423)
			return
		}
		s.inflight[p] = true
		s.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		s.mu.Lock()
		delete(s.inflight, p)
		s.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	case "PUT":
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusCreated)
	case "PROPFIND":
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(wdCollectionBody))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Uploads racing into one new directory send a single MKCOL per directory
// level, so the server never sees the duplicate it would answer with 423.
func TestWebDAVEnsureDirOnceUnderRace(t *testing.T) {
	s := &wdMkcolServer{inflight: map[string]bool{}, mkcols: map[string]int{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend("webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 8)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.CopyFrom(context.Background(), fmt.Sprintf("a/b/c/f%d", i), strings.NewReader("x"), 0644); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locked != 0 {
		t.Errorf("%d MKCOLs answered 423 Locked", s.locked)
	}
	for _, p := range []string{"/a", "/a/b", "/a/b/c"} {
		if n := s.mkcols[p]; n != 1 {
			t.Errorf("MKCOL %s sent %d times, want 1", p, n)
		}
	}
}
