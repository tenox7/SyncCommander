package transport

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// wdStatusServer answers PROPFIND for the base, so the backend opens, and
// every other request with status, counting them. A PROPFIND of cut, when
// set, gets an answer cut in the middle.
type wdStatusServer struct {
	mu     sync.Mutex
	status int
	cut    string
	reqs   int
}

func (s *wdStatusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "PROPFIND" && r.URL.Path == "/" {
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(wdCollectionBody))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs++
	if r.Method == "PROPFIND" && s.cut != "" && strings.Trim(r.URL.Path, "/") == s.cut {
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response><d:href>/`))
		return
	}
	http.Error(w, http.StatusText(s.status), s.status)
}

func (s *wdStatusServer) answer(code int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.reqs
	s.status, s.reqs = code, 0
	return n
}

func newWDStatusBackend(t *testing.T, s *wdStatusServer) *WebDAVBackend {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(context.Background(), "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// W-M1: an HTTP answer was made text, and the text was read for words: a
// 404 or a 403 was tried again and again, and a 502 for a file named "no
// such file.bin" was taken for a missing file and tried once. Now the
// code says what an answer means, the same for every name.
func TestHTTPAnswersAreReadByTheirCode(t *testing.T) {
	ctx := context.Background()
	s := &wdStatusServer{}
	dav := newWDStatusBackend(t, s)
	f := &fakeRestic{objects: map[string]string{"config": "cfg"}}
	rst, err := NewResticBackend(ctx, fakeResticURL(t, f), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rst.Close() })
	for _, c := range []struct {
		code  int
		final bool
		kind  error
	}{
		{http.StatusNotFound, true, fs.ErrNotExist},
		{http.StatusGone, true, fs.ErrNotExist},
		{http.StatusForbidden, true, fs.ErrPermission},
		{http.StatusBadRequest, true, nil},
		{http.StatusConflict, true, nil},
		{http.StatusLengthRequired, true, nil},
		{http.StatusRequestedRangeNotSatisfiable, true, nil},
		{http.StatusNotImplemented, true, nil},
		{http.StatusInternalServerError, false, nil},
		{http.StatusBadGateway, false, nil},
		{http.StatusServiceUnavailable, false, nil},
		{http.StatusGatewayTimeout, false, nil},
		{http.StatusRequestTimeout, false, nil},
		{http.StatusLocked, false, nil},
		{http.StatusTooManyRequests, false, nil},
	} {
		check := func(what string, err error) {
			t.Helper()
			if err == nil {
				t.Errorf("%s answered %d: no error", what, c.code)
				return
			}
			if got := isPermanentError(err); got != c.final {
				t.Errorf("%s answered %d: final=%v, want %v (%v)", what, c.code, got, c.final, err)
			}
			if c.kind != nil && !errors.Is(err, c.kind) {
				t.Errorf("%s answered %d: %v is no %v", what, c.code, err, c.kind)
			}
		}
		for _, name := range []string{"archive.bin", "no such file.bin", "permission denied.bin"} {
			s.answer(c.code)
			_, err := dav.OpenAt(ctx, name, 0)
			check("webdav GET "+name, err)
		}
		f.setFail("GET data/"+keyB, c.code)
		_, err := rst.OpenAt(ctx, "data/1d/"+keyB, 0)
		check("restic GET", err)
	}
	f.setFail("GET data/"+keyB, 0)
}

// W-M1: under the retry, a 404 is asked once, and a 502 for a file named
// "no such file.bin" as often as any other 502. A folder of that name
// whose listing came cut short is listed again too: the retry read the
// words in the path put with the error.
func TestHTTPRetriesGoByTheAnswer(t *testing.T) {
	ctx := context.Background()
	defer SetMaxRetries(MaxRetries())
	SetMaxRetries(1)
	s := &wdStatusServer{}
	dav := newWDStatusBackend(t, s)
	get := func(name string) error {
		return Retry(ctx, "webdav", "GET "+name, func(ctx context.Context) error {
			_, err := dav.OpenAt(ctx, name, 0)
			return err
		})
	}
	s.answer(http.StatusNotFound)
	if err := get("archive.bin"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a 404: %v, want a missing file", err)
	}
	if n := s.answer(http.StatusBadGateway); n != 1 {
		t.Errorf("a 404 was asked %d times, want once", n)
	}
	if err := get("no such file.bin"); err == nil {
		t.Error("a 502: no error")
	}
	if n := s.answer(0); n != 2 {
		t.Errorf("a 502 for \"no such file.bin\" was asked %d times, want 2 (one more try)", n)
	}
	s.mu.Lock()
	s.cut = "no such file"
	s.mu.Unlock()
	err := Retry(ctx, "webdav", "LIST", func(ctx context.Context) error {
		_, err := dav.propfind(ctx, "no such file", "1")
		return err
	})
	if err == nil {
		t.Error("a cut listing: no error")
	}
	if n := s.answer(0); n != 2 {
		t.Errorf("a cut listing of the folder \"no such file\" was asked %d times, want 2 (one more try): %v", n, err)
	}
}

// W-H6 stays: rclone answers 404 for a restic listing that failed, so that
// 404 is no missing folder, and the listing is tried again.
func TestResticRclone404IsNoMissingFolder(t *testing.T) {
	f := &fakeRestic{objects: map[string]string{"config": "cfg"}, server: "rclone/v1.75.1", fail: map[string]int{"GET locks/": http.StatusNotFound}}
	b, err := NewResticBackend(context.Background(), fakeResticURL(t, f), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	_, err = b.List(context.Background(), "locks")
	if err == nil || errors.Is(err, fs.ErrNotExist) || isPermanentError(err) {
		t.Errorf("rclone's 404 for a listing: %v (missing: %v, final: %v); want a failure that is tried again", err, errors.Is(err, fs.ErrNotExist), isPermanentError(err))
	}
}
