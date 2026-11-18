package transport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rangeServer answers a GET of a file from an offset in the ways servers
// do: right and wrong 206s, a 200 of the whole file, slow or short, a 416.
func rangeServer(w http.ResponseWriter, r *http.Request) {
	if r.Method == "PROPFIND" {
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(wdCollectionBody))
		return
	}
	part := func(cr, body string) {
		w.Header().Set("Content-Range", cr)
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte(body))
	}
	switch strings.Trim(r.URL.Path, "/") {
	case "right":
		part("bytes 6-9/10", "6789")
	case "wrong", "data/" + keyB:
		part("bytes 0-9/10", "0123456789")
	case "none":
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("6789"))
	case "whole":
		w.Write([]byte("0123456789"))
	case "short":
		w.Write([]byte("0123"))
	case "beyond":
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	case "slow":
		f := w.(http.Flusher)
		for range 10 {
			w.Write([]byte(strings.Repeat("p", 1024)))
			f.Flush()
			time.Sleep(150 * time.Millisecond)
		}
		w.Write([]byte("6789"))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func readAt(t *testing.T, ctx context.Context, b interface {
	OpenAt(context.Context, string, int64) (io.ReadCloser, error)
}, name string, offset int64) (string, error) {
	t.Helper()
	rc, err := b.OpenAt(ctx, name, offset)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	return string(got), err
}

// W-L1: the answer to a GET from an offset was not checked. A 206 from
// the start of the file was taken for the rest: asked from 6, the caller
// got 0123456789, and a resume wrote the file with a part twice. Now a
// 206 must start where asked; one from elsewhere, a 416, or a file that
// ends before the offset say the server sends no such part, and a resume
// copies the whole file instead.
func TestRangeAnswersAreChecked(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(rangeServer))
	t.Cleanup(srv.Close)
	dav, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"right", "whole"} {
		if got, err := readAt(t, ctx, dav, name, 6); err != nil || got != "6789" {
			t.Errorf("%s from 6: %q, %v; want 6789", name, got, err)
		}
	}
	for _, name := range []string{"wrong", "none", "short", "beyond"} {
		if got, err := readAt(t, ctx, dav, name, 6); !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s from 6: %q, %v; want no part (ErrUnsupported)", name, got, err)
		}
	}
	f := &fakeRestic{objects: map[string]string{"config": "cfg"}, lists: map[string]int{}, fail: map[string]int{}}
	rsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, keyB) {
			rangeServer(w, r)
			return
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(rsrv.Close)
	rst, err := NewResticBackend(ctx, "restic://u:p@"+rsrv.Listener.Addr().String()+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rst.Close() })
	if got, err := readAt(t, ctx, rst, "data/1d/"+keyB, 6); !errors.Is(err, ErrUnsupported) {
		t.Errorf("restic: a 206 from 0 asked from 6: %q, %v; want no part (ErrUnsupported)", got, err)
	}
}

// W-L1: a server that sends the whole file for a GET from an offset: sc
// reads and drops what comes before it, and no progress shows that work,
// so the stall guard ended a long one. Now each read of it touches the
// guard.
func TestRangePrefixKeepsTheStallGuardCalm(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(rangeServer))
	t.Cleanup(srv.Close)
	dav, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	var progress atomic.Int64
	var got string
	err = WithStallGuard(ctx, &progress, time.Second, func(ctx context.Context) error {
		var err error
		got, err = readAt(t, ctx, dav, "slow", 10*1024)
		return err
	})
	if err != nil || got != "6789" {
		t.Errorf("a slow whole file from 10240: %q, %v; want 6789", got, err)
	}
}
