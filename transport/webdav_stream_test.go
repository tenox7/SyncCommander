package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// W-M8: a listing of one folder is read as it comes. It was read whole
// first, each response with its strings, and made into entries after: a
// folder of 1M files was held twice. Here the server sends 50 files and
// holds the end of its answer back: the 50 are read before the end comes.
func TestWebDAVListingIsReadAsItComes(t *testing.T) {
	const n = 50
	sent, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PROPFIND" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		root := `<d:response><d:href>/</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>`
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">` + root))
		if r.Header.Get("Depth") == "1" {
			for i := range n {
				fmt.Fprintf(w, `<d:response><d:href>/f%02d.txt</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>`+
					`<d:getcontentlength>3</d:getcontentlength><d:resourcetype/>`+
					`<oc:checksums><oc:checksum>MD5:%032d</oc:checksum></oc:checksums></d:prop></d:propstat></d:response>`, i, i)
			}
			w.(http.Flusher).Flush()
			close(sent)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Write([]byte(`</d:multistatus>`))
	}))
	defer srv.Close()
	ctx := context.Background()
	b, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		entries []model.FileEntry
		err     error
	}
	done := make(chan result, 1)
	go func() {
		e, err := b.List(ctx, "")
		done <- result{e, err}
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("the server sent no listing")
	}
	read := b.sums.held
	for deadline := time.Now().Add(3 * time.Second); read() < n && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	got := read()
	close(release)
	r := <-done
	if got < n {
		t.Errorf("while the answer was still coming, %d of the %d files sent were read; want all", got, n)
	}
	if r.err != nil || len(r.entries) != n {
		t.Errorf("List = %d entries, %v; want %d", len(r.entries), r.err, n)
	}
}

// An entry the listing should not hold fails the listing with one error,
// about the folder, and one line in the log: as it is read, the error of
// an entry is not the answer's.
func TestWebDAVBadEntryIsOneError(t *testing.T) {
	dir := func(href string) string {
		return `<d:response><d:href>` + href + `</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>`
	}
	file := func(href string) string {
		return `<d:response><d:href>` + href + `</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>` +
			`<d:getcontentlength>3</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := dir("/")
		if r.Header.Get("Depth") == "1" { // the listing of d, with a file of another folder
			body = dir("/d/") + file("/d/f.txt") + file("/other/x.txt")
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">` + body + `</d:multistatus>`))
	}))
	defer srv.Close()
	ctx := context.Background()
	b, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	mark := len(Log.Lines())
	entries, err := b.List(ctx, "d")
	var pe *fs.PathError
	if !errors.As(err, &pe) || pe.Path != b.pathFor("d") || errors.As(pe.Err, new(*fs.PathError)) {
		t.Errorf("List = %d entries, %v; want one error about %s", len(entries), err, b.pathFor("d"))
	}
	errs := 0
	for _, l := range Log.Lines()[mark:] {
		if strings.Contains(l, " webdav ERR ") {
			errs++
		}
	}
	if errs != 1 {
		t.Errorf("%d error lines in the log; want 1", errs)
	}
}
