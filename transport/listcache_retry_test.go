package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"sc/model"
)

// W-M7: a run cut off by its connection dropped all it had sent, though
// the folders it had completed were whole: the rest of the scan listed
// them again, one by one. Now those are kept, and the rest is dropped.
func TestListCacheCutOffRunKeepsWhatItListedInFull(t *testing.T) {
	c := newListCache() // no retries, as for a run on an ssh connection that died
	var runs atomic.Int32
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		runs.Add(1)
		emit("a", []model.FileEntry{{Name: "1"}, {Name: "2"}})
		complete("a")
		emit("b", []model.FileEntry{{Name: "3"}})
		return fmt.Errorf("reading the listing: %w", io.ErrUnexpectedEOF)
	})
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 2, false)
	mustServe(t, serveAsync(c, "b"), 1, true)
	if n := runs.Load(); n != 1 {
		t.Errorf("the run ran %d times on a cache that does not try again", n)
	}
}

// A run that failed on what the server sent keeps nothing, as before:
// what it sent is not trusted.
func TestListCacheRunThatFailedOnTheAnswerKeepsNothing(t *testing.T) {
	c := newListCache().retryingRuns("test")
	var runs atomic.Int32
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		runs.Add(1)
		emit("a", []model.FileEntry{{Name: "1"}, {Name: "2"}})
		complete("a")
		return fmt.Errorf("listing: %w", ErrIncompleteListing)
	})
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 1, true)
	if n := runs.Load(); n != 1 {
		t.Errorf("a run that cannot do better ran %d times", n)
	}
}

// W-M7: a cache whose backend opens a connection for each run tries a
// cut-off run again. A waiter waits through the retry and gets its folder
// from it; a folder the first try listed in full is kept, not added twice.
func TestListCacheTriesACutOffRunAgain(t *testing.T) {
	c := newListCache().retryingRuns("test")
	var runs atomic.Int32
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}, {Name: "2"}})
		complete("a")
		if runs.Add(1) == 1 {
			emit("b", []model.FileEntry{{Name: "3"}})
			return io.ErrUnexpectedEOF
		}
		emit("b", []model.FileEntry{{Name: "3"}, {Name: "4"}})
		complete("b")
		return nil
	})
	mustServe(t, serveAsync(c, "b"), 2, false)
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 2, false)
	if n := runs.Load(); n != 2 {
		t.Errorf("the run ran %d times, want 2", n)
	}
}

// davTree answers PROPFIND for a tree of a/x.txt and b/y.txt. infinity
// says how to answer each Depth: infinity request: by its number, from 1.
func davTree(t *testing.T, infinity func(n int, w http.ResponseWriter, body string)) (b *WebDAVBackend, hits func() (inf, one int)) {
	t.Helper()
	resp := func(href string, dir bool) string {
		rt := "<d:resourcetype/>"
		if dir {
			rt = "<d:resourcetype><d:collection/></d:resourcetype>"
		}
		return fmt.Sprintf(`<d:response><d:href>%s</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop>`+
			`<d:getcontentlength>3</d:getcontentlength>%s</d:prop></d:propstat></d:response>`, href, rt)
	}
	ms := func(r ...string) string {
		return `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">` + strings.Join(r, "") + `</d:multistatus>`
	}
	var mu sync.Mutex
	var inf, one int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PROPFIND" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		depth := r.Header.Get("Depth")
		n := 0
		switch depth {
		case "infinity":
			inf++
			n = inf
		case "1":
			one++
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		switch depth {
		case "0":
			w.WriteHeader(http.StatusMultiStatus)
			io.WriteString(w, ms(resp("/", true)))
		case "infinity":
			infinity(n, w, ms(resp("/", true), resp("/a/", true), resp("/a/x.txt", false), resp("/b/", true), resp("/b/y.txt", false)))
		case "1":
			w.WriteHeader(http.StatusMultiStatus)
			switch strings.Trim(r.URL.Path, "/") {
			case "a":
				io.WriteString(w, ms(resp("/a/", true), resp("/a/x.txt", false)))
			case "b":
				io.WriteString(w, ms(resp("/b/", true), resp("/b/y.txt", false)))
			default:
				io.WriteString(w, ms(resp("/", true), resp("/a/", true), resp("/b/", true)))
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	b, err := NewWebDAVBackend(context.Background(), "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/", false, 8)
	if err != nil {
		t.Fatal(err)
	}
	return b, func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		return inf, one
	}
}

// W-M7: a deep listing cut off half way was not sent again: the scan
// listed each folder on its own. Now it is, and the folders come from it.
func TestWebDAVDeepListingCutOffIsSentAgain(t *testing.T) {
	b, hits := davTree(t, func(n int, w http.ResponseWriter, body string) {
		if n == 1 {
			// Half the answer, then the connection goes.
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusMultiStatus)
			io.WriteString(w, body[:len(body)*2/3])
			return
		}
		w.WriteHeader(http.StatusMultiStatus)
		io.WriteString(w, body)
	})
	ctx := context.Background()
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for dir, want := range map[string]int{"": 2, "a": 1, "b": 1} {
		e, err := b.List(ctx, dir)
		if err != nil || len(e) != want {
			t.Errorf("List %q = %d entries, %v; want %d", dir, len(e), err, want)
		}
	}
	if inf, one := hits(); inf != 2 || one != 0 {
		t.Errorf("%d deep and %d one-level listings; want the deep one sent again, and no one-level one", inf, one)
	}
}

// W-M7: only 403 latched noInfinity. A server that answers Depth: infinity
// with 400, 501 or 507 answers it so each time, and each scan asked again.
// Now each latches, and none is tried again.
func TestWebDAVRefusedDeepListingsLatch(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotImplemented, http.StatusInsufficientStorage} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			b, hits := davTree(t, func(_ int, w http.ResponseWriter, _ string) { w.WriteHeader(status) })
			ctx := context.Background()
			for range 2 {
				if err := b.PreloadRecursive(ctx, ""); err != nil {
					t.Fatal(err)
				}
				if e, err := b.List(ctx, ""); err != nil || len(e) != 2 {
					t.Fatalf("List = %d entries, %v; want the 2 of a one-level listing", len(e), err)
				}
			}
			if !b.noInfinity.Load() {
				t.Error("noInfinity not latched")
			}
			if inf, _ := hits(); inf != 1 {
				t.Errorf("%d deep listings asked for, want 1", inf)
			}
		})
	}
}
