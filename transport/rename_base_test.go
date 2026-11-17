package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"sc/model"
)

// U11: no backend renames its base, from it or onto it. The UI's e on the
// top row asked for Rename("", name); rclone, on a remote without folder
// moves, then moved every object into name and joined the paths wrongly.
// Each base sits in the test's own folder or on an in-memory server.
func TestRenameRefusesTheBase(t *testing.T) {
	ctx := context.Background()
	calls := [][2]string{{"", "x"}, {"..", "x"}, {"a.txt", ""}, {"a.txt", ".."}}
	refusesAll := func(t *testing.T, b model.Backend) {
		t.Helper()
		for _, c := range calls {
			if err := b.Rename(ctx, c[0], c[1]); !refused(err) {
				t.Errorf("Rename(%q, %q) = %v, want it refused", c[0], c[1], err)
			}
		}
	}
	localBase := func(t *testing.T) string {
		base := filepath.Join(t.TempDir(), "a", "base")
		putFile(t, filepath.Join(base, "a.txt"), "a")
		return base
	}

	t.Run("local", func(t *testing.T) {
		base := localBase(t)
		refusesAll(t, NewLocalBackend(base))
		if got := filesUnder(t, base); !slices.Equal(got, []string{"a.txt"}) {
			t.Errorf("the base holds %q, want a.txt alone", got)
		}
	})

	t.Run("ftp", func(t *testing.T) {
		f := &fakeFTPD{
			ls:    map[string][]string{"/srv": {lsLine("base", true)}, "/srv/base": {lsLine("a.txt", false)}},
			store: map[string][]byte{"/srv/base/a.txt": []byte("a")},
		}
		refusesAll(t, openFakeFTP(t, f, "/srv/base"))
		if got := f.storedPaths(); !slices.Equal(got, []string{"/srv/base/a.txt"}) {
			t.Errorf("the server holds %q, want /srv/base/a.txt alone", got)
		}
	})

	t.Run("sftp", func(t *testing.T) {
		base := localBase(t)
		b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+startSFTPServer(t)+filepath.ToSlash(base), true, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		refusesAll(t, b)
		if got := filesUnder(t, base); !slices.Equal(got, []string{"a.txt"}) {
			t.Errorf("the base holds %q, want a.txt alone", got)
		}
	})

	t.Run("scp", func(t *testing.T) {
		base := localBase(t)
		b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+filepath.ToSlash(base), true, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		refusesAll(t, b)
		if got := filesUnder(t, base); !slices.Equal(got, []string{"a.txt"}) {
			t.Errorf("the base holds %q, want a.txt alone", got)
		}
	})

	t.Run("rclone", func(t *testing.T) {
		b, err := NewRcloneBackend(ctx, "rclone://:memory:u11/base", false)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		for _, p := range []string{"a.txt", "d/c.txt"} {
			if err := b.CopyFrom(ctx, p, strings.NewReader("x"), model.PutParams{Mode: 0o644}); err != nil {
				t.Fatal(err)
			}
		}
		refusesAll(t, b)
		var got []string
		for _, dir := range []string{"", "d"} {
			e, err := b.List(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range e {
				got = append(got, f.RelPath)
			}
		}
		slices.Sort(got) // the memory remote lists in no fixed order
		if want := []string{"a.txt", "d", "d/c.txt"}; !slices.Equal(got, want) {
			t.Errorf("the remote holds %q, want %q", got, want)
		}
	})

	t.Run("webdav", func(t *testing.T) {
		var mu sync.Mutex
		var sent []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "PROPFIND" && r.Header.Get("Depth") == "0" {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusMultiStatus)
				w.Write([]byte(`<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response>` +
					`<d:href>/dav/base/</d:href><d:propstat><d:status>HTTP/1.1 200 OK</d:status>` +
					`<d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat>` +
					`</d:response></d:multistatus>`))
				return
			}
			mu.Lock()
			sent = append(sent, r.Method+" "+r.URL.Path)
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		}))
		defer srv.Close()
		b, err := NewWebDAVBackend(ctx, "webdav://"+strings.TrimPrefix(srv.URL, "http://")+"/dav/base", false, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		refusesAll(t, b)
		mu.Lock()
		defer mu.Unlock()
		if len(sent) != 0 {
			t.Errorf("the server was sent %q, want nothing", sent)
		}
	})

	t.Run("fake", func(t *testing.T) {
		refusesAll(t, newFake(t, "fake://tiny"))
	})
}
