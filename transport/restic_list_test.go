package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// W-L2: a restic v2 list was read to its last object, and its closing ]
// never: a list cut between two objects read as whole with one JSON
// decoder and not with another, and an {} read as no objects. A listing
// taken for whole lets a mirror delete what it does not show. Now only a
// list from its [ to its ] is whole; a null is none, as restic reads it
// (rest-server and rclone send [] for none).
func TestResticListMustEndWithItsBracket(t *testing.T) {
	ctx := context.Background()
	obj := `{"name":"` + keyB + `","size":3}`
	obj2 := `{"name":"` + strings.Repeat("e", 64) + `","size":4}`
	for _, c := range []struct {
		body string
		n    int // the objects a whole list holds; -1 for a list refused
	}{
		{"[" + obj + "]", 1},
		{"[" + obj + "," + obj2 + "]", 2},
		{"[]", 0},
		{"null", 0},
		{"[" + obj + "," + obj2, -1},
		{"[" + obj + ",", -1},
		{"[" + obj, -1},
		{"[", -1},
		{"", -1},
		{"{}", -1},
		{`{"error":"no such folder"}`, -1},
		{"nul", -1},
	} {
		f := &fakeRestic{objects: map[string]string{"config": "cfg"}, lists: map[string]int{}, fail: map[string]int{}}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && strings.Trim(r.URL.Path, "/") == "locks" {
				w.Header().Set("Content-Type", resticAPIV2)
				w.Write([]byte(c.body))
				return
			}
			f.ServeHTTP(w, r)
		}))
		b, err := NewResticBackend(ctx, "restic://u:p@"+srv.Listener.Addr().String()+"/", false, 1)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := b.List(ctx, "locks")
		if c.n < 0 && err == nil {
			t.Errorf("a list %q read as whole, %d entries; want it refused", c.body, len(entries))
		}
		if c.n >= 0 && (err != nil || len(entries) != c.n) {
			t.Errorf("a list %q: %d entries, %v; want %d", c.body, len(entries), err, c.n)
		}
		b.Close()
		srv.Close()
	}
}
