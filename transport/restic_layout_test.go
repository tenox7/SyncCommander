package transport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"sc/model"
)

// fakeRestic is a restic REST server in memory: objects by REST path
// (data/<name>, keys/<name>, config), and what reached it.
type fakeRestic struct {
	mu      sync.Mutex
	objects map[string]string
	lists   map[string]int // listings of each type
	writes  int
	deletes int
}

func (f *fakeRestic) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := strings.TrimPrefix(r.URL.Path, "/")
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(p, "/"):
		typ := strings.TrimSuffix(p, "/")
		f.lists[typ]++
		list := []map[string]any{}
		for k, body := range f.objects {
			if t, name, _ := strings.Cut(k, "/"); t == typ {
				list = append(list, map[string]any{"name": name, "size": len(body)})
			}
		}
		w.Header().Set("Content-Type", resticAPIV2)
		json.NewEncoder(w).Encode(list)
	case r.Method == http.MethodHead || r.Method == http.MethodGet:
		body, ok := f.objects[p]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodGet {
			io.WriteString(w, body)
		}
	case r.Method == http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		f.objects[p] = string(body)
		f.writes++
	case r.Method == http.MethodDelete:
		delete(f.objects, p)
		f.deletes++
	default:
		http.Error(w, "not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *fakeRestic) counts() (writes, deletes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes, f.deletes
}

func openFakeRestic(t *testing.T, objects map[string]string) (*ResticBackend, *fakeRestic) {
	t.Helper()
	f := &fakeRestic{objects: objects, lists: map[string]int{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	b, err := NewResticBackend(context.Background(), "restic://u:p@"+srv.Listener.Addr().String()+"/", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	return b, f
}

const (
	packA = "9f81e2c4a1b3d5f7091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70"
	packB = "9f00aa11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff0011"
	packC = "ab0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd"
	keyA  = "0c1d2e3f405162738495a6b7c8d9eaf00112233445566778899aabbccddeeff0"
)

// W-C3, D10: each object of a repository has one path, the one restic
// keeps it at on disk. The REST name of a data pack, data/<name>, and any
// other spelling name no object: a write there is refused before it
// reaches the server, which would store it as the object it names.
func TestResticOnePathPerObject(t *testing.T) {
	for _, c := range []struct{ typ, name, path string }{
		{"data", packA, "data/9f/" + packA},
		{"data", "ab", "data/ab/ab"},
		{"data", "x", "data/x"},        // too short for a shard folder
		{"data", "-odd", "data/-odd"},  // no ASCII shard
		{"keys", keyA, "keys/" + keyA}, // only data has shard folders
		{"snapshots", "ab", "snapshots/ab"},
	} {
		if got := resticPath(c.typ, c.name); got != c.path {
			t.Errorf("resticPath(%s, %s) = %s, want %s", c.typ, c.name, got, c.path)
		}
		if obj, ok := resticObject(c.path); !ok || obj != c.typ+"/"+c.name {
			t.Errorf("resticObject(%s) = %s, %v; want %s/%s", c.path, obj, ok, c.typ, c.name)
		}
	}
	if obj, ok := resticObject("config"); !ok || obj != "config" {
		t.Errorf("resticObject(config) = %s, %v", obj, ok)
	}
	second := []string{
		"data/" + packA,           // the REST name
		"data/00/" + packA,        // another shard folder
		"data/9f/" + packA + "/x", // below an object
		"data/9f",                 // a shard folder
		"data", "", "keys",        // folders
		"keys/9f/" + keyA,           // keys has no shard folders
		"nope/" + packA, "config/x", // no such type
		"data/..", "keys/..", "keys/.", // not a name
	}
	for _, p := range second {
		if obj, ok := resticObject(p); ok {
			t.Errorf("resticObject(%q) = %s: a second name for an object", p, obj)
		}
	}

	b, f := openFakeRestic(t, map[string]string{"data/" + packA: "pack a"})
	ctx := context.Background()
	folders := map[string]bool{"": true, "data": true, "keys": true, "data/9f": true}
	for _, p := range second {
		err := b.CopyFrom(ctx, p, strings.NewReader("other bytes"), model.PutParams{Size: 11, Mode: 0o644})
		if !errors.Is(err, ErrUnsupported) {
			t.Errorf("CopyFrom(%q) = %v; want a refusal", p, err)
		}
		if folders[p] {
			continue
		}
		if e, err := b.Stat(ctx, p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat(%q) = %+v, %v; want fs.ErrNotExist", p, e, err)
		}
	}
	if w, _ := f.counts(); w != 0 {
		t.Errorf("%d write(s) reached the server", w)
	}
	if got := f.objects["data/"+packA]; got != "pack a" {
		t.Errorf("the pack holds %q", got)
	}
}

// A name the server lists that is not one path element would put the
// object under another folder's path; it is not listed.
func TestResticListsOnlyNames(t *testing.T) {
	b, _ := openFakeRestic(t, map[string]string{"keys/" + keyA: "k", "keys/..": "x", "keys/.": "y"})
	entries, err := b.List(context.Background(), "keys")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].RelPath != "keys/"+keyA {
		t.Errorf("keys lists %+v; want only keys/%s", entries, keyA)
	}
}

// The REST API lists a whole type at once: a scan that opens the shard
// folders of data one by one fetches data once, not once per shard.
func TestResticListsDataOnce(t *testing.T) {
	// With a config the connect check asks for it alone, not for data.
	b, f := openFakeRestic(t, map[string]string{"config": "cfg", "data/" + packA: "a", "data/" + packB: "b", "data/" + packC: "c", "data/x": "short"})
	ctx := context.Background()
	shards, err := b.List(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range shards {
		got = append(got, e.RelPath)
		if e.IsDir {
			objs, err := b.List(ctx, e.RelPath)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range objs {
				got = append(got, o.RelPath)
			}
		}
	}
	want := []string{"data/9f", "data/9f/" + packB, "data/9f/" + packA, "data/ab", "data/ab/" + packC, "data/x"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("data lists\n %q\nwant\n %q", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := f.lists["data"]; n != 1 {
		t.Errorf("listing data and its %d shard folders fetched data %d times; want once", len(shards)-1, n)
	}
}

// W-C3: a repository is a backup. No delete reaches the server unless sc
// was started with -restic-delete; the repository itself is never removed;
// a folder's delete deletes the objects in it. A folder was sent as one
// DELETE of its path, which deleted nothing and succeeded.
func TestResticDeletes(t *testing.T) {
	ctx := context.Background()
	objects := func() map[string]string {
		return map[string]string{"data/" + packA: "a", "data/" + packB: "b", "data/" + packC: "c", "keys/" + keyA: "k", "config": "cfg"}
	}

	SetResticDeletes(false)
	b, f := openFakeRestic(t, objects())
	for _, p := range []string{"data/9f/" + packA, "config"} {
		if err := b.Remove(ctx, p); !errors.Is(err, fs.ErrPermission) {
			t.Errorf("Remove(%s) without -restic-delete = %v; want a refusal", p, err)
		}
	}
	if err := b.RemoveAll(ctx, "data/9f"); !errors.Is(err, fs.ErrPermission) {
		t.Errorf("RemoveAll of a shard folder without -restic-delete = %v; want a refusal", err)
	}
	if err := b.RemoveAll(ctx, "locks"); err != nil {
		t.Errorf("RemoveAll of an empty folder = %v; nothing to delete is no refusal", err)
	}
	if _, d := f.counts(); d != 0 {
		t.Errorf("%d delete(s) reached the server without -restic-delete", d)
	}

	SetResticDeletes(true)
	defer SetResticDeletes(false)
	for _, p := range []string{"", ".", "/"} {
		if err := b.RemoveAll(ctx, p); err == nil {
			t.Errorf("RemoveAll(%q) removed the repository", p)
		}
	}
	if _, d := f.counts(); d != 0 {
		t.Errorf("%d delete(s) reached the server for the repository itself", d)
	}
	if err := b.RemoveAll(ctx, "data/9f"); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove(ctx, "keys/"+keyA); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	left := len(f.objects)
	_, kept := f.objects["data/"+packC]
	f.mu.Unlock()
	if left != 2 || !kept {
		t.Errorf("after deleting data/9f and one key the server holds %v; want data/%s and config", f.objects, packC)
	}
}
