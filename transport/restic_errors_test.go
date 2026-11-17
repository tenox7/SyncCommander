package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

const keyB = "1d2e3f405162738495a6b7c8d9eaf00112233445566778899aabbccddeeff00"

// fakeResticURL serves f and gives the URL of its repository.
func fakeResticURL(t *testing.T, f *fakeRestic) string {
	t.Helper()
	if f.lists == nil {
		f.lists = map[string]int{}
	}
	if f.fail == nil {
		f.fail = map[string]int{}
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return "restic://u:p@" + srv.Listener.Addr().String() + "/"
}

func (f *fakeRestic) setFail(req string, code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if code == 0 {
		delete(f.fail, req)
		return
	}
	f.fail[req] = code
}

// requests gives the requests since its last call.
func (f *fakeRestic) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.reqs
	f.reqs = nil
	return out
}

func resticRows(entries []model.FileEntry) []string {
	var out []string
	for _, e := range entries {
		out = append(out, e.RelPath)
	}
	slices.Sort(out)
	return out
}

// W-H6: a failed HEAD of config said the repository has none. A 502 left
// config out of the root's listing with no error, and a mirror from that
// side deleted the other repository's config. Only a 404 says there is
// none; any other answer fails the listing, live or preloaded.
func TestResticConfigThatFailsIsNoMissingConfig(t *testing.T) {
	f := &fakeRestic{objects: map[string]string{"config": "cfg", "keys/" + keyA: "k"}}
	url := fakeResticURL(t, f)
	b, err := NewResticBackend(context.Background(), url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	f.setFail("HEAD config", http.StatusBadGateway)
	if e, err := b.List(ctx, ""); err == nil {
		t.Errorf("List of the root while HEAD config answers 502 = %q, nil; want an error", resticRows(e))
	}
	b.PreloadRecursive(ctx, "")
	if e, err := b.List(ctx, ""); err == nil {
		t.Errorf("List of the root after a preload while HEAD config answers 502 = %q, nil; want an error", resticRows(e))
	}
	if _, err := NewResticBackend(ctx, url, false, 1); err == nil {
		t.Error("the connect check took a 502 for config for a repository")
	}
	f.setFail("HEAD config", -1)
	if e, err := b.List(model.WithFreshListing(ctx), ""); err == nil {
		t.Errorf("List of the root while HEAD config gets no answer = %q, nil; want an error", resticRows(e))
	}

	// A 404 is a repository with no config: listed without it, no error.
	f.setFail("HEAD config", http.StatusNotFound)
	want := []string{"data", "index", "keys", "locks", "snapshots"}
	if e, err := b.List(model.WithFreshListing(ctx), ""); err != nil || !slices.Equal(resticRows(e), want) {
		t.Errorf("List of the root while config is not there = %q, %v; want %q", resticRows(e), err, want)
	}
	f.setFail("HEAD config", 0)
	if e, err := b.List(model.WithFreshListing(ctx), ""); err != nil || !slices.Contains(resticRows(e), "config") {
		t.Errorf("List of the root = %q, %v; want config in it", resticRows(e), err)
	}
}

// W-H6: a v1 server lists names only, and each size comes from a HEAD. A
// failed HEAD made the size 0 with no error. An object deleted since the
// listing is not there; any other failure fails the listing.
func TestResticV1SizeThatFailsFailsTheListing(t *testing.T) {
	f := &fakeRestic{objects: map[string]string{"config": "cfg", "keys/" + keyA: "k", "keys/" + keyB: "kk"}, v1: true}
	b, err := NewResticBackend(context.Background(), fakeResticURL(t, f), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := model.WithFreshListing(context.Background())
	e, err := b.List(ctx, "keys")
	if err != nil || len(e) != 2 {
		t.Fatalf("List(keys) on a v1 server = %+v, %v", e, err)
	}
	for _, x := range e {
		if want := int64(len(f.objects["keys/"+x.Name])); x.Size != want {
			t.Errorf("%s: size %d, want %d", x.RelPath, x.Size, want)
		}
	}
	f.setFail("HEAD keys/"+keyB, http.StatusInternalServerError)
	if e, err := b.List(ctx, "keys"); err == nil {
		t.Errorf("List(keys) while a size answers 500 = %+v, nil; want an error", e)
	}
	f.setFail("HEAD keys/"+keyB, http.StatusNotFound)
	if e, err := b.List(ctx, "keys"); err != nil || !slices.Equal(resticRows(e), []string{"keys/" + keyA}) {
		t.Errorf("List(keys) with one object gone since the listing = %q, %v; want only keys/%s", resticRows(e), err, keyA)
	}
}

// W-H6: rclone answers a listing of a path that does not exist as an
// empty one, so a mistyped path opened as an empty repository, and a
// mirror from it deleted what the other side holds. A path without a
// config is no repository and is refused, unless sc runs with -restic-new
// to copy one into an empty path; even then the server must answer as a
// restic server does.
func TestResticPathWithoutARepositoryIsRefused(t *testing.T) {
	ctx := context.Background()
	SetResticNew(false)
	for name, objects := range map[string]map[string]string{
		"nothing":      {},
		"objects only": {"data/" + packA: "a", "keys/" + keyA: "k"},
	} {
		_, err := NewResticBackend(ctx, fakeResticURL(t, &fakeRestic{objects: objects}), false, 1)
		if !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), "-restic-new") {
			t.Errorf("%s: open = %v; want no repository, naming -restic-new", name, err)
		}
	}

	SetResticNew(true)
	defer SetResticNew(false)
	b, err := NewResticBackend(ctx, fakeResticURL(t, &fakeRestic{objects: map[string]string{}}), false, 1)
	if err != nil {
		t.Fatalf("-restic-new, an empty path: %v", err)
	}
	b.Close()
	// Any web server answers a 404 for config; the listing must answer too.
	web := &fakeRestic{objects: map[string]string{}, fail: map[string]int{"GET keys/": http.StatusNotFound}}
	if _, err := NewResticBackend(ctx, fakeResticURL(t, web), false, 1); err == nil {
		t.Error("-restic-new took a server that answers no listing for a repository")
	}
}

// W-H6, on rclone: a path that does not exist, which rclone lists as an
// empty repository, is refused; with -restic-new it opens empty, and once
// it has a config it is a repository.
func TestResticMistypedPathOnRclone(t *testing.T) {
	ctx := context.Background()
	SetResticNew(false)
	port, _ := startRcloneRestic(t, t.TempDir())
	mistyped := fmt.Sprintf("restic://u:p@127.0.0.1:%d/no/such/repo/", port)
	var err error
	for range 20 { // until rclone answers
		if _, err = NewResticBackend(ctx, mistyped, false, 1); errors.Is(err, fs.ErrNotExist) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a path that does not exist: open = %v; want no repository", err)
	}

	SetResticNew(true)
	defer SetResticNew(false)
	b, err := NewResticBackend(ctx, mistyped, false, 1)
	if err != nil {
		t.Fatalf("-restic-new, an empty path: %v", err)
	}
	defer b.Close()
	if e, err := b.List(ctx, ""); err != nil || slices.Contains(resticRows(e), "config") {
		t.Errorf("-restic-new: the root of an empty path = %q, %v", resticRows(e), err)
	}
	resticPut(t, b, "config", []byte("cfg"))
	SetResticNew(false)
	if b, err := NewResticBackend(ctx, mistyped, false, 1); err != nil {
		t.Errorf("the path with its config: %v", err)
	} else {
		b.Close()
	}
}

// W-H6: Mkdir and SetTimes did nothing and returned nil. An object keeps no
// time, and a repository has only its layout's folders: they say so, and
// nothing reaches the server.
func TestResticSaysWhatItCannotDo(t *testing.T) {
	f := &fakeRestic{objects: map[string]string{"config": "cfg", "data/" + packA: "a"}}
	b, err := NewResticBackend(context.Background(), fakeResticURL(t, f), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	f.requests()
	for _, p := range []string{"", "data", "index", "keys", "locks", "snapshots", "data/9f", "data/ab/"} {
		if err := b.Mkdir(ctx, p, 0o755); err != nil {
			t.Errorf("Mkdir(%q), a folder of the layout = %v", p, err)
		}
	}
	for _, p := range []string{"tmp", "data/x", "data/9f/sub", "config", "keys/sub", "data/9f/" + packA} {
		if err := b.Mkdir(ctx, p, 0o755); !errors.Is(err, ErrUnsupported) {
			t.Errorf("Mkdir(%q) = %v; want ErrUnsupported", p, err)
		}
	}
	for _, p := range []string{"data/9f/" + packA, "config"} {
		if err := b.SetTimes(ctx, p, time.Unix(1e9, 0), time.Time{}, time.Time{}); !errors.Is(err, ErrUnsupported) {
			t.Errorf("SetTimes(%s) = %v; want ErrUnsupported", p, err)
		}
	}
	if r := f.requests(); len(r) > 0 {
		t.Errorf("requests reached the server: %q", r)
	}
}

// W-H6: a DELETE answered 404 counted as done. Nothing was deleted: the
// object is not there by that name. It is fs.ErrNotExist, and a folder's
// delete fails on it.
func TestResticDeleteOfAMissingObjectFails(t *testing.T) {
	SetResticDeletes(true)
	defer SetResticDeletes(false)
	snapA, snapB := packC, keyB
	f := &fakeRestic{objects: map[string]string{"config": "cfg", "keys/" + keyA: "k", "snapshots/" + snapA: "s", "snapshots/" + snapB: "t"}}
	b, err := NewResticBackend(context.Background(), fakeResticURL(t, f), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if err := b.Remove(ctx, "keys/"+keyB); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Remove of an object that is not there = %v; want fs.ErrNotExist", err)
	}
	if err := b.Remove(ctx, "keys/"+keyA); err != nil {
		t.Errorf("Remove of an object = %v", err)
	}
	f.setFail("DELETE snapshots/"+snapB, http.StatusNotFound)
	if err := b.RemoveAll(ctx, "snapshots"); err == nil {
		t.Error("RemoveAll(snapshots) = nil while the server deleted not all of it")
	}
	f.setFail("DELETE snapshots/"+snapB, 0)
	if err := b.RemoveAll(ctx, "snapshots"); err != nil {
		t.Errorf("RemoveAll(snapshots) = %v", err)
	}

	// An object deleted behind sc's back is still in a preloaded listing;
	// the 404 of its delete drops that listing, so the next one is the
	// server's.
	f.mu.Lock()
	f.objects["keys/"+keyB] = "k2"
	f.mu.Unlock()
	b.PreloadRecursive(ctx, "")
	if e, err := b.List(ctx, "keys"); err != nil || !slices.Equal(resticRows(e), []string{"keys/" + keyB}) {
		t.Fatalf("List(keys) after a preload = %q, %v", resticRows(e), err)
	}
	f.mu.Lock()
	delete(f.objects, "keys/"+keyB)
	f.mu.Unlock()
	if err := b.Remove(ctx, "keys/"+keyB); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Remove of an object deleted behind sc's back = %v; want fs.ErrNotExist", err)
	}
	if e, err := b.List(ctx, "keys"); err != nil || len(e) > 0 {
		t.Errorf("List(keys) after the delete found nothing = %q, %v; want it empty, as the server holds it", resticRows(e), err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if want := []string{"config"}; !slices.Equal(slices.Sorted(mapKeys(f.objects)), want) {
		t.Errorf("the server holds %q; want %q", slices.Sorted(mapKeys(f.objects)), want)
	}
}

func mapKeys(m map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// W-H6: a listing answered 404 was an empty folder. From rest-server it
// is one the repository does not have, as restic's own client reads it;
// rclone lists such a folder as empty and answers 404 for a listing that
// failed, so from rclone it is an error.
func TestResticListing404(t *testing.T) {
	for _, c := range []struct {
		server string
		ok     bool
	}{{"", true}, {"rest-server/0.13", true}, {"rclone/v1.75.2", false}} {
		f := &fakeRestic{objects: map[string]string{"config": "cfg"}, server: c.server, fail: map[string]int{"GET locks/": http.StatusNotFound}}
		b, err := NewResticBackend(context.Background(), fakeResticURL(t, f), false, 1)
		if err != nil {
			t.Fatal(err)
		}
		e, err := b.List(context.Background(), "locks")
		if (err == nil) != c.ok || len(e) > 0 {
			t.Errorf("Server %q: List(locks) answered 404 = %+v, %v; want ok %v", c.server, e, err, c.ok)
		}
		b.Close()
	}
}
