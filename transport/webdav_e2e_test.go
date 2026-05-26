package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

func rcloneBin(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat("/tmp/rclone"); err == nil {
		return "/tmp/rclone"
	}
	if p, err := exec.LookPath("rclone"); err == nil {
		return p
	}
	t.Skip("rclone not available")
	return ""
}

func startRcloneWebDAV(t *testing.T, root string) (port int, stop func()) {
	t.Helper()
	bin := rcloneBin(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port = l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(bin, "serve", "webdav", root,
		"--addr", fmt.Sprintf("127.0.0.1:%d", port),
		"--user", "u", "--pass", "p", "--dir-cache-time", "0")
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop = func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait() }) }
	t.Cleanup(stop)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if derr == nil {
			c.Close()
			time.Sleep(100 * time.Millisecond)
			return port, stop
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rclone webdav did not come up")
	return 0, stop
}

// expectedChildren walks root and returns, per directory rel path, the sorted
// "name|isDir" of its immediate children.
func expectedChildren(t *testing.T, root string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		parent := path.Dir(rel)
		if parent == "." {
			parent = ""
		}
		out[parent] = append(out[parent], fmt.Sprintf("%s|%t", d.Name(), d.IsDir()))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

func listSignature(entries []model.FileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, fmt.Sprintf("%s|%t", e.Name, e.IsDir))
	}
	sort.Strings(out)
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestWebDAVRecursivePreload seeds a .git-shaped tree (many tiny nested dirs),
// fires PreloadRecursive (Depth:infinity), and verifies (1) every directory
// lists correctly from the cache and (2) those listings are served entirely
// from the single recursive call — proven by killing the server and re-listing
// every dir, which must still succeed without any live per-dir PROPFIND.
func TestWebDAVRecursivePreload(t *testing.T) {
	root := t.TempDir()

	// .git-like layout: 64 object shards each with a file, plus refs/logs and
	// an intentionally empty dir to exercise the empty-leaf cache-hit path.
	seed := []string{"HEAD", "config", "description"}
	for i := 0; i < 64; i++ {
		seed = append(seed, fmt.Sprintf("objects/%02x/%02x%02x", i, i, i))
	}
	seed = append(seed,
		"objects/pack/pack-abc.idx", "objects/pack/pack-abc.pack",
		"refs/heads/main", "refs/heads/dev", "refs/tags/v1",
		"logs/HEAD", "logs/refs/heads/main",
		"hooks/pre-commit.sample",
	)
	for _, rel := range seed {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(rel), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "objects/info"), 0755); err != nil {
		t.Fatal(err) // empty dir
	}

	port, stop := startRcloneWebDAV(t, root)

	var b *WebDAVBackend
	var err error
	for i := 0; i < 20; i++ {
		b, err = NewWebDAVBackend(fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), false, 8)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("NewWebDAVBackend: %v", err)
	}

	ctx := context.Background()
	expected := expectedChildren(t, root)

	baseLines := Log.Len()
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatalf("PreloadRecursive: %v", err)
	}
	// List("") awaits until the recursive listing completes.
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatalf("List root: %v", err)
	}

	// Exactly one recursive request should have been issued.
	rcount := 0
	for _, ln := range Log.Lines()[baseLines:] {
		if strings.Contains(ln, ">>> RPROPFIND ") {
			rcount++
		}
	}
	if rcount != 1 {
		t.Fatalf("expected 1 RPROPFIND, got %d", rcount)
	}

	// Every directory must list correctly while the server is still up.
	for dir, want := range expected {
		entries, err := b.List(ctx, dir)
		if err != nil {
			t.Fatalf("List %q: %v", dir, err)
		}
		if got := listSignature(entries); !eqStrings(got, want) {
			t.Fatalf("dir %q children mismatch:\n got=%v\nwant=%v", dir, got, want)
		}
	}

	// Kill the server, then re-list every directory. A cache hit needs no
	// network, so these must all still match — proving zero per-dir PROPFINDs.
	stop()
	for dir, want := range expected {
		entries, err := b.List(ctx, dir)
		if err != nil {
			t.Fatalf("List %q after shutdown: %v", dir, err)
		}
		if got := listSignature(entries); !eqStrings(got, want) {
			t.Fatalf("dir %q after shutdown mismatch:\n got=%v\nwant=%v", dir, got, want)
		}
	}
}

// TestWebDAVDepthInfinityFallback verifies that when a server refuses
// Depth:infinity (RFC 4918 §9.1), the backend latches noInfinity, falls back
// to per-dir Depth:1 listing, and does not retry the recursive request.
func TestWebDAVDepthInfinityFallback(t *testing.T) {
	var infinityHits, depth1Hits int
	var mu sync.Mutex

	ms := func(responses string) string {
		return `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">` + responses + `</d:multistatus>`
	}
	resp := func(href string, dir bool) string {
		rt := ""
		if dir {
			rt = "<d:resourcetype><d:collection/></d:resourcetype>"
		} else {
			rt = "<d:resourcetype/>"
		}
		return fmt.Sprintf(`<d:response><d:href>%s</d:href><d:propstat>`+
			`<d:status>HTTP/1.1 200 OK</d:status><d:prop>`+
			`<d:getcontentlength>3</d:getcontentlength>%s</d:prop></d:propstat></d:response>`, href, rt)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PROPFIND" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		switch r.Header.Get("Depth") {
		case "infinity":
			infinityHits++
		case "1":
			depth1Hits++
		}
		mu.Unlock()

		switch r.Header.Get("Depth") {
		case "0":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusMultiStatus)
			w.Write([]byte(ms(resp("/", true))))
		case "infinity":
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`<d:error xmlns:d="DAV:"><d:propfind-finite-depth/></d:error>`))
		case "1":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusMultiStatus)
			w.Write([]byte(ms(resp("/", true) + resp("/a/", true) + resp("/f.txt", false))))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	b, err := NewWebDAVBackend("webdav://"+addr+"/", false, 8)
	if err != nil {
		t.Fatalf("NewWebDAVBackend: %v", err)
	}
	ctx := context.Background()

	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatalf("PreloadRecursive: %v", err)
	}
	// List awaits the (failed) preload, then falls back to a live Depth:1.
	entries, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := listSignature(entries); !eqStrings(got, []string{"a|true", "f.txt|false"}) {
		t.Fatalf("fallback listing wrong: %v", got)
	}
	if !b.noInfinity.Load() {
		t.Fatal("noInfinity not latched after 403")
	}

	// A second preload must be a no-op (no further infinity attempt).
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatalf("PreloadRecursive 2: %v", err)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatalf("List 2: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if infinityHits != 1 {
		t.Fatalf("expected exactly 1 Depth:infinity attempt, got %d", infinityHits)
	}
	if depth1Hits == 0 {
		t.Fatal("expected fallback Depth:1 listing, got none")
	}
}
