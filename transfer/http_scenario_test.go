package transfer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// The use of http://: a folder a web server lists, synced to a local
// folder, through the real scanner and copy engine. A second scan finds
// the two the same, and a mirror deletes what the server does not list.
func TestScenarioDownloadFromWebServer(t *testing.T) {
	root := t.TempDir()
	web, dst := filepath.Join(root, "web"), filepath.Join(root, "local")
	files := map[string]string{"a.txt": "hello", "docs/b.txt": "bb", "docs/sub/c d.bin": "ccc", "100%.txt": "%"}
	when := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	for p, body := range files {
		writeFile(t, filepath.Join(web, filepath.FromSlash(p)), body)
		if err := os.Chtimes(filepath.Join(web, filepath.FromSlash(p)), when, when); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(web, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(web)))
	defer srv.Close()

	scan := func() *scenario {
		src, err := transport.TryOpenBackend(srv.URL+"/", false, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { src.Close() })
		s := &scenario{left: web, right: dst, lb: src, rb: transport.NewLocalBackend(dst)}
		s.sc = model.NewScanner(s.lb, s.rb, 1, 1, true)
		s.sc.Scan(context.Background(), scenarioOpts)
		return s
	}

	s := scan()
	p := s.copy(t, "", true, false)
	if p.Failed.Load() != 0 || p.Done.Load() != 5 {
		t.Errorf("download: done=%d failed=%d, want 4 files and 1 empty folder done", p.Done.Load(), p.Failed.Load())
	}
	wantTree(t, "after the download", dst, "100%.txt", "a.txt", "docs/", "docs/b.txt", "docs/sub/", "docs/sub/c d.bin", "empty/")
	for p, body := range files {
		full := filepath.Join(dst, filepath.FromSlash(p))
		if got := readFile(t, full); got != body {
			t.Errorf("%s holds %q, want %q", p, got, body)
		}
		if st, err := os.Stat(full); err != nil || !st.ModTime().Equal(when) {
			t.Errorf("%s: mtime %v, want the server's %v", p, st.ModTime(), when)
		}
	}

	var stats model.TreeStats
	s = scan()
	s.sc.ReadTree(func(r *model.TreeNode) { stats = model.PropagateStatus(r, &scenarioOpts) })
	if stats.FilesDiff != 0 || stats.FilesLeftOnly != 0 || stats.FilesRightOnly != 0 || stats.FilesEqual != 4 {
		t.Errorf("a scan after the download: %d equal, %d different, %d only on the server, %d only here; want 4 equal",
			stats.FilesEqual, stats.FilesDiff, stats.FilesLeftOnly, stats.FilesRightOnly)
	}

	writeFile(t, filepath.Join(dst, "docs", "extra.txt"), "not on the server")
	s = scan()
	p = s.copy(t, "", true, true)
	if p.Failed.Load() != 0 {
		t.Errorf("mirror: %d failed", p.Failed.Load())
	}
	wantTree(t, "after the mirror", dst, "100%.txt", "a.txt", "docs/", "docs/b.txt", "docs/sub/", "docs/sub/c d.bin", "empty/")

	// A copy to the web server stops at once: it takes no writes.
	writeFile(t, filepath.Join(dst, "up.txt"), "upload")
	s = scan()
	p = s.copy(t, "", false, false)
	if p.Failed.Load() != 1 {
		t.Errorf("an upload to http://: %d failed, want the one", p.Failed.Load())
	}
	if _, err := os.Stat(filepath.Join(web, "up.txt")); err == nil {
		t.Error("an upload reached the server's folder")
	}
}
