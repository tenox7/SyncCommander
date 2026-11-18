package model_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// hintBackend records what each checksum call says the far side hashes
// for it (model.HashBytes). With prefetch, it takes folder fetches, and
// gives no sums for them: every file is then summed by itself too.
type hintBackend struct {
	model.Backend
	prefetch bool
	mu       sync.Mutex
	hints    map[string]int64
}

func (b *hintBackend) note(call string, ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hints[call] = model.HashBytes(ctx)
}

func (b *hintBackend) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.Backend.Capabilities(ctx)
	caps.PrefetchSums = b.prefetch
	return caps, err
}

func (b *hintBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	b.note("sum "+relPath, ctx)
	return b.Backend.Checksum(ctx, relPath, algo)
}

func (b *hintBackend) PrefetchChecksums(ctx context.Context, scope, _ string, _ bool) (map[string]string, error) {
	b.note("fetch "+scope, ctx)
	return nil, nil
}

// R17: an rsync daemon hashes the files a checksum call names before it
// answers, and says nothing while it does; it may take hours. The scan
// says how many bytes that is, so the backend allows the time: a folder's
// fetch, the files under it on that side, those on that side only too,
// and no link; a file's sum, its size.
func TestTheChecksumPassSaysWhatEachCallHashes(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	write := func(root, rel string, size int) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(strings.Repeat("x", size)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	for _, root := range []string{left, right} {
		write(root, "d/a.txt", 100)
		write(root, "d/e/b.txt", 250)
		write(root, "r.txt", 7)
	}
	write(left, "d/only.txt", 30) // on the left only: the fetch hashes it too
	// A link is no file to hash: its size is the length of what it names.
	if err := os.Symlink("a-name-of-some-length.txt", filepath.Join(left, "d", "link")); err != nil {
		t.Fatal(err)
	}
	lb := &hintBackend{Backend: transport.NewLocalBackend(left), prefetch: true, hints: map[string]int64{}}
	rb := &hintBackend{Backend: transport.NewLocalBackend(right), hints: map[string]int64{}}
	s := model.NewScanner(lb, rb, 2, 2, true)
	s.Scan(context.Background(), model.CompareOpts{Size: true, ModTime: true, Checksum: true})

	for _, c := range []struct {
		b    *hintBackend
		side string
		want map[string]int64
	}{
		{lb, "left", map[string]int64{"fetch d": 380, "sum d/a.txt": 100, "sum d/e/b.txt": 250, "sum r.txt": 7}},
		{rb, "right", map[string]int64{"sum d/a.txt": 100, "sum d/e/b.txt": 250, "sum r.txt": 7}},
	} {
		for call, want := range c.want {
			got, ok := c.b.hints[call]
			switch {
			case !ok:
				t.Errorf("%s: no call %q; calls: %v", c.side, call, c.b.hints)
			case got != want:
				t.Errorf("%s: %q says it hashes %d bytes, want %d", c.side, call, got, want)
			}
		}
	}
}
