package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"sc/model"
)

func entryNames(entries []model.FileEntry) []string {
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	slices.Sort(names)
	return names
}

// After the recursive dry run every directory, the empty one included, is a
// cache hit holding exactly what a live listing reports.
func TestRsyncRecursivePreloadPerDir(t *testing.T) {
	port, moduleDir := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"a/x.txt", "a/sub/1.txt", "a/sub/deep/2.txt", "b/1.txt", "top.txt"} {
		full := filepath.Join(moduleDir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(rel), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(moduleDir, "empty"), 0755); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	waitDone(t, b.listCache)
	for _, dir := range []string{"", "a", "a/sub", "a/sub/deep", "b", "empty"} {
		cached, hit, _, _ := b.listCache.lookup(dir)
		if !hit {
			t.Fatalf("%q: not cached after preload", dir)
		}
		live, err := b.liveList(ctx, dir)
		if err != nil {
			t.Fatalf("%q: live list: %v", dir, err)
		}
		if got, want := entryNames(cached), entryNames(live); !slices.Equal(got, want) {
			t.Fatalf("%q: cached %q, live %q", dir, got, want)
		}
	}
}

// OpenAt fetches only the tail of a file, credits only the tail, and reports
// a source that is shorter than the offset instead of returning nothing.
func TestRsyncOpenAt(t *testing.T) {
	port, moduleDir := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 3<<20)
	rand.Read(body)
	size := int64(len(body))
	if err := os.MkdirAll(filepath.Join(moduleDir, "dl"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(moduleDir, "dl", "big.bin"), body, 0644); err != nil {
		t.Fatal(err)
	}
	offset := size/2 + 4099
	var counter atomic.Int64
	ctx := ContextWithProgress(ContextWithFileSize(context.Background(), size), &counter)
	rc, err := b.OpenAt(ctx, "dl/big.bin", offset)
	if err != nil {
		t.Fatalf("OpenAt: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, body[offset:]) {
		t.Fatalf("OpenAt read %d bytes, identical=%v, want tail of %d (err %v)", len(got), bytes.Equal(got, body[offset:]), size-offset, err)
	}
	if c := counter.Load(); c != size-offset {
		t.Fatalf("progress credited %d bytes, want tail %d", c, size-offset)
	}
	if _, err := b.OpenAt(context.Background(), "dl/big.bin", size+10); err == nil {
		t.Fatal("OpenAt past the end of the source succeeded")
	}
}

// --append-verify makes the daemon check the bytes it already holds: a
// prefix that differs from the source fails the append, so the engine
// recopies in full instead of keeping a file that merely has the right size.
func TestRsyncAppendVerifyRejectsCorruptPrefix(t *testing.T) {
	port, moduleDir := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 1<<20)
	rand.Read(body)
	size := int64(len(body))
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "big.bin"), body, 0644); err != nil {
		t.Fatal(err)
	}
	src := &model.BackendRangeOpener{Backend: NewLocalBackend(srcDir), RelPath: "big.bin", FileSize: size}
	offset := size/2 + 4099
	corrupt := slices.Clone(body[:offset])
	corrupt[offset/2] ^= 0xff
	dst := filepath.Join(moduleDir, "v", "big.bin")
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, corrupt, 0644); err != nil {
		t.Fatal(err)
	}
	if err := b.AppendFrom(context.Background(), "v/big.bin", src, 0644, offset); err == nil {
		t.Fatal("append onto a corrupt prefix reported success")
	}
	if got, _ := os.ReadFile(dst); bytes.Equal(got, body) {
		t.Fatal("daemon file equals the source although the prefix was corrupt")
	}
}
