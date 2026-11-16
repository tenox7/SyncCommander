package transport

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/rclone/rclone/backend/memory"

	"sc/model"
)

func TestRcloneLocalE2E(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub/deep"), 0755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello world"), 0644)
	os.WriteFile(filepath.Join(src, "sub/b.txt"), []byte("bbbb"), 0644)
	os.MkdirAll(filepath.Join(src, "empty"), 0755)

	b, err := NewRcloneBackend(context.Background(), "rclone://"+src, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ents, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 3 {
		t.Fatalf("root: got %d entries: %+v", len(ents), ents)
	}
	for _, e := range ents {
		if e.Name == "a.txt" && (e.Size != 11 || e.IsDir || e.ModTime.IsZero()) {
			t.Errorf("a.txt bad: %+v", e)
		}
		if e.Name == "sub" && (!e.IsDir || !e.Mode.IsDir()) {
			t.Errorf("sub bad: %+v", e)
		}
	}

	algos, _ := b.ChecksumAlgos(ctx)
	t.Logf("hashes: %v", algos)
	sum, err := b.Checksum(ctx, "a.txt", "md5")
	if err != nil || sum != "5eb63bbbe01eeed093cb22bb8f5acdc3" {
		t.Fatalf("md5: %q %v", sum, err)
	}
	if _, err := b.Checksum(ctx, "sub/b.txt", "xxh3"); err != nil {
		t.Fatalf("xxh3: %v", err)
	}

	rd, err := b.Open(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rd)
	rd.Close()
	if string(got) != "hello world" {
		t.Fatalf("open: %q", got)
	}

	rd, err = b.OpenAt(ctx, "a.txt", 6)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(rd)
	rd.Close()
	if string(got) != "world" {
		t.Fatalf("openat: %q", got)
	}

	// Copy into a second rclone backend, with and without a known size.
	dstDir := t.TempDir()
	d, err := NewRcloneBackend(context.Background(), "rclone://"+dstDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CopyFrom(ctx, "x/y/a.txt", strings.NewReader("hello world"), model.PutParams{Size: 11, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "x/y/a.txt")); string(got) != "hello world" {
		t.Fatalf("copy sized: %q", got)
	}
	if err := d.CopyFrom(ctx, "unsized.txt", strings.NewReader("nnnn"), model.PutParams{Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "unsized.txt")); string(got) != "nnnn" {
		t.Fatalf("copy unsized: %q", got)
	}

	// Resume into an rclone destination is not supported (see AppendFrom note).
	if _, ok := interface{}(d).(model.Resumer); ok {
		t.Fatal("RcloneBackend must not claim model.Resumer")
	}

	if err := d.Rename(ctx, "unsized.txt", "renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "renamed.txt")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := d.Remove(ctx, "renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if err := d.RemoveAll(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "x")); !os.IsNotExist(err) {
		t.Fatalf("removeall left %v", err)
	}
	if err := d.RemoveAll(ctx, ""); err == nil {
		t.Fatal("RemoveAll(\"\") must refuse")
	}
}

// Memory backend is bucket-based with ListR, exercising the preload path.
func TestRclonePreloadRecursive(t *testing.T) {
	b, err := NewRcloneBackend(context.Background(), "rclone://:memory:bkt", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, p := range []string{"a.txt", "sub/b.txt", "sub/deep/c.txt"} {
		if err := b.CopyFrom(ctx, p, strings.NewReader("data"), model.PutParams{Size: 4, Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	if !b.canListRecursive() {
		t.Fatal("memory should support recursive listing")
	}
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for dir, want := range map[string]int{"": 2, "sub": 2, "sub/deep": 1} {
		ents, err := b.List(ctx, dir)
		if err != nil {
			t.Fatalf("%q: %v", dir, err)
		}
		if len(ents) != want {
			t.Errorf("%q: got %d want %d: %+v", dir, len(ents), want, ents)
		}
	}
}
