package transport

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// HTTP backends report no atime; writing the zero time to the destination
// gave every copied file an atime in the year 1.
func TestLocalSetTimesZeroAtimeFallsBackToMtime(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := NewLocalBackend(dir)
	mtime := time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := b.SetTimes(context.Background(), "f", mtime, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "f"))
	if err != nil {
		t.Fatal(err)
	}
	var e model.FileEntry
	fillTimes(&e, info)
	if !info.ModTime().Equal(mtime) {
		t.Errorf("mtime = %v, want %v", info.ModTime(), mtime)
	}
	if !e.ATime.IsZero() && !e.ATime.Equal(mtime) {
		t.Errorf("atime = %v, want %v", e.ATime, mtime)
	}
}

func TestLocalRelPathsUseSlashes(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := NewLocalBackend(dir).List(context.Background(), "a")
	if err != nil || len(entries) != 1 || entries[0].RelPath != "a/b" {
		t.Fatalf("List = %+v, %v", entries, err)
	}
}

// A relPath that climbs with ".." must stay under base: local is the one
// backend where an escape would touch the host filesystem directly.
func TestLocalPathStaysUnderBase(t *testing.T) {
	dir := t.TempDir()
	b := NewLocalBackend(dir)
	for _, in := range []string{"../escape", "../../etc/passwd", "a/../../escape", "/abs", ".."} {
		got := b.LocalPath(in)
		rel, err := filepath.Rel(dir, got)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Errorf("LocalPath(%q) = %q, escapes %q", in, got, dir)
		}
	}
	if got, want := b.LocalPath("a/b"), filepath.Join(dir, "a", "b"); got != want {
		t.Errorf("LocalPath(a/b) = %q, want %q", got, want)
	}
	if got := b.LocalPath(""); got != dir {
		t.Errorf("LocalPath(\"\") = %q, want %q", got, dir)
	}
}

// Deleting the tree root would remove the whole configured directory.
func TestLocalRemoveRefusesBase(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := NewLocalBackend(dir)
	for _, in := range []string{"", ".", "/", "./"} {
		if err := b.RemoveAll(context.Background(), in); err == nil {
			t.Errorf("RemoveAll(%q) succeeded", in)
		}
		if err := b.Remove(context.Background(), in); err == nil {
			t.Errorf("Remove(%q) succeeded", in)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "f")); err != nil {
		t.Fatal("base directory content was removed")
	}
}
