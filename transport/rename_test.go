package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func writeT(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readT(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(b)
}

// F28: a rename never replaces the file or folder already at the target.
func TestLocalRenameRefusesTakenName(t *testing.T) {
	dir := t.TempDir()
	writeT(t, filepath.Join(dir, "a.txt"), "AAA")
	writeT(t, filepath.Join(dir, "b.txt"), "BBB")
	writeT(t, filepath.Join(dir, "d", "keep.txt"), "in a folder")
	b := NewLocalBackend(dir)
	ctx := context.Background()
	for _, target := range []string{"b.txt", "d"} {
		err := b.Rename(ctx, "a.txt", target)
		if !errors.Is(err, fs.ErrExist) {
			t.Errorf("rename a.txt -> %s: err=%v, want fs.ErrExist", target, err)
		}
	}
	if readT(t, filepath.Join(dir, "a.txt")) != "AAA" || readT(t, filepath.Join(dir, "b.txt")) != "BBB" || readT(t, filepath.Join(dir, "d", "keep.txt")) != "in a folder" {
		t.Error("a refused rename changed a file")
	}
	if err := b.Rename(ctx, "a.txt", "c.txt"); err != nil || readT(t, filepath.Join(dir, "c.txt")) != "AAA" {
		t.Errorf("rename to a free name: err=%v", err)
	}
	// A case-only rename on a case-insensitive disk targets the file itself.
	if _, err := os.Stat(filepath.Join(dir, "C.TXT")); err == nil {
		if err := b.Rename(ctx, "c.txt", "C.txt"); err != nil {
			t.Errorf("case-only rename: %v", err)
		}
	}
}

func TestFakeRenameRefusesTakenName(t *testing.T) {
	b, err := NewFakeBackend("fake://tiny")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	entries, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir {
			files = append(files, e.RelPath)
		}
	}
	if len(files) < 2 {
		t.Skip("fake tree has fewer than two top-level files")
	}
	if err := b.Rename(ctx, files[0], files[1]); !errors.Is(err, fs.ErrExist) {
		t.Errorf("rename onto an existing name: err=%v, want fs.ErrExist", err)
	}
}

// F28: the shell backends' rename command, run by a real sh.
func TestShellRenameCmdRefusesTakenName(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	run := func(src, dst string) error {
		return exec.Command(sh, "-c", renameCmd(filepath.Join(dir, src), filepath.Join(dir, dst))).Run()
	}
	writeT(t, filepath.Join(dir, "a.txt"), "AAA")
	writeT(t, filepath.Join(dir, "b.txt"), "BBB")
	writeT(t, filepath.Join(dir, "d", "keep.txt"), "in a folder")
	if err := os.Symlink(filepath.Join(dir, "nowhere"), filepath.Join(dir, "dangling")); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"b.txt", "d", "dangling"} {
		if run("a.txt", target) == nil {
			t.Errorf("mv a.txt %s ran", target)
		}
	}
	if readT(t, filepath.Join(dir, "a.txt")) != "AAA" || readT(t, filepath.Join(dir, "b.txt")) != "BBB" {
		t.Error("a refused rename changed a file")
	}
	if _, err := os.Stat(filepath.Join(dir, "d", "a.txt")); err == nil {
		t.Error("a.txt was moved into the folder d")
	}
	if err := run("a.txt", "c.txt"); err != nil || readT(t, filepath.Join(dir, "c.txt")) != "AAA" {
		t.Errorf("rename to a free name: err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "C.TXT")); err == nil {
		if err := run("c.txt", "C.txt"); err != nil {
			t.Errorf("case-only rename: %v", err)
		}
	}
}

// F28: WebDAV MOVE is sent without Overwrite, so the server keeps a folder
// at the target instead of deleting it with all it holds.
func TestWebDAVRenameRefusesTakenName(t *testing.T) {
	root := t.TempDir()
	writeT(t, filepath.Join(root, "a.txt"), "AAA")
	writeT(t, filepath.Join(root, "b.txt"), "BBB")
	writeT(t, filepath.Join(root, "d", "keep.txt"), "in a folder")
	port, _ := startRcloneWebDAV(t, root)
	b, err := NewWebDAVBackend(fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, target := range []string{"b.txt", "d"} {
		if err := b.Rename(ctx, "a.txt", target); err == nil {
			t.Errorf("rename a.txt -> %s succeeded", target)
		}
	}
	if readT(t, filepath.Join(root, "b.txt")) != "BBB" || readT(t, filepath.Join(root, "d", "keep.txt")) != "in a folder" {
		t.Error("a refused rename replaced the target")
	}
	if err := b.Rename(ctx, "a.txt", "c.txt"); err != nil || readT(t, filepath.Join(root, "c.txt")) != "AAA" {
		t.Errorf("rename to a free name: err=%v", err)
	}
}
