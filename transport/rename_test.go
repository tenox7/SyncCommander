package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"sc/model"
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
	b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), false, 8)
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

// T1: ftp's Rename and Remove had no test. Most servers rename over a
// file, so Rename looks for the new name in the folder's live listing
// first (F28): a taken name is refused and both files stay; a free one is
// renamed. Remove sends one DELE.
func TestFTPRenameAndRemove(t *testing.T) {
	f := &fakeFTPD{
		ls: map[string][]string{
			"/srv":      {lsLine("base", true)},
			"/srv/base": {lsLine("a.txt", false), lsLine("b.txt", false)},
		},
		store: map[string][]byte{"/srv/base/a.txt": []byte("a"), "/srv/base/b.txt": []byte("b")},
	}
	b := openFakeFTP(t, f, "/srv/base")
	ctx := context.Background()
	stored := func() string {
		f.mu.Lock()
		defer f.mu.Unlock()
		return fmt.Sprint(f.store)
	}
	if err := b.Rename(ctx, "a.txt", "b.txt"); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Rename onto b.txt = %v, want fs.ErrExist", err)
	}
	if got, want := stored(), "map[/srv/base/a.txt:[97] /srv/base/b.txt:[98]]"; got != want {
		t.Errorf("after the refused rename the server holds %s, want %s", got, want)
	}
	if err := b.Rename(ctx, "a.txt", "c.txt"); err != nil {
		t.Errorf("Rename to c.txt = %v", err)
	}
	if err := b.Remove(ctx, "b.txt"); err != nil {
		t.Errorf("Remove(b.txt) = %v", err)
	}
	if got, want := stored(), "map[/srv/base/c.txt:[97]]"; got != want {
		t.Errorf("after the rename and the remove the server holds %s, want %s", got, want)
	}
	if err := b.Remove(ctx, "gone.txt"); err == nil {
		t.Error("Remove of a file that is not there = nil")
	}
}

// T1: a rename over the ssh shell (scp://) that goes through had no test:
// the file is renamed, the next listing shows the new name and not the
// old, also from the listings a scan's recursive preload keeps, and a
// taken name is refused.
func TestShellRenameRenames(t *testing.T) {
	base := filepath.Join(t.TempDir(), "a", "base")
	writeT(t, filepath.Join(base, "a.txt"), "AAA")
	writeT(t, filepath.Join(base, "b.txt"), "BBB")
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+filepath.ToSlash(base), true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	pr, ok := b.(model.RecursivePreloader)
	if !ok {
		t.Fatal("scp:// has no recursive preload")
	}
	if err := pr.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	names := func() string {
		es, err := b.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range es {
			out = append(out, e.Name)
		}
		slices.Sort(out) // a listing comes in no fixed order
		return fmt.Sprint(out)
	}
	if got := names(); got != "[a.txt b.txt]" {
		t.Fatalf("the base lists %s", got)
	}
	if err := b.Rename(ctx, "a.txt", "c.txt"); err != nil {
		t.Errorf("Rename to c.txt = %v", err)
	}
	if got := names(); got != "[b.txt c.txt]" {
		t.Errorf("after the rename the base lists %s, want [b.txt c.txt]", got)
	}
	if err := b.Rename(ctx, "b.txt", "c.txt"); err == nil {
		t.Error("Rename onto c.txt = nil, want it refused")
	}
	if readT(t, filepath.Join(base, "b.txt")) != "BBB" || readT(t, filepath.Join(base, "c.txt")) != "AAA" {
		t.Error("the files changed")
	}
}
