package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// startRcloneS3 serves root over S3 with no auth, which is enough to exercise
// the bucket-backend paths: flat ListR, etag checksums, server modtimes.
func startRcloneS3(t *testing.T, root string) int {
	t.Helper()
	bin := rcloneBin(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(bin, "serve", "s3", root, "--addr", fmt.Sprintf("127.0.0.1:%d", port))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait() }) })

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c, derr := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); derr == nil {
			c.Close()
			time.Sleep(100 * time.Millisecond)
			return port
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rclone serve s3 did not come up")
	return 0
}

func TestRcloneS3E2E(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "bkt/sub/deep"), 0755)
	os.WriteFile(filepath.Join(root, "bkt/a.txt"), []byte("hello world\n"), 0644)
	os.WriteFile(filepath.Join(root, "bkt/sub/b.txt"), []byte("bbbb\n"), 0644)
	os.WriteFile(filepath.Join(root, "bkt/sub/deep/c.txt"), []byte("cccc\n"), 0644)
	port := startRcloneS3(t, root)

	url := fmt.Sprintf("rclone://:s3,provider=Other,endpoint='http://127.0.0.1:%d',"+
		"access_key_id=k,secret_access_key=s,force_path_style=true:bkt", port)

	b, err := OpenBackend(url, false, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if strings.Contains(b.BasePath(), "secret_access_key=s") {
		t.Errorf("secret leaked into BasePath: %s", b.BasePath())
	}

	ents, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 2 {
		t.Fatalf("root: got %d entries %+v", len(ents), ents)
	}
	for _, e := range ents {
		if e.Name == "a.txt" && (e.Size != 12 || e.IsDir || e.ModTime.IsZero()) {
			t.Errorf("a.txt bad: %+v", e)
		}
		if e.Name == "sub" && !e.IsDir {
			t.Errorf("sub not a dir: %+v", e)
		}
	}

	// ListR returns a flat object list with no directories at all, so the
	// preload has to invent every intermediate one.
	if err := b.(model.RecursivePreloader).PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for dir, want := range map[string]int{"": 2, "sub": 2, "sub/deep": 1} {
		e, err := b.List(ctx, dir)
		if err != nil {
			t.Fatalf("%q: %v", dir, err)
		}
		if len(e) != want {
			t.Errorf("preloaded %q: got %d want %d (%+v)", dir, len(e), want, e)
		}
	}

	if got := b.(model.ChecksumProber).ProbeChecksums(); len(got) == 0 || got[0] != "md5" {
		t.Errorf("checksums: %v", got)
	}
	b.(model.ChecksumProber).SetChecksumAlgo("md5")
	sum, err := b.Checksum(ctx, "a.txt")
	if err != nil || sum != "6f5902ac237024bdd0c176cb93063dc4" {
		t.Errorf("md5: %q %v", sum, err)
	}

	rd, err := b.Open(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rd)
	rd.Close()
	if string(got) != "hello world\n" {
		t.Fatalf("open: %q", got)
	}
	rd, err = b.(model.SeekableOpener).OpenAt(ctx, "a.txt", 6)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(rd)
	rd.Close()
	if string(got) != "world\n" {
		t.Fatalf("openat: %q", got)
	}

	if err := b.CopyFrom(ctx, "new/dir/up.txt", sizedRd{Reader: strings.NewReader("uploaded"), n: 8}, 0644); err != nil {
		t.Fatal(err)
	}
	rd, _ = b.Open(ctx, "new/dir/up.txt")
	got, _ = io.ReadAll(rd)
	rd.Close()
	if string(got) != "uploaded" {
		t.Fatalf("roundtrip: %q", got)
	}
	// rclone serve s3 has no server-side copy, so this exercises the
	// copy-then-delete fallback in operations.Move.
	if err := b.Rename(ctx, "new/dir/up.txt", "new/dir/moved.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt/new/dir/moved.txt")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := b.RemoveAll(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt/new")); !os.IsNotExist(err) {
		t.Fatalf("purge left %v", err)
	}
}
