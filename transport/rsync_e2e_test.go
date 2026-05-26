package transport

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func startRsyncDaemon(t *testing.T) (port int, moduleDir string) {
	t.Helper()
	root := t.TempDir()
	moduleDir = filepath.Join(root, "module")
	if err := os.MkdirAll(moduleDir, 0755); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port = l.Addr().(*net.TCPAddr).Port
	l.Close()

	conf := filepath.Join(root, "rsyncd.conf")
	cfg := fmt.Sprintf("use chroot = no\nport = %d\n[testmod]\n  path = %s\n  read only = no\n",
		port, moduleDir)
	if err := os.WriteFile(conf, []byte(cfg), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("rsync", "--daemon", "--no-detach", "--config="+conf,
		"--port="+fmt.Sprint(port), "--log-file="+filepath.Join(root, "d.log"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if derr == nil {
			c.Close()
			return port, moduleDir
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("rsync daemon did not come up")
	return 0, ""
}

func TestRsyncRemoteDelete(t *testing.T) {
	port, moduleDir := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	seed := map[string]string{
		"Gedit.tmTheme": "theme",
		"keep1.txt":     "a",
		"keep2.csv":     "b",
		"sub/keep3.txt": "c",
		"sub/gone.txt":  "d",
	}
	for rel, body := range seed {
		full := filepath.Join(moduleDir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}

	exists := func(rel string) bool {
		_, err := os.Stat(filepath.Join(moduleDir, rel))
		return err == nil
	}

	// Root-level single-file delete: the reported repro. The target must go;
	// every sibling (and the sibling subtree) must survive.
	if err := b.Remove(ctx, "Gedit.tmTheme"); err != nil {
		t.Fatalf("Remove root file: %v", err)
	}
	if exists("Gedit.tmTheme") {
		t.Fatal("Gedit.tmTheme was not deleted")
	}
	for _, rel := range []string{"keep1.txt", "keep2.csv", "sub/keep3.txt", "sub/gone.txt"} {
		if !exists(rel) {
			t.Fatalf("Remove deleted unrelated path %q", rel)
		}
	}

	// File inside a subdirectory: delete one entry, keep its sibling.
	if err := b.Remove(ctx, "sub/gone.txt"); err != nil {
		t.Fatalf("Remove subdir file: %v", err)
	}
	if exists("sub/gone.txt") {
		t.Fatal("sub/gone.txt was not deleted")
	}
	if !exists("sub/keep3.txt") {
		t.Fatal("Remove deleted sibling sub/keep3.txt")
	}

	// RemoveAll on a directory: the subtree goes, root siblings stay.
	if err := b.RemoveAll(ctx, "sub"); err != nil {
		t.Fatalf("RemoveAll subdir: %v", err)
	}
	if exists("sub") {
		t.Fatal("sub/ was not removed")
	}
	for _, rel := range []string{"keep1.txt", "keep2.csv"} {
		if !exists(rel) {
			t.Fatalf("RemoveAll deleted unrelated path %q", rel)
		}
	}
}

func TestRsyncDeepUploadIntoEmptyModule(t *testing.T) {
	port, moduleDir := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// SendLocalFile: the reported repro — deep relPath, empty destination tree.
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "baz.tgz")
	if err := os.WriteFile(srcFile, []byte("hello-send"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := b.SendLocalFile(ctx, srcFile, "foo/bar/baz.tgz", 0644); err != nil {
		t.Fatalf("SendLocalFile deep path: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(moduleDir, "foo/bar/baz.tgz"))
	if err != nil || string(got) != "hello-send" {
		t.Fatalf("SendLocalFile: want hello-send, got %q err %v", got, err)
	}

	// CopyFrom: streamed deep path into another empty subtree.
	if err := b.CopyFrom(ctx, "a/b/c/qux.txt", strings.NewReader("hello-copy"), 0644); err != nil {
		t.Fatalf("CopyFrom deep path: %v", err)
	}
	got, err = os.ReadFile(filepath.Join(moduleDir, "a/b/c/qux.txt"))
	if err != nil || string(got) != "hello-copy" {
		t.Fatalf("CopyFrom: want hello-copy, got %q err %v", got, err)
	}

	// Root-level file still works (no leading dirs).
	if err := b.CopyFrom(ctx, "top.txt", strings.NewReader("root"), 0644); err != nil {
		t.Fatalf("CopyFrom root: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(moduleDir, "top.txt")); string(got) != "root" {
		t.Fatalf("CopyFrom root: want root, got %q", got)
	}
}
