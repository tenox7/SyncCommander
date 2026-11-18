package transport

import (
	"context"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"sc/internal/sshtest"
	"sc/model"
)

// R16: rsync+ssh ran mkdir -p in an exec of its own before each file it
// sent: a round trip per file. A folder that a send made or went into, or
// that a Mkdir made, needs none now. One removed or renamed is made again,
// and so is one removed from elsewhere, by the send that finds it gone.
func TestRsyncSSHMakesEachFolderOnce(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	var mu sync.Mutex
	mkdirs, sessions := 0, 0
	addr := sshtest.StartWith(t, sshtest.Options{Hold: func(cmd string) bool {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasPrefix(cmd, "mkdir -p "):
			mkdirs++
		case strings.HasPrefix(cmd, "rsync "):
			sessions++
		}
		return false
	}})
	made := func() int {
		mu.Lock()
		defer mu.Unlock()
		return mkdirs
	}
	mod := t.TempDir()
	b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+addr+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	ctx := context.Background()
	src := t.TempDir()
	p := model.PutParams{Size: 3, Mode: 0o644}
	send := func(rel string) error { // a local file of the same name, as a copy sends
		writeTestFile(t, src, path.Base(rel), "new")
		return b.SendLocalFile(ctx, filepath.Join(src, path.Base(rel)), rel, p)
	}
	step := func(what string, want int, do func() error) {
		t.Helper()
		mark := made()
		if err := do(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if n := made() - mark; n != want {
			t.Errorf("%s ran %d mkdir commands, want %d", what, n, want)
		}
	}
	step("the first send into a/b", 1, func() error { return send("a/b/f1.txt") })
	step("a second send into a/b", 0, func() error { return send("a/b/f2.txt") })
	step("an upload into a/b", 0, func() error { return b.CopyFrom(ctx, "a/b/f3.txt", strings.NewReader("new"), p) })
	step("a send into a, above a/b", 0, func() error { return send("a/f4.txt") })
	step("a Mkdir of c", 1, func() error { return b.Mkdir(ctx, "c", 0o755) })
	step("a send into c", 0, func() error { return send("c/f5.txt") })
	step("a RemoveAll of a", 0, func() error { return b.RemoveAll(ctx, "a") })
	step("a send into a/b after it", 1, func() error { return send("a/b/f6.txt") })
	step("a send into c after it", 1, func() error { return send("c/f7.txt") })
	step("a rename of c to d", 0, func() error { return b.Rename(ctx, "c", "d") })
	step("a send into c after it", 1, func() error { return send("c/f8.txt") })
	for _, rel := range []string{"a/b/f6.txt", "d/f5.txt", "d/f7.txt", "c/f8.txt"} {
		if got, err := os.ReadFile(filepath.Join(mod, rel)); err != nil || string(got) != "new" {
			t.Errorf("%s there = %q, %v", rel, got, err)
		}
	}
	// Removed behind sc's back with the folder above it, so rsync cannot
	// make it either: the send makes it and sends again, and progress
	// counts the file once.
	step("a send into e/f", 1, func() error { return send("e/f/g1.txt") })
	if err := os.RemoveAll(filepath.Join(mod, "e")); err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("b", 300<<10)
	writeTestFile(t, src, "g2.txt", big)
	var counter atomic.Int64
	err = b.SendLocalFile(ContextWithProgress(ctx, &counter), filepath.Join(src, "g2.txt"), "e/f/g2.txt", model.PutParams{Size: int64(len(big)), Mode: 0o644})
	if err != nil {
		t.Errorf("a send into e/f, removed behind sc's back: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(mod, "e/f/g2.txt")); err != nil || string(got) != big {
		t.Errorf("e/f/g2.txt there: %d bytes, %v; want %d", len(got), err, len(big))
	}
	if n := counter.Load(); n != int64(len(big)) {
		t.Errorf("progress counted %d bytes for a file of %d", n, len(big))
	}
	// A folder that is there, where a send fails for another reason: no
	// second send.
	if os.Geteuid() == 0 {
		return // root writes into a folder it may not
	}
	step("a send into ro", 1, func() error { return send("ro/h1.txt") })
	if err := os.Chmod(filepath.Join(mod, "ro"), 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(mod, "ro"), 0o755) })
	mu.Lock()
	sessions = 0
	mu.Unlock()
	if err := send("ro/h2.txt"); err == nil {
		t.Error("a send into a folder it may not write into went through")
	}
	mu.Lock()
	n := sessions
	mu.Unlock()
	if n != 1 {
		t.Errorf("a send that failed in a folder that is there ran %d rsync sessions, want 1", n)
	}
}
