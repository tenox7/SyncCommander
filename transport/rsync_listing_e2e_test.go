package transport

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sc/internal/sshtest"
)

// R3: a folder the daemon cannot read must fail its listing, through both
// the one-level listing and the recursive preload. The preload keeps the
// folders it did list in full.
func TestRsyncListingUnreadableFolder(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	port, mod := startRsyncDaemon(t)
	for _, p := range []string{"photos/a.jpg", "photos/b.jpg", "docs/c.txt", "deep/x/y.txt"} {
		full := filepath.Join(mod, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(mod, "photos"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(mod, "photos"), 0o755) })
	ctx := context.Background()
	names := func(b *RsyncBackend, dir string) ([]string, error) {
		entries, err := b.List(ctx, dir)
		var out []string
		for _, e := range entries {
			out = append(out, e.Name)
		}
		slices.Sort(out)
		return out, err
	}
	url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port)

	b, err := NewRsyncBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := names(b, "photos"); err == nil {
		t.Errorf("one-level listing of an unreadable folder = %q, no error", got)
	}

	b, _ = NewRsyncBackend(url)
	mark := len(Log.Lines())
	b.PreloadRecursive(ctx, "")
	if got, err := names(b, "photos"); err == nil {
		t.Errorf("preloaded listing of an unreadable folder = %q, no error", got)
	}
	for dir, want := range map[string][]string{"": {"deep", "docs", "photos"}, "docs": {"c.txt"}, "deep/x": {"y.txt"}} {
		if got, err := names(b, dir); err != nil || !slices.Equal(got, want) {
			t.Errorf("List(%q) = %q, %v; want %q", dir, got, err, want)
		}
	}
	var live []string
	for _, line := range Log.Lines()[mark:] {
		t.Log(line)
		if strings.Contains(line, ">>> LIST testmod/") {
			live = append(live, line)
		}
	}
	// Only the folders the daemon named are listed again, live: photos and
	// its parent.
	if len(live) > 4 {
		t.Errorf("%d live listings after the preload, want photos and the top only:\n%s", len(live), strings.Join(live, "\n"))
	}
}

// startExecSSHServer runs an in-process sshd that runs every exec request
// with sh -c on this machine, so ssh backends drive the real rsync, find
// and perl as on a remote host. It has no SFTP, so ssh:// and scp:// get
// the shell backend.
func startExecSSHServer(t *testing.T) string { return startShellServer(t, false) }

// startExecSFTPServer is startExecSSHServer with the sftp subsystem too, as
// a stock sshd has it.
func startExecSFTPServer(t *testing.T) string { return startShellServer(t, true) }

func startShellServer(t *testing.T, withSFTP bool) string { return sshtest.Start(t, withSFTP) }

// R3 over ssh: the remote rsync names the unreadable folder by its absolute
// path; only that folder and its parent are listed again.
func TestRsyncSSHListingUnreadableFolder(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	root := t.TempDir()
	for _, p := range []string{"photos/a.jpg", "docs/c.txt", "deep/x/y.txt"} {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(root, "photos"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(root, "photos"), 0o755) })
	b, err := NewRsyncSSHBackend("rsync+ssh://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	mark := len(Log.Lines())
	b.PreloadRecursive(ctx, "")
	if entries, err := b.List(ctx, "photos"); err == nil {
		t.Errorf("List(photos) = %d entries and no error; the folder cannot be read", len(entries))
	}
	for dir, want := range map[string]int{"": 3, "docs": 1, "deep/x": 1} {
		if entries, err := b.List(ctx, dir); err != nil || len(entries) != want {
			t.Errorf("List(%q) = %d entries, %v; want %d", dir, len(entries), err, want)
		}
	}
	// photos and its parent are listed again, live; docs and deep/x come
	// from the preload.
	var livePhotos bool
	for _, line := range Log.Lines()[mark:] {
		if !strings.Contains(line, ">>> ") || strings.Contains(line, "RLIST") {
			continue
		}
		livePhotos = livePhotos || strings.Contains(line, "/photos'")
		if strings.Contains(line, "/docs") || strings.Contains(line, "/deep") {
			t.Errorf("listed live although the preload read it in full: %s", line)
		}
	}
	if !livePhotos {
		t.Error("photos was served from the preload")
	}
}

// R3 with a base folder in the URL: the daemon names paths from the module
// root, which must map back under the base.
func TestRsyncListingUnreadableFolderUnderBase(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	port, mod := startRsyncDaemon(t)
	for _, p := range []string{"in/photos/a.jpg", "in/docs/c.txt"} {
		full := filepath.Join(mod, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(mod, "in", "photos"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(mod, "in", "photos"), 0o755) })
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod/in", port))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	mark := len(Log.Lines())
	b.PreloadRecursive(ctx, "")
	if _, err := b.List(ctx, "photos"); err == nil {
		t.Error("preloaded listing of an unreadable folder: no error")
	}
	if entries, err := b.List(ctx, "docs"); err != nil || len(entries) != 1 {
		t.Errorf("List(docs) = %d entries, %v", len(entries), err)
	}
	for _, line := range Log.Lines()[mark:] {
		t.Log(line)
		if strings.Contains(line, ">>> LIST testmod/in/docs/") {
			t.Error("docs listed live although the preload read it in full")
		}
	}
}
