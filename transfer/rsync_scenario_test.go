package transfer

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// startRsyncd runs a stock rsync daemon with one writable module, backups.
func startRsyncd(t *testing.T) (url, moduleDir string) {
	t.Helper()
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	root := t.TempDir()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	moduleDir = filepath.Join(root, "module")
	if err := os.MkdirAll(moduleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(root, "rsyncd.conf")
	cfg := fmt.Sprintf("use chroot = no\nport = %d\n[backups]\n  path = %s\n  read only = no\n", port, moduleDir)
	if err := os.WriteFile(conf, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("rsync", "--daemon", "--no-detach", "--config="+conf, "--port="+fmt.Sprint(port), "--log-file="+filepath.Join(root, "d.log"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	for range 250 {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			return fmt.Sprintf("rsync://127.0.0.1:%d/backups", port), moduleDir
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("rsync daemon did not come up")
	return "", ""
}

// R3, end to end as the program runs it (lazy wrapper, deep scan): a folder
// the daemon cannot read is a failed listing, not an empty folder, so a
// mirror from the daemon keeps the local copy.
func TestScenarioRsyncUnreadableFolderMirror(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	url, mod := startRsyncd(t)
	local := t.TempDir()
	for _, side := range []string{mod, local} {
		writeFile(t, filepath.Join(side, "photos", "a.jpg"), "same on both sides")
		writeFile(t, filepath.Join(side, "photos", "b.jpg"), "same on both sides")
		writeFile(t, filepath.Join(side, "docs", "c.txt"), "same on both sides")
	}
	if err := os.Chmod(filepath.Join(mod, "photos"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(mod, "photos"), 0o755) })

	src, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	dst := transport.NewLocalBackend(local)
	sc := model.NewScanner(src, dst, 4, 8, true)
	sc.Scan(context.Background(), scenarioOpts)
	s := &scenario{right: local, lb: src, rb: dst, sc: sc}
	if !s.node(t, "photos").ListErr {
		t.Error("photos is not flagged as a failed listing")
	}

	for _, rel := range []string{"photos", ""} {
		if p := s.copy(t, rel, true, true); p.Failed.Load() == 0 {
			t.Errorf("mirror of %q from an unreadable folder did not fail", rel)
		}
	}
	wantTree(t, "local side after the mirrors", local, "docs/", "docs/c.txt", "photos/", "photos/a.jpg", "photos/b.jpg")
}
