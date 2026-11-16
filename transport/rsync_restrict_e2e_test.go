package transport

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
)

// R6: on Linux a gorsync client that is not told otherwise puts the whole
// process in a Landlock sandbox that allows only the folders of that one
// run, and a sandbox cannot be lifted. After sc's first rsync+ssh download
// it could no longer make temp folders or read any other folder. Without
// Landlock (macOS) this passes either way. A failure leaves the test binary
// sandboxed, so later tests in it fail too.
func TestRsyncRunsLeaveProcessUnrestricted(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	other := t.TempDir() // no run touches this folder
	writeTestFile(t, other, "keep.txt", "keep")
	check := func(after string) {
		t.Helper()
		d, err := os.MkdirTemp("", "r6-*")
		if err != nil {
			t.Fatalf("after %s: making a temp folder: %v", after, err)
		}
		os.Remove(d)
		if _, err := os.ReadFile(filepath.Join(other, "keep.txt")); err != nil {
			t.Fatalf("after %s: reading a folder the run did not touch: %v", after, err)
		}
	}
	ctx := context.Background()

	mod := t.TempDir()
	writeTestFile(t, mod, "f.txt", "data")
	sb, err := NewRsyncSSHBackend("rsync+ssh://x:pw@"+startExecSSHServer(t)+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	if err := sb.RecvToLocalFile(ctx, "f.txt", filepath.Join(t.TempDir(), "f.txt")); err != nil {
		t.Fatalf("rsync+ssh RecvToLocalFile: %v", err)
	}
	check("an rsync+ssh download")
	r, err := sb.Open(ctx, "f.txt")
	if err != nil {
		t.Fatalf("rsync+ssh Open: %v", err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "data" {
		t.Fatalf("rsync+ssh Open read %q", got)
	}
	check("an rsync+ssh Open")
	if err := sb.CopyFrom(ctx, "up.txt", strings.NewReader("up"), model.PutParams{Mode: 0o644}); err != nil {
		t.Fatalf("rsync+ssh CopyFrom: %v", err)
	}
	check("an rsync+ssh upload")

	port, dmod := startRsyncDaemon(t)
	writeTestFile(t, dmod, "g.txt", "daemon")
	db, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CopyFrom(ctx, "dir/up.txt", strings.NewReader("up"), model.PutParams{Mode: 0o644}); err != nil {
		t.Fatalf("rsync:// CopyFrom: %v", err)
	}
	check("an rsync:// upload")
	if err := db.Mkdir(ctx, "made", 0o755); err != nil {
		t.Fatalf("rsync:// Mkdir: %v", err)
	}
	check("an rsync:// mkdir")
	if err := db.Remove(ctx, "dir/up.txt"); err != nil {
		t.Fatalf("rsync:// Remove: %v", err)
	}
	check("an rsync:// delete")
	if err := db.RecvToLocalFile(ctx, "g.txt", filepath.Join(t.TempDir(), "g.txt")); err != nil {
		t.Fatalf("rsync:// RecvToLocalFile: %v", err)
	}
	check("an rsync:// download")
}
