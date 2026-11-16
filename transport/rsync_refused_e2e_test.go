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
	"time"

	"sc/model"
)

// refusable is what the R4 cases call on both rsync backends.
type refusable interface {
	CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error
	SendLocalFile(ctx context.Context, srcPath, relPath string, p model.PutParams) error
	RecvToLocalFile(ctx context.Context, relPath, dstPath string) error
	Open(ctx context.Context, relPath string) (io.ReadCloser, error)
	Mkdir(ctx context.Context, relPath string, mode os.FileMode) error
	Remove(ctx context.Context, relPath string) error
	RemoveAll(ctx context.Context, relPath string) error
}

// R4: a transfer the server refuses, or a local file the client cannot
// send, fails the call. sc used to report these as done: a download the
// daemon could not read left the old local file in place, and the engine
// then stamped the source mtime on it.
func TestRsyncDaemonRefusedTransfersFail(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes and reads everything")
	}
	port, mod := startRsyncDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	checkRefusedTransfers(t, b, mod)
}

func TestGorsyncdRefusedTransfersFail(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes and reads everything")
	}
	url, mod := startGorsyncd(t)
	b, err := NewRsyncBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	checkRefusedTransfers(t, b, mod)
}

func TestRsyncSSHRefusedTransfersFail(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes and reads everything")
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	mod := t.TempDir()
	b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+startExecSSHServer(t)+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	checkRefusedTransfers(t, b, mod)

	// A tree with a file the client cannot read goes in one rsync run.
	src := t.TempDir()
	for _, rel := range []string{"tree/a.txt", "tree/secret.txt", "tree/z.txt"} {
		writeTestFile(t, src, rel, rel)
	}
	chmodTest(t, filepath.Join(src, "tree/secret.txt"), 0o000)
	err = b.SendLocalTree(context.Background(), src, "tree", nil)
	if err == nil && !(exists(mod, "tree/a.txt") && exists(mod, "tree/secret.txt") && exists(mod, "tree/z.txt")) {
		t.Errorf("SendLocalTree with a file the client cannot read: no error; on the server a=%v secret=%v z=%v", exists(mod, "tree/a.txt"), exists(mod, "tree/secret.txt"), exists(mod, "tree/z.txt"))
	}
}

// checkRefusedTransfers runs calls that the server or the client may
// refuse. Each one either fails or did what it was asked: success that
// did not happen is the bug.
func checkRefusedTransfers(t *testing.T, b refusable, mod string) {
	ctx := context.Background()
	writeTestFile(t, mod, "report/2024.txt", "inside the folder report")
	writeTestFile(t, mod, "unreadable.bin", "REMOTE NEW VERSION")
	writeTestFile(t, mod, "locked.bin", "OLD CONTENT")
	writeTestFile(t, mod, "ro/old.txt", "old")
	writeTestFile(t, mod, "ro/sub/x.txt", "x")
	chmodTest(t, filepath.Join(mod, "unreadable.bin"), 0o000)
	chmodTest(t, filepath.Join(mod, "locked.bin"), 0o444)
	chmodTest(t, filepath.Join(mod, "ro/sub"), 0o555)
	chmodTest(t, filepath.Join(mod, "ro"), 0o555)
	content := func(root, rel string) string {
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return "<" + err.Error() + ">"
		}
		return string(got)
	}
	check := func(what string, err error, done bool, state string) {
		t.Helper()
		if err == nil && !done {
			t.Errorf("%s: no error, but it did not happen: %s", what, state)
		}
	}

	err := b.CopyFrom(ctx, "report", strings.NewReader("a file named report"), model.PutParams{Mode: 0o644})
	check("CopyFrom(report) over a folder that holds files", err, content(mod, "report") == "a file named report", fmt.Sprintf("still a folder=%v", isDir(mod, "report")))
	err = b.CopyFrom(ctx, "locked.bin", strings.NewReader("NEW CONTENT"), model.PutParams{Mode: 0o644})
	check("CopyFrom(locked.bin) over a read-only file", err, content(mod, "locked.bin") == "NEW CONTENT", "server holds "+content(mod, "locked.bin"))
	err = b.CopyFrom(ctx, "ro/new.txt", strings.NewReader("new"), model.PutParams{Mode: 0o644})
	check("CopyFrom(ro/new.txt) into a read-only folder", err, content(mod, "ro/new.txt") == "new", "server holds "+content(mod, "ro/new.txt"))
	err = b.Mkdir(ctx, "ro/newdir", 0o755)
	check("Mkdir(ro/newdir) in a read-only folder", err, isDir(mod, "ro/newdir"), "no folder")
	err = b.Remove(ctx, "ro/old.txt")
	check("Remove(ro/old.txt) in a read-only folder", err, !exists(mod, "ro/old.txt"), "still there")
	err = b.RemoveAll(ctx, "ro/sub")
	check("RemoveAll(ro/sub) in a read-only folder", err, !exists(mod, "ro/sub"), "still there")

	local := t.TempDir()
	dst := filepath.Join(local, "unreadable.bin")
	writeTestFile(t, local, "unreadable.bin", "LOCAL OLD VERSION")
	err = b.RecvToLocalFile(ctx, "unreadable.bin", dst)
	check("RecvToLocalFile of a file the server cannot read", err, content(local, "unreadable.bin") == "REMOTE NEW VERSION", "local file holds "+content(local, "unreadable.bin"))
	if r, err := b.Open(ctx, "unreadable.bin"); err == nil {
		got, _ := io.ReadAll(r)
		r.Close()
		check("Open of a file the server cannot read", nil, string(got) == "REMOTE NEW VERSION", fmt.Sprintf("read %q", got))
	}

	writeTestFile(t, local, "secret.txt", "client cannot read this")
	chmodTest(t, filepath.Join(local, "secret.txt"), 0o000)
	err = b.SendLocalFile(ctx, filepath.Join(local, "secret.txt"), "up/secret.txt", model.PutParams{Mode: 0o644})
	check("SendLocalFile of a file the client cannot read", err, content(mod, "up/secret.txt") == "client cannot read this", "server holds "+content(mod, "up/secret.txt"))

	// An upload leaves the dates of the folders above it alone: sc never
	// sends folder times, which the server may not let it set.
	writeTestFile(t, mod, "a/b/old.txt", "old")
	old := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(mod, "a"), old, old); err != nil {
		t.Fatal(err)
	}
	if err := b.CopyFrom(ctx, "a/b/new.txt", strings.NewReader("new"), model.PutParams{Mode: 0o644}); err != nil {
		t.Errorf("CopyFrom(a/b/new.txt): %v", err)
	}
	if fi, err := os.Stat(filepath.Join(mod, "a")); err != nil || !fi.ModTime().Equal(old) {
		t.Errorf("after an upload into a/b, the date of a changed: %v, %v; want %v", fi.ModTime(), err, old)
	}
}

func writeTestFile(t *testing.T, root, rel, data string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

// chmodTest sets mode on path and puts 0755 back when the test ends, so
// the temp folders can be removed.
func chmodTest(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0o755) })
}

func exists(root, rel string) bool {
	_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel)))
	return err == nil
}

func isDir(root, rel string) bool {
	fi, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel)))
	return err == nil && fi.IsDir()
}

// The remote rsync's exit status counts too: rsync exits non-zero for
// errors that need not reach the client in the protocol. Here a wrapper
// runs the real rsync and then exits 23.
func TestRsyncSSHRemoteExitStatusFails(t *testing.T) {
	real, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("no rsync binary")
	}
	bin := t.TempDir()
	script := "#!/bin/sh\n" + shellQuote(real) + " \"$@\"\nexit 23\n"
	if err := os.WriteFile(filepath.Join(bin, "rsync"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	mod := t.TempDir()
	writeTestFile(t, mod, "f.txt", "data")
	b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+startExecSSHServer(t)+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	dst := filepath.Join(t.TempDir(), "f.txt")
	if err := b.RecvToLocalFile(ctx, "f.txt", dst); err == nil || !strings.Contains(err.Error(), "23") {
		t.Errorf("RecvToLocalFile with the remote rsync exiting 23: %v, want an error naming status 23", err)
	}
	if err := b.CopyFrom(ctx, "g.txt", strings.NewReader("up"), model.PutParams{Mode: 0o644}); err == nil {
		t.Error("CopyFrom with the remote rsync exiting 23: no error")
	}
}
