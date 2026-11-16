package transport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// R5: the daemon password stays out of error text and the log. Transfers
// ran gorsync's command line with the password in its rsync:// URL, and
// gorsync's first stderr line repeats that command line; sc put all of
// stderr into the error, which went to the log view and to saved log files.
func TestRsyncDaemonPasswordStaysSecret(t *testing.T) {
	const secret = "Sekr1t:R5@p%w#?"
	port, root := startRsyncDaemonConf(t, func(root string) string {
		secrets := filepath.Join(root, "secrets")
		if err := os.WriteFile(secrets, []byte("carol:"+secret+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return "[priv]\n  path = " + filepath.Join(root, "priv") + "\n  read only = no\n  auth users = carol\n  secrets file = " + secrets + "\n"
	})
	mod := filepath.Join(root, "priv")
	open := func(pass, module string) *RsyncBackend {
		b, err := NewRsyncBackend(fmt.Sprintf("rsync://carol:%s@127.0.0.1:%d/%s", pass, port, module))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	ctx := context.Background()
	mark := len(Log.Lines())
	leaks := func(what string, err error, want string) {
		t.Helper()
		switch {
		case err == nil:
			t.Errorf("%s: no error", what)
		case strings.Contains(err.Error(), "Sekr1t"):
			t.Errorf("%s: the error holds the password: %v", what, err)
		case !strings.Contains(err.Error(), want):
			t.Errorf("%s: %v; want the error to say %q", what, err, want)
		}
	}

	// The password works, odd characters and all.
	b := open(secret, "priv")
	if err := b.CopyFrom(ctx, "dir/up.txt", strings.NewReader("up"), 0o644); err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(mod, "dir", "up.txt")); string(got) != "up" {
		t.Fatalf("CopyFrom: the server holds %q", got)
	}
	dst := filepath.Join(t.TempDir(), "up.txt")
	if err := b.RecvToLocalFile(ctx, "dir/up.txt", dst); err != nil {
		t.Fatalf("RecvToLocalFile: %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "up" {
		t.Fatalf("RecvToLocalFile: got %q", got)
	}
	if err := b.Mkdir(ctx, "made", 0o755); err != nil || !isDir(mod, "made") {
		t.Fatalf("Mkdir: %v, folder there=%v", err, isDir(mod, "made"))
	}
	if err := b.Remove(ctx, "dir/up.txt"); err != nil || exists(mod, "dir/up.txt") {
		t.Fatalf("Remove: %v, still there=%v", err, exists(mod, "dir/up.txt"))
	}

	// Failures say what went wrong, without the password.
	writeTestFile(t, mod, "report/2024.txt", "a folder that holds a file")
	err := b.CopyFrom(ctx, "report", strings.NewReader("a file named report"), 0o644)
	leaks("CopyFrom over a folder that holds files", err, "report")
	leaks("CopyFrom to a module that is not there", open(secret, "nosuchmod").CopyFrom(ctx, "x.txt", strings.NewReader("x"), 0o644), "nosuchmod")
	leaks("Mkdir with a wrong password", open("Sekr1t-wrong", "priv").Mkdir(ctx, "other", 0o755), "auth failed")
	leaks("RecvToLocalFile of a file that is not there", b.RecvToLocalFile(ctx, "missing.txt", filepath.Join(t.TempDir(), "missing.txt")), "missing.txt")

	for _, line := range Log.Lines()[mark:] {
		if strings.Contains(line, "Sekr1t") {
			t.Errorf("the log holds the password: %s", line)
		}
	}
}

// An upload to a read-only module stops the whole operation: every other
// upload would fail the same way. The daemon says so in a message to the
// client, which sc now reads from the server's messages, not from all of
// gorsync's stderr. rsync 3.1.3 on Linux resets the connection before that
// message gets out, for its own client too ("read error: Connection reset
// by peer"); then the upload only fails.
func TestRsyncReadOnlyModuleAbortsOperation(t *testing.T) {
	port, _ := startRsyncDaemonConf(t, func(root string) string {
		return "[ro]\n  path = " + filepath.Join(root, "ro") + "\n  read only = yes\n"
	})
	stock, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/ro", port))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	gorsyncd := startGorsyncdModule(t, dir, false)
	gb, err := NewRsyncBackend(gorsyncd)
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string]*RsyncBackend{"stock rsync": stock, "gorsyncd": gb} {
		ctx, cancel := context.WithCancel(context.Background())
		ctx = ContextWithFatalCancel(ctx, cancel)
		err := b.CopyFrom(ctx, "x.txt", strings.NewReader("x"), 0o644)
		switch {
		case err == nil:
			t.Errorf("%s: CopyFrom to a read-only module: no error", name)
		case !strings.Contains(err.Error(), "module is read only"):
			if name == "gorsyncd" {
				t.Errorf("%s: CopyFrom to a read-only module: %v; want an error saying the module is read only", name, err)
			} else {
				t.Logf("%s: the daemon's message did not arrive: %v", name, err)
			}
		case ctx.Err() == nil:
			t.Errorf("%s: a read-only module did not abort the operation", name)
		}
		cancel()
	}
}
