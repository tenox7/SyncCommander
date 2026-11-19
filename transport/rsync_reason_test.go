package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"sc/model"
)

// R18: the words an error is final by were read in all of its text, names
// included: a passing failure on a file or folder named "no such file" was
// final, never tried again. They are read in each message's reason now,
// the words after its last ": " outside quotes.
func TestRetryReadsTheReason(t *testing.T) {
	failed := errors.New("rsync: some files could not be transferred (code 23)")
	for _, c := range []struct {
		what  string
		err   error
		final bool
	}{
		{"a name holding the words, a lost connection",
			fmt.Errorf("RECV %s: %w", "d/no such file/x.txt", syscall.ECONNRESET), false},
		{"the daemon names such a file, an I/O error",
			fmt.Errorf("%w: %s", errors.New("read tcp 127.0.0.1:1->127.0.0.1:2: read: connection reset by peer"), `[sender] read errors mapping "/m/permission denied.txt" (in m): Input/output error (5)`), false},
		{"a quoted name holding \": \" and \"; \"",
			errors.New(`[sender] read errors mapping "/m/a: no such file; b" (in m): Input/output error (5)`), false},
		{"joined errors, the words in names only",
			errors.Join(errors.New("stat d/does not exist: i/o timeout"), errors.New("list d: connection reset by peer")), false},
		{"a module name",
			fmt.Errorf("%s: %w", "unknown module", io.ErrUnexpectedEOF), false},
		{"a folder name",
			errors.New("copy d/not supported/x: i/o timeout"), false},
		{"a file name, a timeout",
			fmt.Errorf("SEND %s: %w", "operation not permitted.txt", os.ErrDeadlineExceeded), false},

		{"the daemon's reason",
			fmt.Errorf("%w: %s", failed, `[receiver] mkstemp "/m/.x.Ab12Cd" (in m) failed: Permission denied (13)`), true},
		{"one of the daemon's messages",
			fmt.Errorf("%w: %s", failed, `[receiver] mkstemp "/m/.a.Ab12Cd" (in m) failed: Permission denied (13); [sender] read errors mapping "/m/b" (in m): Input/output error (5)`), true},
		{"the run's own error, the daemon's messages after it",
			fmt.Errorf("%w: %s", errors.New("open /tmp/x: permission denied"), `[sender] read errors mapping "/m/y" (in m): Input/output error (5)`), true},
		{"an SFTP server's quoted message",
			errors.New(`sftp: "/x: No such file or directory" (SSH_FX_FAILURE)`), true},
		{"the first of two lines",
			errors.New("open a: permission denied\nlist b: connection reset by peer"), true},
		{"the second of two joined errors",
			errors.Join(errors.New("list b: connection reset by peer"), errors.New("open a: operation not permitted")), true},
		{"a daemon's refusal as text",
			errors.New("@ERROR: Unknown module 'x'"), true},
		{"words alone",
			errors.New("operation not supported"), true},
	} {
		if got := isPermanentError(c.err); got != c.final {
			t.Errorf("%s: isPermanentError(%q) = %v; want %v", c.what, c.err, got, c.final)
		}
	}
}

// The daemon's word that a module is read only is read in its reason: a
// message naming a file "module is read only" aborted the operation.
func TestRsyncReadOnlyIsTheReason(t *testing.T) {
	for _, c := range []struct {
		msgs []string
		want bool
	}{
		{[]string{"ERROR: module is read only"}, true},
		{[]string{`[receiver] mkstemp "/d/.module is read only.Ab12Cd" (in testmod) failed: Permission denied (13)`}, false},
		{[]string{`[sender] link_stat "/module is read only" (in testmod) failed: No such file or directory (2)`}, false},
		{[]string{`could not make way for new regular file: "d/module is read only" (in testmod)`}, false},
		{nil, false},
	} {
		if got := readOnly(c.msgs); got != c.want {
			t.Errorf("readOnly(%q) = %v; want %v", c.msgs, got, c.want)
		}
	}
}

// On a stock rsyncd: an upload of a file named "module is read only" into a
// folder the daemon cannot write fails, final by its reason, and the
// operation goes on (old code: the whole operation was aborted). The folder
// is made immutable: rsyncd makes a folder it owns writable for the upload.
func TestRsyncUploadNamedReadOnlyGoesOn(t *testing.T) {
	port, dir := startRsyncDaemon(t)
	d := filepath.Join(dir, "d")
	if err := os.Mkdir(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("chflags", "uchg", d).Run(); err != nil {
		t.Skipf("no immutable folder here (chflags: %v)", err)
	}
	t.Cleanup(func() { exec.Command("chflags", "nouchg", d).Run() })
	if err := os.WriteFile(filepath.Join(d, "probe"), nil, 0o644); err == nil {
		t.Skip("the immutable folder can be written to")
	}
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod/", port))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = ContextWithFatalCancel(ctx, cancel)
	err = b.CopyFrom(ctx, "d/module is read only", strings.NewReader("x"), model.PutParams{Mode: 0o644})
	switch {
	case err == nil:
		t.Fatal("an upload into an immutable folder: no error")
	case !strings.Contains(err.Error(), "module is read only"):
		t.Fatalf("%v: the daemon's message does not name the file; the test proves nothing", err)
	case ctx.Err() != nil:
		t.Errorf("the operation was aborted: %v", err)
	case !isPermanentError(err):
		t.Errorf("%v: not final; want the daemon's reason read", err)
	}
	t.Logf("error: %v", err)
}
