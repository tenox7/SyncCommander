package transport

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"sc/internal/sshtest"
)

// R16: rsync+ssh received a file whole into a temp folder before a copy
// read it: a second copy of each file on the local disk, and room for the
// largest in $TMPDIR. Open streams it through the shell now, as scp://
// does, and a file it cannot read is an error, not an empty file.
func TestRsyncSSHOpenStreams(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	var mu sync.Mutex
	var cmds []string
	addr := sshtest.StartWith(t, sshtest.Options{Hold: func(cmd string) bool {
		mu.Lock()
		cmds = append(cmds, cmd)
		mu.Unlock()
		return false
	}})
	ran := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), cmds...)
	}
	mod := t.TempDir()
	body := strings.Repeat("0123456789", 30<<10) // more than a pipe holds
	writeTestFile(t, mod, "d/f.bin", body)
	ctx := context.Background()
	b, err := NewRsyncSSHBackend(ctx, "rsync+ssh://x:pw@"+addr+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	mark := len(ran())
	rc, err := b.Open(ctx, "d/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	if left, _ := os.ReadDir(tmp); len(left) > 0 {
		t.Errorf("Open put %s in TMPDIR", left[0].Name())
	}
	got, err := io.ReadAll(rc)
	if cerr := rc.Close(); err == nil {
		err = cerr
	}
	if err != nil || string(got) != body {
		t.Errorf("Open read %d bytes, %v; want the %d of the file", len(got), err, len(body))
	}
	for _, c := range ran()[mark:] {
		if strings.HasPrefix(c, "rsync ") {
			t.Errorf("Open ran rsync: %q", c)
		}
	}

	rc, err = b.Open(ctx, "d/none")
	if err == nil {
		_, err = io.ReadAll(rc)
		if cerr := rc.Close(); err == nil {
			err = cerr
		}
	}
	if err == nil {
		t.Error("Open of a file that is not there read as an empty file")
	}
}
