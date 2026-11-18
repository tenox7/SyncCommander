package transport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// R13: -c stood in for --ignore-times. Both ends read each listed file
// before a byte moved, and a listed file whose bytes matched there was not
// sent, nor named. The names came from the sender's --progress output,
// where a file sent within a second has a progress line with no \r, and
// that line was named too. Now -I sends every listed file, and the sender
// names each one, once.
func TestBatchSendsAndNamesEachListedFile(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	ctx := context.Background()
	mod, src := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "d/a.txt", "same")
	writeTestFile(t, src, "d/sub/b.txt", "new")
	// d/a.txt is there already, the same to the byte, in size and time:
	// the caller listed it by a rule rsync does not know. It still goes.
	writeTestFile(t, mod, "d/a.txt", "same")
	mt := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, root := range []string{src, mod} {
		if err := os.Chtimes(filepath.Join(root, "d", "a.txt"), mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	b, err := NewRsyncSSHBackend(ctx, "rsync+ssh://x:pw@"+startExecSSHServer(t)+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var names []string
	if err := b.SendLocalTree(ctx, src, "d", []string{"d/a.txt", "d/sub/b.txt"}, func(name string) { names = append(names, name) }); err != nil {
		t.Fatal(err)
	}
	slices.Sort(names)
	if want := []string{"a.txt", "sub/b.txt"}; !slices.Equal(names, want) {
		t.Errorf("the send named %q, want %q", names, want)
	}
	if got, err := os.ReadFile(filepath.Join(mod, "d", "sub", "b.txt")); err != nil || string(got) != "new" {
		t.Errorf("d/sub/b.txt there = %q, %v", got, err)
	}
}
