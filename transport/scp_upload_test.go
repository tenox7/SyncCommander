package transport

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
)

// S19: an upload over the shell made its folder with a command of its own,
// one more round trip for each file. The folder is made by the command
// that writes the file. A folder that cannot be made still fails the
// upload, and nothing is written.
func TestSCPUploadIsOneCommand(t *testing.T) {
	root := t.TempDir()
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	mark := len(Log.Lines())
	if err := b.CopyFrom(ctx, "new/dir/f.txt", strings.NewReader("data"), model.PutParams{Size: 4, Mode: 0o640}); err != nil {
		t.Fatal(err)
	}
	var sent []string
	for _, line := range Log.Lines()[mark:] {
		if strings.Contains(line, "scp >>> ") {
			sent = append(sent, line)
		}
	}
	if len(sent) != 1 {
		t.Errorf("the upload sent %d commands, want 1: %q", len(sent), sent)
	}
	full := filepath.Join(root, "new", "dir", "f.txt")
	if got, err := os.ReadFile(full); err != nil || string(got) != "data" {
		t.Errorf("the file holds %q, %v; want data", got, err)
	}
	if fi, err := os.Stat(full); err != nil || fi.Mode().Perm() != 0o640 {
		t.Errorf("the file's mode = %v, %v; want 0640", fi.Mode().Perm(), err)
	}

	if err := os.WriteFile(filepath.Join(root, "plain"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.CopyFrom(ctx, "plain/f.txt", strings.NewReader("data"), model.PutParams{Size: 4, Mode: 0o644}); err == nil {
		t.Error("an upload into a folder that is a file went through")
	}
	if got, _ := os.ReadFile(filepath.Join(root, "plain")); string(got) != "x" {
		t.Errorf("the file in the way holds %q, want x", got)
	}
}
