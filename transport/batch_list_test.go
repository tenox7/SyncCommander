package transport

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"testing"
)

// F20: the batch is told its files relative to the base; rsync names them
// relative to the folder it sends. A path that is not in that folder, or
// that has a "..", "." or empty part, is refused before anything is sent.
func TestBatchNamesAreInTheFolder(t *testing.T) {
	for _, c := range []struct {
		rel   string
		files []string
		want  []string // nil: refused
	}{
		{"d", []string{"d/a.txt", "d/e/f.txt", "d/e"}, []string{"a.txt", "e/f.txt", "e"}},
		{"", []string{"a.txt", "e/f.txt"}, []string{"a.txt", "e/f.txt"}},
		{"d/e", []string{"d/e/f.txt"}, []string{"f.txt"}},
		{"d", []string{"x/a.txt"}, nil},
		{"d", []string{"dd/a.txt"}, nil},
		{"d", []string{"d/../x.txt"}, nil},
		{"d", []string{"d/./x.txt"}, nil},
		{"d", []string{"d//x.txt"}, nil},
		{"d", []string{"d/"}, nil},
		{"", []string{"../x.txt"}, nil},
		{"", []string{"/etc/passwd"}, nil},
	} {
		got, err := namesUnder(c.rel, c.files)
		switch {
		case c.want == nil && err == nil:
			t.Errorf("namesUnder(%q, %q) = %q, want it refused", c.rel, c.files, got)
		case c.want != nil && (err != nil || !slices.Equal(got, c.want)):
			t.Errorf("namesUnder(%q, %q) = %q, %v, want %q", c.rel, c.files, got, err, c.want)
		}
	}
}

// No list is no run: a client sender without one sends the whole folder.
func TestBatchWithNoFilesSendsNothing(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	mod, src := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "d/a.txt", "would go with a whole-folder send")
	b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+startExecSSHServer(t)+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, files := range [][]string{nil, {}} {
		if err := b.SendLocalTree(context.Background(), src, "d", files, nil); err != nil {
			t.Errorf("SendLocalTree with files %#v: %v", files, err)
		}
	}
	if entries, _ := os.ReadDir(mod); len(entries) != 0 {
		t.Errorf("SendLocalTree with no files sent something: the remote holds %d entries", len(entries))
	}
}
