package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sc/model"
)

// R20: rsync:// ran gorsync's command line with the local paths among its
// options. With `sc . rsync://…`, a file at the top with a name that looks
// like an option was one there: "-V" and "--version" ask for the version,
// and gorsync printed it and ended the program (os.Exit). A resumed upload
// sends the local file by its own path. Since 86f53f9 sc gives gorsync its
// paths apart from its options.
func TestRsyncSendsALocalNameThatLooksLikeAnOption(t *testing.T) {
	ctx := context.Background()
	mod, src := t.TempDir(), t.TempDir()
	b, err := NewRsyncBackend(startGorsyncdModule(t, mod, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(src)
	local := NewLocalBackend(".")
	for _, name := range []string{"-V", "--version"} {
		writeTestFile(t, src, name, "0123456789")
		writeTestFile(t, mod, name, "01234")
		if err := b.AppendFrom(ctx, name, localRangeOpener{b: local, rel: name, size: 10}, model.PutParams{Size: 10}, 5); err != nil {
			t.Errorf("AppendFrom %s: %v", name, err)
			continue
		}
		if got, err := os.ReadFile(filepath.Join(mod, name)); err != nil || string(got) != "0123456789" {
			t.Errorf("%s there = %q, %v; want the whole file", name, got, err)
		}
	}
}
