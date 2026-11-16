package transport

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sc/model"
)

// sameOnDisk reports whether alt opens the file name is, in dir.
func sameOnDisk(t *testing.T, dir, name, alt string) bool {
	t.Helper()
	a, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Lstat(filepath.Join(dir, alt))
	return err == nil && os.SameFile(a, b)
}

func namesIn(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(dir, func(p string, _ os.DirEntry, _ error) error {
		out = append(out, p)
		return nil
	})
	return out
}

// D10: a local root says how its file system matches names, and finds it
// out without writing: a name in the root, or the root's own name, looked
// up in another spelling. The test asks the disk itself, by writing, and
// the two must agree: on a Mac's own disk letter case and Unicode forms
// fold, on Linux neither does.
func TestLocalNamesMatchTheDisk(t *testing.T) {
	ctx := context.Background()
	nfc, nfd := "café", "café"
	truth := t.TempDir()
	writeFile(t, filepath.Join(truth, "probe-a"), "x")
	writeFile(t, filepath.Join(truth, nfc), "x")
	foldCase, foldForms := sameOnDisk(t, truth, "probe-a", "PROBE-A"), sameOnDisk(t, truth, nfc, nfd)
	t.Logf("this disk folds case: %v, Unicode forms: %v", foldCase, foldForms)

	full := t.TempDir()
	writeFile(t, filepath.Join(full, "Readme.txt"), "x")
	writeFile(t, filepath.Join(full, nfc), "x")
	empty := filepath.Join(t.TempDir(), "EmptyRoot")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	fold := func(folds bool) model.Fold { return map[bool]model.Fold{true: model.Folded, false: model.Apart}[folds] }
	for what, c := range map[string]struct {
		root string
		want model.NameRule
	}{
		"a root with names in it": {full, model.NameRule{Case: fold(foldCase), Forms: fold(foldForms)}},
		// No name has another Unicode form to try: that stays unknown.
		"an empty root, by its own name": {empty, model.NameRule{Case: fold(foldCase), Forms: model.FoldUnknown}},
	} {
		before := namesIn(t, filepath.Dir(c.root))
		caps, err := NewLocalBackend(c.root).Capabilities(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if caps.Names != c.want {
			t.Errorf("%s: Names = %+v, want %+v (this disk folds case %v, forms %v)", what, caps.Names, c.want, foldCase, foldForms)
		}
		if after := namesIn(t, filepath.Dir(c.root)); !slices.Equal(after, before) {
			t.Errorf("%s: the probe wrote: %q became %q", what, before, after)
		}
	}
}
