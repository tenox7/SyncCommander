package transport

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// windowsRules makes, for one test, the local backend follow the rules of
// Windows for names, as it does there.
func windowsRules(t *testing.T) {
	t.Helper()
	old := windowsNames
	windowsNames = true
	t.Cleanup(func() { windowsNames = old })
}

// L25: on Windows a name from another system was mapped onto the disk as
// it was. a\b.txt was the file b.txt in a folder a, x:y went into a hidden
// part of the file x, "a." was the file a, and NUL the device: a copy into
// such a name said OK while its bytes went elsewhere, or nowhere. Every
// call on such a name now fails with the reason and writes nothing, and
// the name has no local path, which rsync took for the folder sc runs in.
// Names Windows holds work as before.
func TestLocalRefusesNamesWindowsCannotHold(t *testing.T) {
	windowsRules(t)
	dir := t.TempDir()
	b := NewLocalBackend(dir)
	ctx := context.Background()
	put := func(rel string) error {
		return b.CopyFrom(ctx, rel, strings.NewReader("x"), model.PutParams{Size: 1, Mode: 0o644})
	}
	if err := put("keep.txt"); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{`a\b.txt`, "x:y", "nul.txt", "NUL", "a.", "a ", "dir./f.txt", "aux/g.txt", "a*b", `d\..\..\up.txt`} {
		for _, c := range []struct {
			what string
			call func() error
		}{
			{"CopyFrom", func() error { return put(rel) }},
			{"AppendFrom", func() error { return b.AppendFrom(ctx, rel, nil, model.PutParams{}, 5) }},
			{"Mkdir", func() error { return b.Mkdir(ctx, rel, 0o755) }},
			{"SetTimes", func() error { return b.SetTimes(ctx, rel, time.Unix(1e9, 0), time.Time{}, time.Time{}) }},
			{"Rename onto it", func() error { return b.Rename(ctx, "keep.txt", rel) }},
			{"Rename from it", func() error { return b.Rename(ctx, rel, "other.txt") }},
			{"Remove", func() error { return b.Remove(ctx, rel) }},
			{"RemoveAll", func() error { return b.RemoveAll(ctx, rel) }},
			{"CommitPart", func() error { return b.CommitPart(ctx, rel, 0o644) }},
			{"PartSize", func() error { _, err := b.PartSize(ctx, rel); return err }},
			{"Stat", func() error { _, err := b.Stat(ctx, rel); return err }},
			{"List", func() error { _, err := b.List(ctx, rel); return err }},
			{"Checksum", func() error { _, err := b.Checksum(ctx, rel, "sha256"); return err }},
			{"Open", func() error {
				r, err := b.Open(ctx, rel)
				if r != nil {
					r.Close()
				}
				return err
			}},
		} {
			if err := c.call(); !errors.Is(err, ErrBadName) {
				t.Errorf("%s of %#q: %v; want ErrBadName", c.what, rel, err)
			}
		}
		if p := b.LocalPath(rel); p != "" {
			t.Errorf("LocalPath of %#q is %q; want none", rel, p)
		}
	}
	for _, rel := range []string{"con2.txt", "COM10", "a.b", "nul_", "sub/console.log"} {
		if err := put(rel); err != nil {
			t.Errorf("CopyFrom of %s: %v", rel, err)
		}
	}
	if want := []string{"COM10", "a.b", "con2.txt", "keep.txt", "nul_", "sub/", "sub/console.log"}; !slices.Equal(filesUnder(t, dir), want) {
		t.Errorf("the folder holds %q, want %q", filesUnder(t, dir), want)
	}
	// Another try has the same name: one is enough.
	tries := 0
	err := Retry(ctx, "local", "copy nul.txt", func(context.Context) error {
		tries++
		return put("nul.txt")
	})
	if !errors.Is(err, ErrBadName) || tries != 1 {
		t.Errorf("a name Windows cannot hold was tried %d times: %v; want once, ErrBadName", tries, err)
	}
	if caps, err := b.Capabilities(ctx); err != nil || !caps.Names.Windows {
		t.Errorf("the local backend's names: %+v, %v; want the rules of Windows, for the copy to refuse a name before it writes", caps.Names, err)
	}
}
