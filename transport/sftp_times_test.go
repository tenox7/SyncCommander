package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
)

// S25: sc sets an SFTP file's times with SETSTAT, whole seconds in protocol
// v3, and READDIR lists them so; find listed the same file to the
// nanosecond. With sub-second compare on, a copy whose time sc had set
// differed from its source for good when find listed it. Both listings now
// keep the times to the second, and the copy equals its source.
func TestSFTPListingsKeepTimesToTheSecond(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	addr := startShellServer(t, true)
	open := func() *SFTPBackend {
		b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+addr+root, true, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close() })
		return b
	}
	src := model.FileEntry{RelPath: "f.txt", Name: "f.txt", Size: 1,
		ModTime: time.Date(2026, 11, 18, 10, 0, 0, 123456789, time.UTC)}
	if err := open().SetTimes(ctx, "f.txt", src.ModTime, src.ModTime, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(root, "f.txt")); err != nil || !fi.ModTime().Equal(src.ModTime.Truncate(time.Second)) {
		t.Fatalf("the server holds %v, %v; want the time cut to the second", fi.ModTime(), err)
	}
	listed := func(b *SFTPBackend) model.FileEntry {
		entries, err := b.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.Name == "f.txt" {
				return e
			}
		}
		t.Fatal("f.txt not listed")
		return model.FileEntry{}
	}
	readDir := listed(open())
	preloaded := open()
	mark := len(Log.Lines())
	if err := preloaded.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	found := listed(preloaded)
	if loggedSince(mark, "READDIR") {
		t.Fatal("the preloaded backend listed with READDIR, not find")
	}
	opts := model.CompareOpts{Size: true, ModTime: true, SubSecond: true}
	for name, e := range map[string]model.FileEntry{"READDIR": readDir, "find": found} {
		if e.TimeRes != model.TimeSecond {
			t.Errorf("%s: time kept to %v, want TimeSecond, as SFTP sets it", name, e.TimeRes)
		}
		root := model.NewRootNode()
		rows := model.MergeChildren(root, []model.FileEntry{src}, []model.FileEntry{e}, 1, opts, nil)
		if len(rows) != 1 || rows[0].Compare.ModTime != model.AttrEqual {
			t.Errorf("%s: the copy's time %v compares %v with its source's %v, want equal", name, e.ModTime, rows[0].Compare.ModTime, src.ModTime)
		}
	}
}
