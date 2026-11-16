package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
)

// statTree writes a.txt (5 bytes), sub/b.txt (3 bytes) and, with links,
// link.txt, a link to a.txt.
func statTree(t *testing.T, root string, links bool) {
	t.Helper()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "sub/b.txt", "bee")
	if links {
		if err := os.Symlink("a.txt", filepath.Join(root, "link.txt")); err != nil {
			t.Fatal(err)
		}
	}
}

// sameRow reports whether two rows say the same, times by Equal.
func sameRow(a, b model.FileEntry) bool {
	return a.RelPath == b.RelPath && a.Name == b.Name && a.Size == b.Size && a.IsDir == b.IsDir &&
		a.Mode == b.Mode && a.ModTime.Equal(b.ModTime)
}

// checkStat holds b to the Stat contract (D1): each path gets the row a
// fresh listing gives it, a link is the link, nothing there is
// fs.ErrNotExist, and the answer is what the server holds now, whatever a
// listing cache still says. grow makes a.txt 9 bytes on the server; nil
// skips that part.
func checkStat(t *testing.T, b model.Backend, links bool, grow func()) {
	t.Helper()
	ctx := context.Background()
	for _, dir := range []string{"", "sub"} {
		entries, err := b.List(model.WithFreshListing(ctx), dir)
		if err != nil {
			t.Fatalf("List %q: %v", dir, err)
		}
		for _, want := range entries {
			got, err := b.Stat(ctx, want.RelPath)
			if err != nil || !sameRow(got, want) {
				t.Errorf("Stat %s = %+v, %v\n  want the listing's row %+v", want.RelPath, got, err, want)
			}
		}
	}
	if e, err := b.Stat(ctx, "a.txt"); err != nil || e.Size != 5 || e.IsDir || e.Mode.Type() != 0 {
		t.Errorf("Stat a.txt = %+v, %v; want a 5-byte file", e, err)
	}
	if e, err := b.Stat(ctx, "sub"); err != nil || !e.IsDir || !e.Mode.IsDir() || e.Name != "sub" {
		t.Errorf("Stat sub = %+v, %v; want the folder", e, err)
	}
	if e, err := b.Stat(ctx, ""); err != nil || !e.IsDir || e.RelPath != "" || e.Name != "" {
		t.Errorf("Stat of the root = %+v, %v; want a folder", e, err)
	}
	if links {
		if e, err := b.Stat(ctx, "link.txt"); err != nil || e.Mode&os.ModeSymlink == 0 || e.IsDir {
			t.Errorf("Stat link.txt = %+v, %v; want the link itself", e, err)
		}
	}
	for _, p := range []string{"nope.txt", "nodir/x.txt", "sub/nope"} {
		if e, err := b.Stat(ctx, p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat %s = %+v, %v; want fs.ErrNotExist", p, e, err)
		}
	}
	if grow == nil {
		return
	}
	if p, ok := b.(model.RecursivePreloader); ok {
		if err := p.PreloadRecursive(ctx, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	grow()
	if e, err := b.Stat(ctx, "a.txt"); err != nil || e.Size != 9 {
		t.Errorf("Stat a.txt after it grew = %+v, %v; want the 9 bytes it holds now, not a listing's 5", e, err)
	}
}

// growOnDisk makes root/a.txt 9 bytes.
func growOnDisk(t *testing.T, root string) func() {
	return func() { writeTestFile(t, root, "a.txt", "123456789") }
}

func TestStatLocal(t *testing.T) {
	root := t.TempDir()
	statTree(t, root, true)
	checkStat(t, NewLocalBackend(root), true, growOnDisk(t, root))
}

// scp:// asks the shell (find, or perl where find has no -printf); sftp://
// asks the sftp server. "auto" starts with find and, where find has no
// -printf, turns to perl.
func TestStatShell(t *testing.T) {
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	for _, lister := range []string{"find", "perl", "auto"} {
		for scheme, addr := range servers {
			t.Run(scheme+"/"+lister, func(t *testing.T) {
				root := t.TempDir()
				statTree(t, root, true)
				b := openShellBackend(t, scheme, addr, root, lister)
				defer b.Close()
				checkStat(t, b.(model.Backend), true, growOnDisk(t, root))
			})
		}
	}
}

// S10: a login banner ahead of the record is not a file of 0 bytes.
func TestStatRecordRefusesNoise(t *testing.T) {
	const rec = "5\t1700000000.5\t1700000000\t1700000000\t644\tf\ta.txt\x00"
	if r, ok := statRecord(rec); !ok || r.entry.Size != 5 {
		t.Errorf("statRecord(%q) = %+v, %v; want 5 bytes", rec, r, ok)
	}
	if r, ok := statRecord("Welcome to the server\n" + rec); ok {
		t.Errorf("a banner ahead of the record read as %+v; want no record", r.entry)
	}
	if r, ok := statRecord("Permission denied\t0\t0\t0\t0\t!\t/x/a.txt\x00"); !ok || r.err != "Permission denied" {
		t.Errorf("an error record read as %+v, %v", r, ok)
	}
}

// rsync:// over the in-process gorsyncd and over a stock rsync daemon,
// which leaves links out of a listing run without -l.
func TestStatRsyncDaemon(t *testing.T) {
	t.Run("gorsyncd", func(t *testing.T) {
		url, mod := startGorsyncd(t)
		statTree(t, mod, true)
		b, err := OpenBackend(url, false, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer CloseBackend(b)
		checkStat(t, b, true, growOnDisk(t, mod))
	})
	t.Run("rsyncd", func(t *testing.T) {
		port, mod := startRsyncDaemon(t)
		statTree(t, mod, true)
		b, err := OpenBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port), false, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer CloseBackend(b)
		checkStat(t, b, true, growOnDisk(t, mod))
	})
}

// ftp:// with MLST, whose type fact the library does not read, and without
// it, where the folder is listed.
func TestStatFTP(t *testing.T) {
	t.Run("mlst", func(t *testing.T) {
		b := openFakeFTP(t, &fakeFTPD{mlsd: map[string][]string{
			"/": {
				"type=file;size=5;modify=20260101000000; a.txt",
				"type=dir;modify=20260101000000; sub",
				"type=OS.unix=symlink;size=5;modify=20260101000000; link.txt",
			},
			"/sub": {"type=file;size=3;modify=20260101000000; b.txt"},
		}}, "/")
		checkStat(t, b, true, nil)
	})
	t.Run("list", func(t *testing.T) {
		b := openFakeFTP(t, &fakeFTPD{ls: map[string][]string{
			"/": {
				lsLine("a.txt", false),
				lsLine("sub", true),
				"lrwxrwxrwx    1 1000     1000            5 Nov 16 10:00 link.txt -> a.txt",
			},
			"/sub": {lsLine("b.txt", false)},
		}}, "/")
		checkStat(t, b, true, nil)
	})
}

func TestStatWebDAV(t *testing.T) {
	root := t.TempDir()
	statTree(t, root, false)
	port, _ := startRcloneWebDAV(t, root)
	b, err := NewWebDAVBackend(context.Background(), fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), false, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	checkStat(t, b, false, growOnDisk(t, root))
}

func TestStatRclone(t *testing.T) {
	root := t.TempDir()
	statTree(t, root, false)
	b, err := NewRcloneBackend(context.Background(), "rclone://"+root, false)
	if err != nil {
		t.Fatal(err)
	}
	checkStat(t, b, false, growOnDisk(t, root))
}

// restic:// has a fixed top level; config and objects are asked by HEAD.
func TestStatRestic(t *testing.T) {
	root := t.TempDir()
	port, _ := startRcloneRestic(t, root)
	open := func() *ResticBackend {
		var b *ResticBackend
		var err error
		for range 20 {
			if b, err = NewResticBackend(context.Background(), fmt.Sprintf("restic://u:p@127.0.0.1:%d/", port), false, 8); err == nil {
				return b
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("NewResticBackend: %v", err)
		return nil
	}
	b := open()
	ctx := context.Background()
	resticPut(t, b, "config", []byte("config v1"))
	pack := []byte("a small pack")
	hash := sha256hex(pack)
	resticPut(t, b, "data/"+hash, pack)

	for _, dir := range []string{"", "data"} {
		entries, err := b.List(model.WithFreshListing(ctx), dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range entries {
			got, err := b.Stat(ctx, want.RelPath)
			if err != nil || !sameRow(got, want) {
				t.Errorf("Stat %s = %+v, %v\n  want the listing's row %+v", want.RelPath, got, err, want)
			}
		}
	}
	if e, err := b.Stat(ctx, "data/"+hash); err != nil || e.Size != int64(len(pack)) || e.IsDir {
		t.Errorf("Stat of an object = %+v, %v; want %d bytes", e, err, len(pack))
	}
	for _, p := range []string{"", "data", "keys"} {
		if e, err := b.Stat(ctx, p); err != nil || !e.IsDir {
			t.Errorf("Stat %q = %+v, %v; want a folder", p, e, err)
		}
	}
	for _, p := range []string{"nope", "data/" + sha256hex([]byte("other")), "data/a/b"} {
		if e, err := b.Stat(ctx, p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat %s = %+v, %v; want fs.ErrNotExist", p, e, err)
		}
	}
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	other := open()
	resticPut(t, other, "config", []byte("config v2, longer"))
	if e, err := b.Stat(ctx, "config"); err != nil || e.Size != int64(len("config v2, longer")) {
		t.Errorf("Stat config after another client rewrote it = %+v, %v; want its new size", e, err)
	}
}

// fake:// answers from its generated tree.
func TestStatFake(t *testing.T) {
	b, err := NewFakeBackend("fake://x?dirs=2&files=3&depth=2")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	entries, err := b.List(ctx, "")
	if err != nil || len(entries) == 0 {
		t.Fatalf("List = %d entries, %v", len(entries), err)
	}
	for _, want := range entries {
		if got, err := b.Stat(ctx, want.RelPath); err != nil || !sameRow(got, want) {
			t.Errorf("Stat %s = %+v, %v; want %+v", want.RelPath, got, err, want)
		}
	}
	if _, err := b.Stat(ctx, "nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat nope = %v, want fs.ErrNotExist", err)
	}
}
