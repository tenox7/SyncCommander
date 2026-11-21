//go:build unix

package transport

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"sc/model"
)

// specialTree makes tree/a.txt, tree/link.txt (a link to outside.txt, out
// of the tree), tree/dirlink (a link to the folder outside/) and tree/pipe
// (a named pipe).
func specialTree(t *testing.T) (dir, root string) {
	t.Helper()
	root = t.TempDir()
	dir = filepath.Join(root, "tree")
	writeTestFile(t, dir, "a.txt", "a")
	writeTestFile(t, root, "outside.txt", "keep me")
	if err := os.MkdirAll(filepath.Join(root, "outside"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "outside.txt"), filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "outside"), filepath.Join(dir, "dirlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, root
}

var specialTreeTypes = map[string]os.FileMode{"a.txt": 0, "link.txt": os.ModeSymlink, "dirlink": os.ModeSymlink, "pipe": os.ModeNamedPipe}

// specialTreeTargets are the targets of specialTree's links under root.
func specialTreeTargets(root string) map[string]string {
	return map[string]string{"link.txt": filepath.Join(root, "outside.txt"), "dirlink": filepath.Join(root, "outside")}
}

// linkTargets maps each link of entries to its target, "?" when the listing
// gave none.
func linkTargets(entries []model.FileEntry) map[string]string {
	got := map[string]string{}
	for _, e := range entries {
		if e.IsLink() {
			t, ok := e.Target()
			if !ok {
				t = "?"
			}
			got[e.Name] = t
		}
	}
	return got
}

func entryTypes(entries []model.FileEntry) map[string]os.FileMode {
	got := make(map[string]os.FileMode, len(entries))
	for _, e := range entries {
		got[e.Name] = e.Mode.Type()
	}
	return got
}

// within runs f and fails when it still runs after d: an open of a named
// pipe waits in the kernel for the other end, where cancel cannot reach
// it. The pipe is then opened from the other end so the call can return.
func within(t *testing.T, what string, d time.Duration, pipe string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
		return
	case <-time.After(d):
		t.Errorf("%s: still blocked after %v", what, d)
	}
	if p, err := os.OpenFile(pipe, os.O_RDWR|syscall.O_NONBLOCK, 0); err == nil {
		time.Sleep(100 * time.Millisecond)
		p.Close()
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}

// L2, L9: the local backend left links out of its listings, so a copy took
// the name for free, and listed named pipes as files.
func TestLocalListsLinksAndPipes(t *testing.T) {
	dir, root := specialTree(t)
	entries, err := NewLocalBackend(dir).List(context.Background(), "")
	if got := entryTypes(entries); err != nil || !maps.Equal(got, specialTreeTypes) {
		t.Errorf("List = %v, %v; want %v", got, err, specialTreeTypes)
	}
	if got, want := linkTargets(entries), specialTreeTargets(root); !maps.Equal(got, want) {
		t.Errorf("List gave the targets %v; want %v", got, want)
	}
	if e, err := NewLocalBackend(dir).Stat(context.Background(), "dirlink"); err != nil || linkTargets([]model.FileEntry{e})["dirlink"] != filepath.Join(root, "outside") {
		t.Errorf("Stat(dirlink) = %+v, %v; want the link with its target", e, err)
	}
}

// L2, L9: a write to a name that is a link wrote into the link's target,
// out of the tree, and a folder made at a link to a folder took the link
// as that folder. A write to a named pipe waited for a reader, and a read
// of one for a writer, for good. A checksum of a link summed its target.
// Each now fails at once.
func TestLocalRefusesLinksAndPipes(t *testing.T) {
	dir, root := specialTree(t)
	b := NewLocalBackend(dir)
	ctx := context.Background()
	pipe := filepath.Join(dir, "pipe")
	refused := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, ErrSpecialFile) {
			t.Errorf("%s: %v; want ErrSpecialFile", what, err)
		}
	}
	within(t, "CopyFrom onto a link", 5*time.Second, pipe, func() {
		refused("CopyFrom onto a link", b.CopyFrom(ctx, "link.txt", strings.NewReader("NEW DATA"), model.PutParams{Mode: 0o644}))
	})
	within(t, "CopyFrom into a link to a folder", 5*time.Second, pipe, func() {
		refused("Mkdir at a link to a folder", b.Mkdir(ctx, "dirlink", 0o755))
	})
	within(t, "Checksum of a link", 5*time.Second, pipe, func() {
		_, err := b.Checksum(ctx, "link.txt", "md5")
		refused("Checksum of a link", err)
	})
	within(t, "CopyFrom onto a named pipe", 5*time.Second, pipe, func() {
		refused("CopyFrom onto a named pipe", b.CopyFrom(ctx, "pipe", strings.NewReader("x"), model.PutParams{Mode: 0o644}))
	})
	within(t, "Open of a named pipe", 5*time.Second, pipe, func() {
		r, err := b.Open(ctx, "pipe")
		if err == nil {
			r.Close()
		}
		refused("Open of a named pipe", err)
	})
	within(t, "Checksum of a named pipe", 5*time.Second, pipe, func() {
		_, err := b.Checksum(ctx, "pipe", "md5")
		refused("Checksum of a named pipe", err)
	})
	if got, _ := os.ReadFile(filepath.Join(root, "outside.txt")); string(got) != "keep me" {
		t.Errorf("the link's target out of the tree now holds %q", got)
	}
	if fi, err := os.Lstat(filepath.Join(dir, "link.txt")); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("link.txt is no longer the link: %v", err)
	}
}

// L2, L9 on SFTP and the shell (S4, S13): links were left out of the
// listings, the shell lister gave named pipes as files, and its perl
// fallback gave every pipe and device as a file. Live listings and the deep
// listing, with find and with perl.
func TestRemoteListsLinksAndPipes(t *testing.T) {
	dir, root := specialTree(t)
	ctx := context.Background()
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	for _, lister := range []string{"find", "perl"} {
		t.Run(lister, func(t *testing.T) {
			for scheme, addr := range servers {
				live := openShellBackend(t, scheme, addr, dir, lister)
				defer live.Close()
				deep := openShellBackend(t, scheme, addr, dir, lister)
				defer deep.Close()
				if err := deep.PreloadRecursive(ctx, ""); err != nil {
					t.Fatal(err)
				}
				for name, b := range map[string]shellBackend{"live": live, "deep": deep} {
					entries, err := b.List(ctx, "")
					if got := entryTypes(entries); err != nil || !maps.Equal(got, specialTreeTypes) {
						t.Errorf("%s %s: List = %v, %v; want %v", scheme, name, got, err, specialTreeTypes)
					}
					if got, want := linkTargets(entries), specialTreeTargets(root); !maps.Equal(got, want) {
						t.Errorf("%s %s: List gave the targets %v; want %v", scheme, name, got, want)
					}
				}
				if e, err := live.(model.Backend).Stat(ctx, "dirlink"); err != nil || linkTargets([]model.FileEntry{e})["dirlink"] != filepath.Join(root, "outside") {
					t.Errorf("%s: Stat(dirlink) = %+v, %v; want the link with its target", scheme, e, err)
				}
			}
		})
	}
}

// L2, L9 on FTP: LIST lines of links were left out; in MLSD the library
// takes any type it does not know (OS.unix=symlink, OS.unix=slink:target,
// a device) as a file.
func TestFTPListsLinksAndDevices(t *testing.T) {
	ctx := context.Background()
	list := openFakeFTP(t, &fakeFTPD{ls: map[string][]string{"/": {
		lsLine("a.txt", false),
		"lrwxrwxrwx    1 1000     1000           11 Nov 16 10:00 link.txt -> /etc/passwd",
	}}}, "/")
	entries, err := list.List(ctx, "")
	if got, want := entryTypes(entries), map[string]os.FileMode{"a.txt": 0, "link.txt": os.ModeSymlink}; err != nil || !maps.Equal(got, want) {
		t.Errorf("LIST: %v, %v; want %v", got, err, want)
	}
	if got, want := linkTargets(entries), map[string]string{"link.txt": "/etc/passwd"}; !maps.Equal(got, want) {
		t.Errorf("LIST gave the targets %v; want %v", got, want)
	}
	mlsd := openFakeFTP(t, &fakeFTPD{mlsd: map[string][]string{"/": {
		"type=file;size=5;modify=20260101000000; a.txt",
		"type=OS.unix=symlink;size=11;modify=20260101000000; link.txt",
		"type=OS.unix=slink:/etc;modify=20260101000000; etc",
		"type=OS.unix=chr-1/3;modify=20260101000000; null",
		"type=dir;modify=20260101000000; sub",
	}}}, "/")
	entries, err = mlsd.List(ctx, "")
	got := entryTypes(entries)
	want := map[string]os.FileMode{"a.txt": 0, "link.txt": os.ModeSymlink, "etc": os.ModeSymlink, "null": os.ModeIrregular, "sub": os.ModeDir}
	if err != nil || !maps.Equal(got, want) {
		t.Errorf("MLSD: %v, %v; want %v", got, err, want)
	}
	if got, want := linkTargets(entries), map[string]string{"link.txt": "?", "etc": "/etc"}; !maps.Equal(got, want) {
		t.Errorf("MLSD gave the targets %v; want %v", got, want)
	}
}

// L2, L9 on rsync://: gorsyncd lists links and named pipes even without -l
// and -D. sc dropped the links and took a pipe for a file, which a
// download then opened. Stock rsync leaves both out of the listing. Since
// 2026-11-20 every listing asks with -l, which sends each link's target.
func TestRsyncListsLinksAndPipes(t *testing.T) {
	ctx := context.Background()
	url, mod := startGorsyncd(t)
	dir, root := specialTree(t)
	targets := specialTreeTargets(root)
	for name := range specialTreeTypes {
		if err := os.Rename(filepath.Join(dir, name), filepath.Join(mod, name)); err != nil {
			t.Fatal(err)
		}
	}
	b, err := OpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	entries, err := b.List(ctx, "")
	if got := entryTypes(entries); err != nil || !maps.Equal(got, specialTreeTypes) {
		t.Errorf("gorsyncd: List = %v, %v; want %v", got, err, specialTreeTypes)
	}
	if got := linkTargets(entries); !maps.Equal(got, targets) {
		t.Errorf("gorsyncd: List gave the targets %v; want %v", got, targets)
	}
	if e, err := b.Stat(ctx, "dirlink"); err != nil || !e.IsLink() || e.Link == nil || *e.Link != targets["dirlink"] {
		t.Errorf("gorsyncd: Stat dirlink = %+v, %v; want the link to %s", e, err, targets["dirlink"])
	}
	deep, err := OpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(deep)
	p, ok := deep.(interface {
		PreloadRecursive(context.Context, string) error
	})
	if !ok {
		t.Fatalf("rsync:// has no deep listing: %T", deep)
	}
	if err := p.PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	entries, err = deep.List(ctx, "")
	if got := entryTypes(entries); err != nil || !maps.Equal(got, specialTreeTypes) {
		t.Errorf("gorsyncd deep: List = %v, %v; want %v", got, err, specialTreeTypes)
	}
	if got := linkTargets(entries); !maps.Equal(got, targets) {
		t.Errorf("gorsyncd deep: List gave the targets %v; want %v", got, targets)
	}
}
