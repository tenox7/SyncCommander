package transport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sc/model"
)

// gnuFindHere reports whether find on this machine has what the find form
// of the shell lister needs. Where it does not (macOS, BSD, busybox) the
// shell backends use perl, and the find subtests skip.
func gnuFindHere() bool {
	return exec.Command("sh", "-c", `find / -maxdepth 0 -ignore_readdir_race -readable -executable -printf ''`).Run() == nil
}

// listerOf names the lister sh uses: find, perl, or unknown before its
// first listing or stat.
func listerOf(sh *sshShell) string {
	switch sh.form.Load() {
	case formFind:
		return "find"
	case formPerl:
		return "perl"
	}
	return "unknown"
}

type shellBackend interface {
	List(context.Context, string) ([]model.FileEntry, error)
	PreloadRecursive(context.Context, string) error
	Close() error
}

// openShellBackend opens scheme://addr/base and pins its lister to find or
// perl.
func openShellBackend(t *testing.T, scheme, addr, base, lister string) shellBackend {
	t.Helper()
	if lister == "find" && !gnuFindHere() {
		t.Skip("find here lacks -printf, -readable or -executable; the shell backends use perl")
	}
	b, err := OpenBackend(scheme+"://x:pw@"+addr+base, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	var sh *sshShell
	switch b := b.(type) {
	case *SCPBackend:
		sh = &b.sshShell
	case *SFTPBackend:
		sh = &b.sshShell
	default:
		t.Fatalf("%s:// opened %T", scheme, b)
	}
	sh.form.Store(map[string]int32{"find": formFind, "perl": formPerl}[lister])
	return b.(shellBackend)
}

type lister interface {
	List(context.Context, string) ([]model.FileEntry, error)
}

func listNames(ctx context.Context, b lister, dir string) ([]string, error) {
	entries, err := b.List(ctx, dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	slices.Sort(names)
	return names, err
}

// S1: a folder the shell lister cannot read fails its listing instead of
// reading as empty: one it cannot open (photos, mode 000) and one whose
// names it can read but not stat (scans, mode 444). Both before the deep
// listing and after it, with find and with perl, for scp:// and for
// sftp://, whose deep listing is the shell's too. The folders the deep
// listing read in full are still served from it.
func TestShellListingUnreadableFolder(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	root := t.TempDir()
	for _, p := range []string{"photos/a.jpg", "scans/s.tif", "docs/c.txt", "deep/x/y.txt", "top.txt"} {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for dir, mode := range map[string]os.FileMode{"photos": 0o000, "scans": 0o444} {
		if err := os.Chmod(filepath.Join(root, dir), mode); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(filepath.Join(root, dir), 0o755) })
	}
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	want := map[string][]string{"": {"deep", "docs", "photos", "scans", "top.txt"}, "docs": {"c.txt"}, "deep": {"x"}, "deep/x": {"y.txt"}}
	for _, scheme := range []string{"scp", "sftp"} {
		for _, lister := range []string{"find", "perl"} {
			t.Run(scheme+"/"+lister, func(t *testing.T) {
				ctx := context.Background()
				live := openShellBackend(t, scheme, servers[scheme], root, lister)
				defer live.Close()
				for _, dir := range []string{"photos", "scans"} {
					if names, err := listNames(ctx, live, dir); err == nil {
						t.Errorf("live List(%q) = %q and no error; the folder cannot be read", dir, names)
					}
				}

				b := openShellBackend(t, scheme, servers[scheme], root, lister)
				defer b.Close()
				mark := len(Log.Lines())
				b.PreloadRecursive(ctx, "")
				for _, dir := range []string{"photos", "scans"} {
					if names, err := listNames(ctx, b, dir); err == nil {
						t.Errorf("List(%q) after the deep listing = %q and no error; the folder cannot be read", dir, names)
					}
				}
				for _, dir := range []string{"", "docs", "deep", "deep/x"} {
					if names, err := listNames(ctx, b, dir); err != nil || !slices.Equal(names, want[dir]) {
						t.Errorf("List(%q) = %q, %v; want %q", dir, names, err, want[dir])
					}
				}
				for _, line := range Log.Lines()[mark:] {
					if strings.Contains(line, ">>> ") && (strings.Contains(line, root+"/docs") || strings.Contains(line, root+"/deep")) {
						t.Errorf("listed live although the deep listing read it in full: %s", line)
					}
				}
			})
		}
	}
}

// S1: a base folder that does not exist, a typing mistake in the URL, fails
// its listing instead of showing an empty side.
func TestShellListingMissingBase(t *testing.T) {
	addr := startExecSSHServer(t)
	base := filepath.Join(t.TempDir(), "nope")
	for _, lister := range []string{"find", "perl"} {
		t.Run(lister, func(t *testing.T) {
			ctx := context.Background()
			b := openShellBackend(t, "scp", addr, base, lister)
			defer b.Close()
			if names, err := listNames(ctx, b, ""); err == nil {
				t.Errorf("live List of a missing base = %q and no error", names)
			}
			b.PreloadRecursive(ctx, "")
			if names, err := listNames(ctx, b, ""); err == nil {
				t.Errorf("List of a missing base after the deep listing = %q and no error", names)
			}
		})
	}
}

// S3: a base folder that is a symlink lists the folder it points to, live
// and after the deep listing, with find and with perl; GNU find printed
// nothing for it and exited 0. A base that is a file fails its listing.
// rsync+ssh:// lists folders with find too.
func TestShellListingSymlinkBase(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []string{"real/a.txt", "real/sub/b.txt"} {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(dir, "real"), filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("not a folder"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	want := map[string][]string{"": {"a.txt", "sub"}, "sub": {"b.txt"}}
	addr := startExecSSHServer(t)
	for _, lister := range []string{"find", "perl"} {
		t.Run(lister, func(t *testing.T) {
			ctx := context.Background()
			live := openShellBackend(t, "scp", addr, link, lister)
			defer live.Close()
			deep := openShellBackend(t, "scp", addr, link, lister)
			defer deep.Close()
			deep.PreloadRecursive(ctx, "")
			for name, b := range map[string]shellBackend{"live": live, "after the deep listing": deep} {
				for d, w := range want {
					if names, err := listNames(ctx, b, d); err != nil || !slices.Equal(names, w) {
						t.Errorf("%s: List(%q) under a linked base = %q, %v; want %q", name, d, names, err, w)
					}
				}
			}
			file := openShellBackend(t, "scp", addr, filepath.Join(dir, "file"), lister)
			defer file.Close()
			if names, err := listNames(ctx, file, ""); err == nil {
				t.Errorf("List of a base that is a file = %q and no error", names)
			}
		})
	}
	t.Run("rsync+ssh", func(t *testing.T) {
		if !gnuFindHere() {
			t.Skip("find here lacks -printf, -readable or -executable; the shell backends use perl")
		}
		if _, err := exec.LookPath("rsync"); err != nil {
			t.Skip("no rsync binary")
		}
		b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+addr+link, true, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		for d, w := range want {
			if names, err := listNames(context.Background(), b, d); err != nil || !slices.Equal(names, w) {
				t.Errorf("List(%q) under a linked base = %q, %v; want %q", d, names, err, w)
			}
		}
	})
}
