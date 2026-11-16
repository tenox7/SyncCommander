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
	return exec.Command("sh", "-c", `find / -maxdepth 0 -readable -executable -printf ''`).Run() == nil
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
	sh.usePerl.Store(lister == "perl")
	t.Cleanup(func() { sh.usePerl.Store(false) })
	return b.(shellBackend)
}

func listNames(ctx context.Context, b shellBackend, dir string) ([]string, error) {
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
