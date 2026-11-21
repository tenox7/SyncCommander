//go:build unix

package transfer

import (
	"os"
	"path/filepath"
	"testing"

	"sc/internal/sshtest"
	"sc/model"
	"sc/transport"
)

// linkRemote opens a folder as the remote kind names, and returns it and
// the folder: sftp with a shell (its listings are find's), sftp alone
// (READDIR and READLINK), scp (the shell, and ln -s), a stock rsync daemon
// (links sent with -l), or rsync+ssh (rsync's listings, and ln -s).
func linkRemote(t *testing.T, kind, dir string) (model.Backend, string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var url string
	switch kind {
	case "sftp":
		url = "sftp://x:pw@" + sshtest.Start(t, true) + dir
	case "sftp-only":
		url = "sftp://x:pw@" + sshtest.StartWith(t, sshtest.Options{SFTP: true, OnlySFTP: true}) + dir
	case "scp":
		url = "scp://x:pw@" + sshtest.Start(t, false) + dir
	case "rsync+ssh":
		url = "rsync+ssh://x:pw@" + sshtest.Start(t, false) + dir
	case "rsync":
		url, dir = startRsyncd(t)
	}
	b, err := transport.OpenBackend(url, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(b) })
	return b, dir
}

// munged is target as a stock rsync daemon keeps it on its disk: with use
// chroot off it munges every link it takes (munge symlinks), and gives the
// target back as it was when it sends it.
func munged(kind, target string) string {
	if kind == "rsync" {
		return "/rsyncd-munged/" + target
	}
	return target
}

// 2026-11-20: links go to a remote as links, and come back from one: its
// listing gives each link's target, so the links compare equal after the
// copy, and a copy back makes the same links. A link to a folder that is
// there is not followed: the folder is not sent. A mirror deletes a link
// only the remote has, and not what it names.
func TestLinksToAndFromRemotes(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	writeFile(t, filepath.Join(outside, "keep.txt"), "keep me")
	links := map[string]string{
		"rel.lnk":     "a.txt",
		"abs.lnk":     "/nonexistent/sc-link-test",
		"dir.lnk":     "sub",
		"sub/up.lnk":  "../a.txt",
		"odd.lnk":     "-n x\ty'z",
		"out.lnk":     outside,
		"sub/out.lnk": outside,
	}
	left := filepath.Join(root, "left")
	writeFile(t, filepath.Join(left, "a.txt"), "a")
	writeFile(t, filepath.Join(left, "sub", "s.txt"), "s")
	for name, target := range links {
		linkTo(t, target, filepath.Join(left, name))
	}
	for _, name := range []string{"sftp", "sftp-only", "scp", "rsync", "rsync+ssh"} {
		t.Run(name, func(t *testing.T) {
			remote, right := linkRemote(t, name, filepath.Join(root, name))
			s := scanned(t, left, right, transport.NewLocalBackend(left), remote)
			if p := s.copy(t, "", true, false); p.Failed.Load() != 0 {
				t.Fatalf("copy to %s: %d failed", name, p.Failed.Load())
			}
			for rel, target := range links {
				wantLink(t, filepath.Join(right, rel), munged(name, target))
			}

			s = scanned(t, left, right, transport.NewLocalBackend(left), remote)
			for rel := range links {
				if st := linkStatus(t, s, rel); st != model.AttrEqual {
					t.Errorf("%s after the copy: status %v, want equal", rel, st)
				}
			}

			linkTo(t, filepath.Join(outside, "keep.txt"), filepath.Join(right, "sub", "extra.lnk"))
			s = scanned(t, left, right, transport.NewLocalBackend(left), remote)
			if p := s.copy(t, "", true, true); p.Failed.Load() != 0 {
				t.Fatalf("mirror to %s: %d failed", name, p.Failed.Load())
			}
			if onDisk(filepath.Join(right, "sub", "extra.lnk")) {
				t.Errorf("the mirror to %s left sub/extra.lnk, a link only there", name)
			}
			if readFile(t, filepath.Join(outside, "keep.txt")) != "keep me" {
				t.Errorf("the mirror to %s changed what sub/extra.lnk names", name)
			}
			for rel, target := range links {
				wantLink(t, filepath.Join(right, rel), munged(name, target))
			}

			back := filepath.Join(root, name+"-back")
			if err := os.MkdirAll(back, 0o755); err != nil {
				t.Fatal(err)
			}
			s = scanned(t, back, right, transport.NewLocalBackend(back), remote)
			if p := s.copy(t, "", false, false); p.Failed.Load() != 0 {
				t.Fatalf("copy from %s: %d failed", name, p.Failed.Load())
			}
			for rel, target := range links {
				wantLink(t, filepath.Join(back, rel), target)
			}
		})
	}
}
