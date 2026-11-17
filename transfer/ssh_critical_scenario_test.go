package transfer

// Scenario tests for the ssh items of the review's critical list: S1, S2 and
// S3, each as a user meets it. An in-process ssh server runs the real shell
// tools on this machine; the real scanner and copy engine, and the
// connection wrapper the program opens remote URLs with.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sc/internal/sshtest"
	"sc/model"
	"sc/transport"
)

// sshScenario scans the remote folder dir, which url names, on the left,
// against the local folder local, on the right.
func sshScenario(t *testing.T, url, dir, local string) *scenario {
	t.Helper()
	rb, err := transport.TryOpenBackend(url, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(rb) })
	return scanned(t, dir, local, rb, transport.NewLocalBackend(local))
}

// S1: over ssh, a remote folder sc could not read, or a base path typed
// wrong, read as empty, and a mirror to the local side deleted its copy.
func TestScenarioSSHUnreadableFolderMirror(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	for _, tc := range []struct {
		scheme string
		sftp   bool
	}{{"scp", false}, {"sftp", true}} {
		t.Run(tc.scheme, func(t *testing.T) {
			addr := sshtest.Start(t, tc.sftp)
			remote, local := t.TempDir(), t.TempDir()
			for _, side := range []string{remote, local} {
				writeFile(t, filepath.Join(side, "photos", "a.jpg"), "same on both sides")
				writeFile(t, filepath.Join(side, "photos", "b.jpg"), "same on both sides")
				writeFile(t, filepath.Join(side, "docs", "c.txt"), "same on both sides")
			}
			lockDir(t, filepath.Join(remote, "photos"), 0o000)
			want := []string{"docs/", "docs/c.txt", "photos/", "photos/a.jpg", "photos/b.jpg"}

			s := sshScenario(t, tc.scheme+"://x:pw@"+addr+remote, remote, local)
			if !s.node(t, "photos").ListErr {
				t.Error("photos is not flagged as a failed listing")
			}
			for _, rel := range []string{"photos", ""} {
				if p := s.copy(t, rel, true, true); p.Failed.Load() == 0 {
					t.Errorf("a mirror of %q from an unreadable folder did not fail", rel)
				}
			}
			wantTree(t, "the local side after the mirrors", local, want...)

			typo := remote + "-typo"
			s = sshScenario(t, tc.scheme+"://x:pw@"+addr+typo, typo, local)
			if !s.sc.Tree().ListErr {
				t.Error("a base path that is not there is not flagged as a failed listing")
			}
			if p := s.copy(t, "", true, true); p.Failed.Load() == 0 {
				t.Error("a mirror from a base path that is not there did not fail")
			}
			wantTree(t, "the local side after a mirror from a base path typed wrong", local, want...)
		})
	}
}

// S3: over scp:// and rsync+ssh://, a base folder that is a symlink listed
// as empty with GNU find, and a mirror to the local side deleted all of it.
// (The shell backends list with perl where find lacks GNU's options, as on
// macOS; perl followed the link.)
func TestScenarioSSHLinkedBaseMirror(t *testing.T) {
	for _, scheme := range []string{"scp", "rsync+ssh"} {
		t.Run(scheme, func(t *testing.T) {
			if _, err := exec.LookPath("rsync"); err != nil && scheme == "rsync+ssh" {
				t.Skip("no rsync binary")
			}
			addr := sshtest.Start(t, false)
			dir, local := t.TempDir(), t.TempDir()
			real, link := filepath.Join(dir, "real"), filepath.Join(dir, "link")
			for _, side := range []string{real, local} {
				writeFile(t, filepath.Join(side, "a.txt"), "same on both sides")
				writeFile(t, filepath.Join(side, "sub", "b.txt"), "same on both sides")
			}
			if err := os.Symlink(real, link); err != nil {
				t.Fatal(err)
			}
			s := sshScenario(t, scheme+"://x:pw@"+addr+link, link, local)
			if p := s.copy(t, "", true, true); p.Failed.Load() != 0 {
				t.Errorf("the mirror failed %d files", p.Failed.Load())
			}
			wantTree(t, "the local side after a mirror from a linked base", local, "a.txt", "sub/", "sub/b.txt")
		})
	}
}

// S2: with no path in the URL, sc asks the server for the login home. When
// that failed the base became "/"; an empty answer became "", which put
// files in the remote shell's folder, and rsync+ssh listed the server from
// "/" down. Now the connect fails and says to write the full path, and a
// copy fails. The test server runs commands in this process's folder, with
// its environment: an empty HOME is an empty answer, and the folder is a
// temp folder. The scan is shallow, and the copy of the top row runs only
// once the connect has failed as it must, and never as a mirror: on the old
// code it listed the server from "/" down, and a mirror deletes.
func TestScenarioSSHHomeLookupFailure(t *testing.T) {
	t.Setenv("HOME", "")
	cwd := t.TempDir()
	t.Chdir(cwd)
	oneRetry(t)
	addr := sshtest.Start(t, false)
	local := t.TempDir()
	writeFile(t, filepath.Join(local, "up.txt"), "for the home folder")
	for _, scheme := range []string{"ssh", "rsync+ssh"} {
		t.Run(scheme, func(t *testing.T) {
			if _, err := exec.LookPath("rsync"); err != nil && scheme == "rsync+ssh" {
				t.Skip("no rsync binary")
			}
			ctx := context.Background()
			rb, err := transport.TryOpenBackend(scheme+"://x:pw@"+addr, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { transport.CloseBackend(rb) })
			if _, err := rb.List(ctx, ""); err == nil || !strings.Contains(err.Error(), "full remote path") {
				t.Fatalf("listing a side with no home folder: %v; want the connect to fail and say to write the full remote path", err)
			}
			s := &scenario{left: local, lb: transport.NewLocalBackend(local), rb: rb}
			s.sc = model.NewScanner(s.lb, rb, 1, 1, false)
			s.sc.Scan(ctx, scenarioOpts)
			if !s.sc.Tree().ListErr {
				t.Error("with no home folder, the remote side is not flagged as a failed listing")
			}
			if p := s.copy(t, "", true, false); p.Failed.Load() == 0 {
				t.Error("a copy to a side with no home folder did not fail")
			}
			wantTree(t, "the remote shell's folder after the copy", cwd)
		})
	}
}
