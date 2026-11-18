package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/internal/sshtest"
)

// makeTree writes 30 files in three folders under root/d and returns them.
func makeTree(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	for i := range 30 {
		p := filepath.Join(root, "d", fmt.Sprintf("s%d", i%3), fmt.Sprintf("f%02d.txt", i))
		writeFile(t, p, "x")
		paths = append(paths, p)
	}
	return paths
}

// countSent counts the commands proto sent since mark that hold what.
func countSent(mark int, proto, what string) int {
	n := 0
	for _, line := range sentSince(mark, proto) {
		if strings.Contains(line, what) {
			n++
		}
	}
	return n
}

// S19: SFTP removed a folder with a request for each entry, a round trip
// each. With a login shell that runs commands it is one rm -rf; whether
// the shell runs them is asked once for the connection.
func TestSFTPRemoveAllIsOneCommand(t *testing.T) {
	root := t.TempDir()
	b, err := OpenBackend("sftp://x:pw@"+startExecSFTPServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	mark := len(Log.Lines())
	for range 2 {
		makeTree(t, root)
		if err := b.RemoveAll(ctx, "d"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(root, "d")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("d is still there: %v", err)
		}
	}
	if rms, checks := countSent(mark, "sftp", "rm -rf "), countSent(mark, "sftp", ">>> echo "); rms != 2 || checks != 1 {
		t.Errorf("two removes sent %d rm -rf and %d shell checks; want 2 and 1: %q", rms, checks, sentSince(mark, "sftp"))
	}
}

// A server that runs the sftp server for every command, as ForceCommand
// internal-sftp does, is never sent rm -rf, which would wait there on its
// input for good: the folder goes by SFTP requests.
func TestSFTPRemoveAllWithNoShell(t *testing.T) {
	root := t.TempDir()
	b, err := OpenBackend("sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{OnlySFTP: true})+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mark := len(Log.Lines())
	for range 2 {
		makeTree(t, root)
		if err := b.RemoveAll(ctx, "d"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(root, "d")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("d is still there: %v", err)
		}
	}
	if rms, checks := countSent(mark, "sftp", "rm -rf "), countSent(mark, "sftp", ">>> echo "); rms != 0 || checks != 1 {
		t.Errorf("two removes sent %d rm -rf and %d shell checks; want 0 and 1: %q", rms, checks, sentSince(mark, "sftp"))
	}
}

// S19: sshd leaves a command that reads and writes nothing running when
// its client goes, so a cancelled delete went on removing on the server.
// The rm is stopped when its session closes: what it had not removed
// stays. Here rm waits a second before it removes anything.
func TestRemoveAllStopsOnTheServerWhenCancelled(t *testing.T) {
	realRm, err := exec.LookPath("rm")
	if err != nil {
		t.Skip("no rm")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "rm"), []byte("#!/bin/sh\nsleep 1\nexec "+realRm+" \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if out, _ := exec.Command("sh", "-c", "command -v rm").Output(); strings.TrimSpace(string(out)) != filepath.Join(bin, "rm") {
		t.Skipf("this sh runs %q for rm, not the one on its PATH", out)
	}
	// scp:// takes the sftp subsystem when the server has one.
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	for _, scheme := range []string{"scp", "sftp"} {
		t.Run(scheme, func(t *testing.T) {
			root := t.TempDir()
			paths := makeTree(t, root)
			b, err := OpenBackend(scheme+"://x:pw@"+servers[scheme]+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if _, isSCP := b.(*SCPBackend); scheme == "scp" && !isSCP {
				t.Fatalf("scp:// opened a %T, want the SCPBackend", b)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			time.AfterFunc(300*time.Millisecond, cancel)
			start := time.Now()
			err = b.RemoveAll(ctx, "d")
			if took := time.Since(start); err == nil || took > 900*time.Millisecond {
				t.Errorf("a cancelled RemoveAll gave %v after %v; want an error at once", err, took)
			}
			time.Sleep(1500 * time.Millisecond)
			for _, p := range paths {
				if _, err := os.Lstat(p); err != nil {
					t.Fatalf("%s went after the cancel: %v", p, err)
				}
			}
		})
	}
}

// A shell with no rm, as a restricted one: SFTP removes the folder by its
// requests. A shell with nothing but sh cannot even look at the folder, so
// it is not used, and it is sent no rm (S25).
func TestSFTPRemoveAllWithNoRm(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rms   int
		shell func(t *testing.T)
	}{
		{"no rm", 1, func(t *testing.T) {
			fakeCommand(t, "rm", `echo "sh: rm: not found" >&2; exit 127`)
		}},
		{"nothing but sh", 0, func(t *testing.T) {
			sh, err := exec.LookPath("sh")
			if err != nil {
				t.Skip("no sh")
			}
			bin := t.TempDir()
			if err := os.Symlink(sh, filepath.Join(bin, "sh")); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin)
			if out, _ := exec.Command("sh", "-c", "command -v rm").Output(); len(out) > 0 {
				t.Skipf("this sh finds rm with no rm on its PATH: %s", out)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.shell(t)
			root := t.TempDir()
			makeTree(t, root)
			b, err := OpenBackend("sftp://x:pw@"+startExecSFTPServer(t)+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			mark := len(Log.Lines())
			if err := b.RemoveAll(context.Background(), "d"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(root, "d")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("d is still there: %v", err)
			}
			if n := countSent(mark, "sftp", "rm -rf "); n != tc.rms {
				t.Errorf("%d rm -rf sent, want %d", n, tc.rms)
			}
		})
	}
}

// A shell that takes the command and does nothing, as a restricted one may:
// the delete is not taken for one that went through. Over scp it fails,
// saying so; SFTP removes the folder by its requests.
func TestRemoveAllSeesAnRmThatDidNotRun(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	bin := t.TempDir()
	fake := "#!" + sh + "\ncase \"$2\" in *\"rm -rf \"*) exit 0;; esac\nexec " + sh + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "sh"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	for _, scheme := range []string{"scp", "sftp"} {
		t.Run(scheme, func(t *testing.T) {
			root := t.TempDir()
			makeTree(t, root)
			b, err := OpenBackend(scheme+"://x:pw@"+servers[scheme]+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			err = b.RemoveAll(context.Background(), "d")
			_, lerr := os.Lstat(filepath.Join(root, "d"))
			gone := errors.Is(lerr, fs.ErrNotExist)
			switch scheme {
			case "scp":
				if err == nil || gone {
					t.Errorf("RemoveAll gave %v, the folder gone: %v; want an error, and the folder there", err, gone)
				}
			case "sftp":
				if err != nil || !gone {
					t.Errorf("RemoveAll gave %v, the folder gone: %v; want it gone by SFTP", err, gone)
				}
			}
		})
	}
}
