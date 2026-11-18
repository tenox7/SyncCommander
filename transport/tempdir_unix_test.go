//go:build linux || darwin || freebsd

package transport

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// holdLock takes the lock in home through an open of its own, as a
// running sc holds it.
func holdLock(t *testing.T, home string) {
	t.Helper()
	f, err := os.Open(filepath.Join(home, tempLockName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := lockFile(f); err != nil {
		t.Fatal(err)
	}
}

// lockFree reports whether the lock in home can be taken now: no process
// holds it, and a sweep would remove home.
func lockFree(t *testing.T, home string) bool {
	t.Helper()
	f, err := os.Open(filepath.Join(home, tempLockName))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	return lockFile(f) == nil
}

func gone(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return err != nil
}

// R16: a spool holds a whole file, and an sc that was killed left it in the
// temp folder for good. Now each sc keeps its temp folders in a home of its
// own, locked while it runs, and the first temp folder an sc makes removes
// the homes whose lock is free: their sc ended. Nothing else goes.
func TestTempDirSweepsWhatEndedProcessesLeft(t *testing.T) {
	tmp, outside := t.TempDir(), t.TempDir()
	setTemp(t, tmp)
	home := func(name string, lock bool) {
		writeTestFile(t, tmp, name+"/rsync-upload-1/spool", "a whole file")
		if lock {
			writeTestFile(t, tmp, name+"/"+tempLockName, "")
		}
	}
	home("sc-tmp-ended", true) // its sc was killed
	home("sc-tmp-live", true)  // its sc runs
	holdLock(t, filepath.Join(tmp, "sc-tmp-live"))
	home("sc-tmp-nolock", false)  // being made, or on a disk with no locks
	home("other-tmp-1", true)     // not sc's
	home("rsync-upload-2", false) // an older sc's, made straight in the temp folder
	writeTestFile(t, outside, "sc-tmp-x/"+tempLockName, "")
	writeTestFile(t, outside, "sc-tmp-x/keep", "behind a link")
	if err := os.Symlink(filepath.Join(outside, "sc-tmp-x"), filepath.Join(tmp, "sc-tmp-link")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, tmp, "sc-tmp-file", "a file, not a home")

	dir, err := tempDir("rsync-upload-*")
	if err != nil {
		t.Fatal(err)
	}
	mine := filepath.Dir(dir)
	if filepath.Dir(mine) != tmp || !strings.HasPrefix(filepath.Base(mine), tempHomePrefix) {
		t.Fatalf("tempDir made %s: not in a home of this process's in %s", dir, tmp)
	}
	if lockFree(t, mine) {
		t.Error("this process's home is not locked: another sc would sweep it")
	}
	if !gone(t, filepath.Join(tmp, "sc-tmp-ended")) {
		t.Error("the home of an sc that ended is still there")
	}
	for _, kept := range []string{"sc-tmp-live/rsync-upload-1/spool", "sc-tmp-nolock/rsync-upload-1/spool", "other-tmp-1/rsync-upload-1/spool", "rsync-upload-2/rsync-upload-1/spool", "sc-tmp-link", "sc-tmp-file"} {
		if !exists(tmp, kept) {
			t.Errorf("the sweep removed %s", kept)
		}
	}
	if !exists(outside, "sc-tmp-x/keep") {
		t.Error("the sweep removed a file behind a link")
	}

	// It sweeps once: a home left after that waits for the next sc, and no
	// spool lists the temp folder again.
	home("sc-tmp-ended2", true)
	if _, err := tempDir("rsync-upload-*"); err != nil {
		t.Fatal(err)
	}
	if gone(t, filepath.Join(tmp, "sc-tmp-ended2")) {
		t.Error("a second temp folder swept the temp folder again")
	}
	f, err := tempFile("sc-rclone-*")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if filepath.Dir(f.Name()) != mine {
		t.Errorf("tempFile made %s, not in this process's home %s", f.Name(), mine)
	}
}

// The same, with a real sc killed: the system lets go of its lock, and the
// next sc removes what it left; while it ran, its home stayed.
func TestTempHomeOfAKilledSc(t *testing.T) {
	if os.Getenv("SC_TEMP_CHILD") != "" {
		dir, err := tempDir("rsync-upload-*")
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, "spool"), []byte("a whole file"), 0o600)
		}
		if err != nil {
			fmt.Println("ERR", err)
			os.Exit(1)
		}
		fmt.Println("DIR", dir)
		io.Copy(io.Discard, os.Stdin) // until it is killed
		os.Exit(0)
	}
	tmp := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestTempHomeOfAKilledSc$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SC_TEMP_CHILD=1", "TMPDIR="+tmp)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		stdin.Close()
		cmd.Wait()
	})
	var dir string
	for sc := bufio.NewScanner(out); dir == "" && sc.Scan(); {
		if rest, ok := strings.CutPrefix(sc.Text(), "DIR "); ok {
			dir = rest
		} else if strings.HasPrefix(sc.Text(), "ERR ") {
			t.Fatal("the child: " + sc.Text())
		}
	}
	if dir == "" || !strings.HasPrefix(dir, tmp+string(filepath.Separator)) {
		t.Fatalf("the child made no temp folder in %s: %q", tmp, dir)
	}
	sweepTempHomes(tmp)
	if gone(t, dir) {
		t.Fatal("a sweep removed the temp folder of a running sc")
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	setTemp(t, tmp)
	mine, err := tempDir("rsync-upload-*")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(mine)
	if !gone(t, dir) {
		t.Errorf("the spool of a killed sc is still there: %s", dir)
	}
	if left := leftInTemp(t, tmp); len(left) > 0 {
		t.Errorf("the temp folder holds %v", left)
	}
}

// A cleaner that removes folders not used for days took the home of an sc
// that ran that long: every spool after it failed. A new home takes its
// place, locked like the first.
func TestTempHomeComesBack(t *testing.T) {
	tmp := t.TempDir()
	setTemp(t, tmp)
	dir, err := tempDir("rsync-upload-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	dir, err = tempDir("rsync-upload-*")
	if err != nil {
		t.Fatalf("tempDir after the home went: %v", err)
	}
	if lockFree(t, filepath.Dir(dir)) {
		t.Error("the new home is not locked")
	}
	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	f, err := tempFile("sc-rclone-*")
	if err != nil {
		t.Fatalf("tempFile after the home went: %v", err)
	}
	f.Close()
}

// A sweep that cannot remove all of a home keeps its lock to the end, so
// the next sc tries again: without the lock the home was nobody's.
func TestTempSweepRemovesTheLockLast(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root removes everything")
	}
	tmp := t.TempDir()
	for _, rel := range []string{tempLockName, "a/spool", "stuck/spool", "z/spool"} {
		writeTestFile(t, tmp, "sc-tmp-ended/"+rel, "")
	}
	chmodTest(t, filepath.Join(tmp, "sc-tmp-ended/stuck"), 0o500)
	sweepTempHomes(tmp)
	if !exists(tmp, "sc-tmp-ended/"+tempLockName) {
		t.Fatal("a sweep that could not remove a home removed its lock")
	}
	os.Chmod(filepath.Join(tmp, "sc-tmp-ended/stuck"), 0o755)
	sweepTempHomes(tmp)
	if !gone(t, filepath.Join(tmp, "sc-tmp-ended")) {
		t.Error("the next sweep did not remove the home")
	}
}

// main sweeps as sc starts, so the room comes back even when nothing
// spools; it makes no home of its own.
func TestSweepTemp(t *testing.T) {
	tmp := t.TempDir()
	setTemp(t, tmp)
	writeTestFile(t, tmp, "sc-tmp-ended/"+tempLockName, "")
	writeTestFile(t, tmp, "sc-tmp-ended/rsync-upload-1/spool", "a whole file")
	SweepTemp()
	if ents, err := os.ReadDir(tmp); err != nil || len(ents) > 0 {
		t.Errorf("after SweepTemp the temp folder holds %v, %v", ents, err)
	}
}
