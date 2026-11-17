package transfer

// Scenario tests for the rsync items of the review's critical list: R1, R2,
// R4 and R5, each as a user meets it. A stock rsync daemon on the right and
// a local folder on the left, the real scanner and copy engine, and the
// connection wrapper the program opens remote URLs with (rsyncScenario).

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/transport"
)

// oneRetry has the connection wrapper retry a failed call once, not five
// times with growing waits, until the test ends.
func oneRetry(t *testing.T) {
	t.Helper()
	retries := transport.MaxRetries()
	transport.SetMaxRetries(1)
	t.Cleanup(func() { transport.SetMaxRetries(retries) })
}

// lockDir takes write away from dir until the test ends.
func lockDir(t *testing.T, dir string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(dir, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
}

// contentOf is what the file p holds, or what keeps it from being read.
func contentOf(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(b)
}

// wantContent checks that each file under root holds what files says.
func wantContent(t *testing.T, what, root string, files map[string]string) {
	t.Helper()
	for rel, want := range files {
		if got := contentOf(filepath.Join(root, filepath.FromSlash(rel))); got != want {
			t.Errorf("%s: %s holds %q, want %q", what, rel, got, want)
		}
	}
}

// copyOne copies the file rel as the user does and returns whether the
// copy counted it as failed. A copy that did not fail must have put the
// source's bytes on the destination.
func (s *scenario) copyOne(t *testing.T, rel string, leftToRight bool, srcDir, dstDir string) (failed bool) {
	t.Helper()
	want := contentOf(filepath.Join(srcDir, filepath.FromSlash(rel)))
	if s.copy(t, rel, leftToRight, false).Failed.Load() > 0 {
		return true
	}
	if got := contentOf(filepath.Join(dstDir, filepath.FromSlash(rel))); got != want {
		t.Errorf("the copy of %s did not fail, but the destination holds %q, not the source's %q", rel, got, want)
	}
	return false
}

// R1: a daemon folder whose name holds [ ], or ends in " <module>", was sent
// to the daemon as a pattern, or split in two. A download got the files of
// d1 for d[1], and "old backups/x.txt" from two other paths; a mirror
// deleted in d1 what it meant to delete in d[1].
func TestScenarioRsyncNamesThatLookLikePatterns(t *testing.T) {
	url, mod := startRsyncd(t)
	files := map[string]string{
		"d[1]/a.txt":        "in d[1]",
		"d1/a.txt":          "in d1",
		"d1/stale.txt":      "in d1, on both sides",
		"old backups/x.txt": "in old backups",
		"old/x.txt":         "in old",
		"x.txt":             "at the top",
	}
	for rel, body := range files {
		writeFile(t, filepath.Join(mod, filepath.FromSlash(rel)), body)
	}
	local := t.TempDir()

	// Download all of it into an empty folder.
	s := rsyncScenario(t, local, url, mod)
	if p := s.copy(t, "", false, false); p.Failed.Load() != 0 {
		t.Errorf("the download failed %d files", p.Failed.Load())
	}
	wantContent(t, "the local side after the download", local, files)

	// Two files are only on the daemon now. A mirror from the local side
	// deletes those two and nothing else.
	writeFile(t, filepath.Join(mod, "d[1]", "stale.txt"), "only on the daemon")
	writeFile(t, filepath.Join(mod, "old backups", "stale.txt"), "only on the daemon")
	s = rsyncScenario(t, local, url, mod)
	s.copy(t, "", true, true)
	wantTree(t, "the daemon after the mirror", mod, "d1/", "d1/a.txt", "d1/stale.txt", "d[1]/", "d[1]/a.txt",
		"old backups/", "old backups/x.txt", "old/", "old/x.txt", "x.txt")
	wantContent(t, "the daemon after the mirror", mod, files)
}

// R2: deleting a folder with a backslash in its name deleted a sibling: the
// delete rule for a\b also named ab. A mirror deletes the names only on the
// daemon, and keeps their look-alikes.
func TestScenarioRsyncMirrorDeletesBackslashNames(t *testing.T) {
	url, mod := startRsyncd(t)
	local := t.TempDir()
	both := map[string]string{"bs/ab/f.txt": "ab", "bs/ab/sub/deep.txt": "ab deep", "bs/xy.txt": "xy", "bs/mn.txt": "mn"}
	for rel, body := range both {
		writeFile(t, filepath.Join(mod, filepath.FromSlash(rel)), body)
		writeFile(t, filepath.Join(local, filepath.FromSlash(rel)), body)
	}
	for _, rel := range []string{`bs/a\b/f.txt`, `bs/a\b/sub/deep.txt`, `bs/x\y.txt`, `bs/m\[n.txt`} {
		writeFile(t, filepath.Join(mod, filepath.FromSlash(rel)), "only on the daemon")
	}
	s := rsyncScenario(t, local, url, mod)
	if p := s.copy(t, "", true, true); p.Failed.Load() != 0 {
		t.Errorf("the mirror failed %d files", p.Failed.Load())
	}
	wantTree(t, "the daemon after the mirror", mod, "bs/", "bs/ab/", "bs/ab/f.txt", "bs/ab/sub/", "bs/ab/sub/deep.txt", "bs/mn.txt", "bs/xy.txt")
	wantContent(t, "the daemon after the mirror", mod, both)
}

// mirrorDeleted says whether the log since mark says relPath was deleted.
func mirrorDeleted(mark int, relPath string) bool {
	for _, line := range transport.Log.Lines()[mark:] {
		if strings.HasSuffix(line, " mirror delete "+relPath) {
			return true
		}
	}
	return false
}

// refusingDaemon runs a daemon whose module refuses some of what a user
// does: it takes no file under blocked/ (its own filter), its folder ro/
// has no write permission (a delete there fails; stock rsync makes a
// folder writable for a moment to put a file in it), locked.bin has none
// either, and it cannot read unreadable.bin. auth adds the module's auth
// lines.
func refusingDaemon(t *testing.T, auth func(root string) string) (port int, mod string) {
	t.Helper()
	oneRetry(t)
	port, root := startRsyncdWith(t, func(root string) string {
		return "[backups]\n  path = " + filepath.Join(root, "module") + "\n  read only = no\n  exclude = /blocked/\n" + auth(root)
	})
	mod = filepath.Join(root, "module")
	old := time.Now().Add(-time.Hour)
	for rel, body := range map[string]string{"ro/keep.txt": "on both sides", "ro/only-here.txt": "only on the daemon",
		"locked.bin": "OLD CONTENT", "unreadable.bin": "the daemon cannot read this"} {
		p := filepath.Join(mod, filepath.FromSlash(rel))
		writeFile(t, p, body)
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	lockDir(t, filepath.Join(mod, "ro"), 0o555)
	lockDir(t, filepath.Join(mod, "locked.bin"), 0o444)
	lockDir(t, filepath.Join(mod, "unreadable.bin"), 0o000)
	return port, mod
}

// refusedLocal is the local side for refusingDaemon: files to upload into
// blocked/ and over locked.bin, and ro/ without the daemon's extra file.
func refusedLocal(t *testing.T) string {
	t.Helper()
	local := t.TempDir()
	writeFile(t, filepath.Join(local, "blocked", "x.txt"), "the daemon takes nothing here")
	writeFile(t, filepath.Join(local, "locked.bin"), "NEW CONTENT")
	writeFile(t, filepath.Join(local, "ro", "keep.txt"), "on both sides")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(local, "ro", "keep.txt"), old, old); err != nil {
		t.Fatal(err)
	}
	return local
}

// R4: uploads, downloads and deletes the daemon refused were done, as far as
// the user could see: no failure, and the mirror logged a delete that did
// not happen. A copy now fails, or the destination holds what was copied;
// a delete is logged as done only when done.
func TestScenarioRsyncRefusedTransfersFail(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root may write and read anything")
	}
	port, mod := refusingDaemon(t, func(string) string { return "" })
	local := refusedLocal(t)
	s := rsyncScenario(t, local, fmt.Sprintf("rsync://127.0.0.1:%d/backups", port), mod)

	if !s.copyOne(t, "blocked/x.txt", true, local, mod) {
		t.Error("an upload the daemon's filter refuses did not fail")
	}
	s.copyOne(t, "locked.bin", true, local, mod)
	if !s.copyOne(t, "unreadable.bin", false, mod, local) {
		t.Error("a download of a file the daemon cannot read did not fail")
	}
	mark := len(transport.Log.Lines())
	s.copy(t, "ro", true, true)
	if mirrorDeleted(mark, "ro/only-here.txt") {
		t.Error("a mirror delete the daemon refused was logged as done")
	}
	if _, err := os.Stat(filepath.Join(mod, "ro", "only-here.txt")); err != nil {
		t.Errorf("ro/only-here.txt on the daemon: %v", err)
	}
	wantTree(t, "the local side", local, "blocked/", "blocked/x.txt", "locked.bin", "ro/", "ro/keep.txt")
}

// R5: the daemon password went into error text and into the log, with the
// command line gorsync wrote out. Copies the daemon refuses, and a scan with
// a wrong password, log their failures without it.
func TestScenarioRsyncPasswordStaysOutOfTheLog(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root may write and read anything")
	}
	const secret = "Sekr1tR5pw"
	port, mod := refusingDaemon(t, func(root string) string {
		secrets := filepath.Join(root, "secrets")
		if err := os.WriteFile(secrets, []byte("carol:"+secret+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return "  auth users = carol\n  secrets file = " + secrets + "\n"
	})
	local := refusedLocal(t)
	url := func(pass string) string { return fmt.Sprintf("rsync://carol:%s@127.0.0.1:%d/backups", pass, port) }

	s := rsyncScenario(t, local, url(secret), mod)
	if !s.copyOne(t, "blocked/x.txt", true, local, mod) {
		t.Error("an upload the daemon's filter refuses did not fail")
	}
	if !s.copyOne(t, "unreadable.bin", false, mod, local) {
		t.Error("a download of a file the daemon cannot read did not fail")
	}
	if bad := rsyncScenario(t, local, url(secret+"-wrong"), mod); !bad.sc.Tree().ListErr {
		t.Error("a scan with a wrong password did not fail")
	}
	for _, line := range transport.Log.Lines() {
		if strings.Contains(line, secret) {
			t.Errorf("the log holds the password: %s", line)
		}
	}
}
