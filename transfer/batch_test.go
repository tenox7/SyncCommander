package transfer

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"sc/internal/sshtest"
	"sc/transport"
)

// T1: a folder copied to rsync+ssh:// goes in one rsync session (-batch, on
// by default), and no test ran one. These do, through the wrapper the
// program opens a remote with: the folder lands whole, a file in the way
// that the user confirmed is removed first, one not confirmed stops the
// batch and stays, and a mirror after a batch deletes what its plan holds.

// batchScenario scans a local left against right over rsync+ssh, with
// -batch on.
func batchScenario(t *testing.T, left, right string) *scenario {
	t.Helper()
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	dst, err := transport.TryOpenBackend("rsync+ssh://x:pw@"+sshtest.Start(t, false)+right, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(dst) })
	s := scanned(t, left, right, transport.NewLocalBackend(left), dst)
	s.batch = true
	return s
}

func TestScenarioBatchSendsAFolderInOneSession(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "f.txt"), "the file")
	writeFile(t, filepath.Join(left, "d", "e", "g.txt"), "another")
	writeFile(t, filepath.Join(left, "outside.txt"), "not in d")
	// An empty folder is on the copy's list too: it goes, and is no file.
	if err := os.MkdirAll(filepath.Join(left, "d", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	s := batchScenario(t, left, right)
	mark := len(transport.Log.Lines())
	p := s.copy(t, "d", true, false)
	if p.Failed.Load() != 0 || p.Done.Load() != 2 || p.Total.Load() != 2 {
		t.Errorf("failed=%d done=%d total=%d, want 0, 2, 2", p.Failed.Load(), p.Done.Load(), p.Total.Load())
	}
	if !logged(mark, "BATCH d (2 files") || !logged(mark, "BATCH d OK") {
		t.Errorf("the log has no BATCH d lines for 2 files: %q", transport.Log.Lines()[mark:])
	}
	wantTree(t, "after the batch", right, "d/", "d/e/", "d/e/g.txt", "d/empty/", "d/f.txt")
	for rel, body := range map[string]string{"d/f.txt": "the file", "d/e/g.txt": "another"} {
		if got := readFile(t, filepath.Join(right, rel)); got != body {
			t.Errorf("%s = %q, want %q", rel, got, body)
		}
	}
}

// A file where the source has a folder is in the batch's way. Confirmed,
// it is removed before the session sends the folder.
func TestScenarioBatchRemovesAConfirmedFileInTheWayFirst(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "f.txt"), "f")
	writeFile(t, filepath.Join(left, "d", "x", "a.txt"), "under x")
	writeFile(t, filepath.Join(right, "d", "x"), "a file in the way")
	s := batchScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "d", true, false, "d/x"); p.Failed.Load() != 0 {
		t.Errorf("failed=%d, want 0", p.Failed.Load())
	}
	if !logged(mark, "removed file d/x to put the copy there") || !logged(mark, "BATCH d OK") {
		t.Error("the log does not say the file in the way went and the batch was sent")
	}
	wantTree(t, "after the batch", right, "d/", "d/f.txt", "d/x/", "d/x/a.txt")
}

// Not confirmed, the file in the way stops the batch: the folder goes file
// by file, the file under x fails, and the file in the way stays.
func TestScenarioBatchLeavesAnUnconfirmedFileInTheWay(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "f.txt"), "f")
	writeFile(t, filepath.Join(left, "d", "x", "a.txt"), "under x")
	writeFile(t, filepath.Join(right, "d", "x"), "a file in the way")
	s := batchScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "d", true, false); p.Failed.Load() != 1 {
		t.Errorf("failed=%d, want 1: the file under x", p.Failed.Load())
	}
	if logged(mark, "BATCH d (") {
		t.Error("a batch was sent with a file in its way that the user did not confirm")
	}
	wantTree(t, "after the copy", right, "d/", "d/f.txt", "d/x")
	if got := readFile(t, filepath.Join(right, "d", "x")); got != "a file in the way" {
		t.Errorf("d/x = %q, want it kept", got)
	}
}

// The batch sends without --delete; the mirror after it deletes what the
// plan holds, and only that.
func TestScenarioBatchThenMirrorDeletesThePlannedFiles(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "f.txt"), "f")
	writeFile(t, filepath.Join(right, "d", "old.txt"), "only on the destination")
	writeFile(t, filepath.Join(right, "other.txt"), "outside d")
	s := batchScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "d", true, true); p.Failed.Load() != 0 {
		t.Errorf("failed=%d, want 0", p.Failed.Load())
	}
	if !logged(mark, "BATCH d OK") || !logged(mark, "mirror delete d/old.txt") {
		t.Error("the log does not show the batch and then the mirror delete")
	}
	wantTree(t, "after the mirror", right, "d/", "d/f.txt", "other.txt")
}

// sameTimes gives the files one mtime on both sides, an hour ago.
func sameTimes(t *testing.T, rels []string, sides ...string) time.Time {
	t.Helper()
	when := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, side := range sides {
		for _, rel := range rels {
			if err := os.Chtimes(filepath.Join(side, filepath.FromSlash(rel)), when, when); err != nil {
				t.Fatal(err)
			}
		}
	}
	return when
}

// F20: the batch sends what the copy would send file by file, and only
// that. twin.txt has its size and mtime on both sides, so the scan holds it
// as equal, though its bytes differ. One session over the whole folder, as
// before, sent it: rsync's -c compares the bytes.
func TestScenarioBatchSendsOnlyTheFilesThatDiffer(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "new.txt"), "only on the left")
	writeFile(t, filepath.Join(left, "d", "changed.txt"), "the new version")
	writeFile(t, filepath.Join(right, "d", "changed.txt"), "old")
	writeFile(t, filepath.Join(left, "d", "twin.txt"), "left body")
	writeFile(t, filepath.Join(right, "d", "twin.txt"), "rght body")
	sameTimes(t, []string{"d/twin.txt"}, left, right)
	sameTimes(t, []string{"d/changed.txt"}, right) // older than the left one
	s := batchScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "d", true, false); p.Failed.Load() != 0 || p.Total.Load() != 2 {
		t.Errorf("failed=%d total=%d, want 0 and 2", p.Failed.Load(), p.Total.Load())
	}
	if !logged(mark, "BATCH d (2 files") || !logged(mark, "BATCH d OK") {
		t.Errorf("the batch did not send the two files that differ: %q", transport.Log.Lines()[mark:])
	}
	for rel, body := range map[string]string{"d/new.txt": "only on the left", "d/changed.txt": "the new version", "d/twin.txt": "rght body"} {
		if got := readFile(t, filepath.Join(right, rel)); got != body {
			t.Errorf("%s = %q, want %q", rel, got, body)
		}
	}
}

// With nothing that differs there is nothing to send, and no session runs.
func TestScenarioBatchDoesNotRunWhenNothingDiffers(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	for _, side := range []string{left, right} {
		writeFile(t, filepath.Join(side, "d", "a.txt"), "same")
	}
	sameTimes(t, []string{"d/a.txt"}, left, right)
	s := batchScenario(t, left, right)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "d", true, false); p.Failed.Load() != 0 {
		t.Errorf("failed=%d, want 0", p.Failed.Load())
	}
	if logged(mark, "BATCH d") {
		t.Errorf("a batch ran with nothing to send: %q", transport.Log.Lines()[mark:])
	}
}
