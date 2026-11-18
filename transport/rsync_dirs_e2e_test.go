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

	"sc/model"
)

// rsyncDaemonsUnderTest starts a writable module on stock rsync and on
// gorsyncd, and gives its URL and folder.
var rsyncDaemonsUnderTest = map[string]func(*testing.T) (url, mod string){
	"stock rsync": func(t *testing.T) (string, string) {
		if _, err := exec.LookPath("rsync"); err != nil {
			t.Skip("no rsync binary")
		}
		port, mod := startRsyncDaemon(t)
		return fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port), mod
	},
	"gorsyncd": func(t *testing.T) (string, string) {
		mod := t.TempDir()
		return startGorsyncdModule(t, mod, true), mod
	},
}

const inTheWay = "a file, not a folder"

func fileStays(t *testing.T, mod, rel string) {
	t.Helper()
	if got, err := os.ReadFile(filepath.Join(mod, rel)); err != nil || string(got) != inTheWay {
		t.Errorf("%s there = %q, %v: the file in the way is gone", rel, got, err)
	}
}

// R21: a staged push sends every folder above what it writes, and rsync
// puts a folder where a file is in its way. An upload or a Mkdir under a
// name that is a file on the daemon deleted that file, with no word. Now
// each fails, and the file stays.
func TestRsyncWritesKeepAFileInTheWay(t *testing.T) {
	for name, start := range rsyncDaemonsUnderTest {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			url, mod := start(t)
			b, err := NewRsyncBackend(url)
			if err != nil {
				t.Fatal(err)
			}
			src := t.TempDir()
			writeTestFile(t, src, "f.txt", "new")
			writeTestFile(t, mod, "plain", inTheWay)
			writeTestFile(t, mod, "deep/file", inTheWay)
			p := model.PutParams{Size: 3, Mode: 0o644}
			for what, do := range map[string]func() error{
				"SendLocalFile plain/f.txt":       func() error { return b.SendLocalFile(ctx, filepath.Join(src, "f.txt"), "plain/f.txt", p) },
				"SendLocalFile deep/file/x/f.txt": func() error { return b.SendLocalFile(ctx, filepath.Join(src, "f.txt"), "deep/file/x/f.txt", p) },
				"CopyFrom plain/g.txt":            func() error { return b.CopyFrom(ctx, "plain/g.txt", strings.NewReader("new"), p) },
				"Mkdir plain":                     func() error { return b.Mkdir(ctx, "plain", 0o755) },
				"Mkdir plain/sub":                 func() error { return b.Mkdir(ctx, "plain/sub", 0o755) },
			} {
				if err := do(); err == nil {
					t.Errorf("%s: no error, with a file in the way", what)
				}
			}
			fileStays(t, mod, "plain")
			fileStays(t, mod, "deep/file")
		})
	}
}

// R21: a delete pushes into the folder of what it deletes, and so made that
// folder when it was not there. Now a delete under a folder that is not
// there, or under a file, makes nothing, and fails as not there, as on a
// local disk.
func TestRsyncRemoveMakesNoFolder(t *testing.T) {
	for name, start := range rsyncDaemonsUnderTest {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			url, mod := start(t)
			b, err := NewRsyncBackend(url)
			if err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, mod, "plain", inTheWay)
			writeTestFile(t, mod, "real/keep.txt", "a folder the backend did not see yet")
			for _, rel := range []string{"missing/x.txt", "missing/deeper/x.txt", "real/missing/x.txt", "plain/x.txt"} {
				if err := b.Remove(ctx, rel); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("Remove(%s) = %v, want not there", rel, err)
				}
				if err := b.RemoveAll(ctx, rel); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("RemoveAll(%s) = %v, want not there", rel, err)
				}
			}
			for _, rel := range []string{"missing", "real/missing"} {
				if _, err := os.Lstat(filepath.Join(mod, rel)); err == nil {
					t.Errorf("a delete under %s, which is not there, made it", rel)
				}
			}
			fileStays(t, mod, "plain")
		})
	}
}

// statsOf counts the daemon Stats of paths under prefix since mark.
func statsOf(mark int, prefix string) int {
	n := 0
	for _, l := range Log.Lines()[mark:] {
		if strings.Contains(l, "STAT testmod/"+prefix) {
			n++
		}
	}
	return n
}

// The check costs a Stat only for a folder not seen yet. A delete makes
// what it deleted unseen again: a file put in its place is in the way.
func TestRsyncWritesSeeAFolderOnce(t *testing.T) {
	for name, start := range rsyncDaemonsUnderTest {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			url, mod := start(t)
			b, err := NewRsyncBackend(url)
			if err != nil {
				t.Fatal(err)
			}
			// A folder in one that is there, and not seen yet.
			writeTestFile(t, mod, "old/keep.txt", "k")
			if err := b.Mkdir(ctx, "old/sub", 0o755); err != nil {
				t.Fatal(err)
			}
			if fi, err := os.Stat(filepath.Join(mod, "old/sub")); err != nil || !fi.IsDir() {
				t.Errorf("Mkdir old/sub made no folder: %v", err)
			}
			src := t.TempDir()
			writeTestFile(t, src, "f.txt", "new")
			p := model.PutParams{Size: 3, Mode: 0o644}
			send := func(rel string) error { return b.SendLocalFile(ctx, filepath.Join(src, "f.txt"), rel, p) }
			if err := send("new/a/f1.txt"); err != nil {
				t.Fatal(err)
			}
			mark := len(Log.Lines())
			if err := send("new/a/f2.txt"); err != nil {
				t.Fatal(err)
			}
			if err := b.CopyFrom(ctx, "new/a/f3.txt", strings.NewReader("new"), p); err != nil {
				t.Fatal(err)
			}
			if n := statsOf(mark, "new"); n != 0 {
				t.Errorf("uploads into a folder an upload made cost %d Stats, want none", n)
			}
			for _, rel := range []string{"new/a/f1.txt", "new/a/f2.txt", "new/a/f3.txt"} {
				if got, err := os.ReadFile(filepath.Join(mod, rel)); err != nil || string(got) != "new" {
					t.Errorf("%s there = %q, %v", rel, got, err)
				}
			}
			if err := b.Mkdir(ctx, "new/b", 0o755); err != nil {
				t.Fatal(err)
			}
			if fi, err := os.Stat(filepath.Join(mod, "new/b")); err != nil || !fi.IsDir() {
				t.Fatalf("Mkdir new/b made no folder: %v", err)
			}
			mark = len(Log.Lines())
			if err := b.Mkdir(ctx, "new/b", 0o755); err != nil {
				t.Fatal(err)
			}
			if n := statsOf(mark, "new"); n != 0 {
				t.Errorf("a Mkdir of a folder a Mkdir made cost %d Stats, want none", n)
			}
			// Deleted, then a file in its place.
			if err := b.RemoveAll(ctx, "new/a"); err != nil {
				t.Fatal(err)
			}
			if err := b.Remove(ctx, "new/b"); err != nil {
				t.Fatal(err)
			}
			for _, rel := range []string{"new/a", "new/b"} {
				if _, err := os.Lstat(filepath.Join(mod, rel)); err == nil {
					t.Fatalf("%s is still there after its delete", rel)
				}
				writeTestFile(t, mod, rel, inTheWay)
				if err := send(rel + "/f.txt"); err == nil {
					t.Errorf("upload into %s: no error, with a file in the way", rel)
				}
				fileStays(t, mod, rel)
			}
		})
	}
}
