package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/sftp"

	"sc/model"
)

// refuseSetstat is an sftp server's file commands with SETSTAT refused, as
// a server that keeps no modes refuses it.
type refuseSetstat struct{ sftp.FileCmder }

func (r refuseSetstat) Filecmd(req *sftp.Request) error {
	if req.Method == "Setstat" {
		return sftp.ErrSSHFxOpUnsupported
	}
	return r.FileCmder.Filecmd(req)
}

// fakeCommand puts a script named name first on the PATH that the test
// ssh server's shell inherits. It skips the test when sh would run another
// one, as a shell that has the command built in.
func fakeCommand(t *testing.T, name, script string) {
	t.Helper()
	bin := t.TempDir()
	p := filepath.Join(bin, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if out, _ := exec.Command("sh", "-c", "command -v "+name).Output(); strings.TrimSpace(string(out)) != p {
		t.Skipf("this sh runs %q for %s, not the one on its PATH", out, name)
	}
}

// writesWithoutModes writes a part of a 0444 file, appends to it and
// commits it on b, a destination that takes no mode: each call must say
// ErrModeNotSet, and the file must be in place under its own name. Then a
// folder, whose mode is a WARN line.
func writesWithoutModes(t *testing.T, b model.Backend, read func(rel string) (string, error)) {
	t.Helper()
	ctx := context.Background()
	part := model.PartPath("a.txt")
	// A part is written with the file's mode and its owner's read and
	// write: a file of 0444 gets its own at the commit.
	pp := model.PutParams{Size: 11, Mode: model.PartMode(0o444)}
	if err := b.CopyFrom(ctx, part, strings.NewReader("hello"), pp); !errors.Is(err, model.ErrModeNotSet) {
		t.Errorf("the write: %v, want ErrModeNotSet", err)
	}
	if err := b.(model.Resumer).AppendFrom(ctx, part, memRangeOpener("hello world"), pp, 5); !errors.Is(err, model.ErrModeNotSet) {
		t.Errorf("the append: %v, want ErrModeNotSet", err)
	}
	if err := b.(model.PartWriter).CommitPart(ctx, "a.txt", 0o444); !errors.Is(err, model.ErrModeNotSet) {
		t.Errorf("the commit: %v, want ErrModeNotSet", err)
	}
	if got, err := read("a.txt"); err != nil || got != "hello world" {
		t.Errorf("a.txt holds %q, %v; want the file", got, err)
	}
	if _, err := read(part); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the part: %v, want it gone", err)
	}
	mark := len(Log.Lines())
	if err := b.Mkdir(ctx, "d", 0o750); err != nil {
		t.Errorf("Mkdir: %v", err)
	}
	if e, err := b.Stat(ctx, "d"); err != nil || !e.IsDir {
		t.Errorf("d is %+v, %v; want a folder", e, err)
	}
	if !loggedSince(mark, "MKDIR d: chmod 0750") {
		t.Error("no line says the folder went without its mode")
	}
}

// S20: a server that refuses SETSTAT took a file's bytes, then refused its
// chmod, and the write failed: the engine sent the file again, up to six
// times, and a commit left it under its part's name.
func TestSFTPWritesOnAServerThatRefusesSetstat(t *testing.T) {
	h := sftp.InMemHandler()
	for _, d := range []string{"/srv", "/srv/base"} {
		if err := h.FileCmd.Filecmd(sftp.NewRequest("Mkdir", d)); err != nil {
			t.Fatal(err)
		}
	}
	h.FileCmd = refuseSetstat{h.FileCmd}
	b, err := NewSFTPBackend(context.Background(), "sftp://x:pw@"+startSFTPRequestServer(t, h)+"/srv/base", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	writesWithoutModes(t, b, func(rel string) (string, error) {
		r := sftp.NewRequest("Get", "/srv/base/"+rel)
		r.Flags = 1 // read
		ra, err := h.FileGet.Fileread(r)
		if err != nil {
			return "", err
		}
		got, err := io.ReadAll(io.NewSectionReader(ra, 0, 1<<20))
		return string(got), err
	})
}

// S20: the same over scp, where chmod is a command of its own after cat,
// and one that fails, as on a disk with no modes, failed the write, and
// the commit before its rename. A part that has the file's mode takes no
// chmod at its commit.
func TestSCPWritesWhenChmodFails(t *testing.T) {
	fakeCommand(t, "chmod", `echo "chmod: $2: Operation not permitted" >&2; exit 1`)
	root := t.TempDir()
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, ok := b.(*SCPBackend); !ok {
		t.Fatalf("scp:// opened a %T, want the SCPBackend", b)
	}
	writesWithoutModes(t, b, func(rel string) (string, error) {
		got, err := os.ReadFile(filepath.Join(root, rel))
		return string(got), err
	})
	ctx := context.Background()
	if err := b.CopyFrom(ctx, model.PartPath("b.txt"), strings.NewReader("b"), model.PutParams{Size: 1, Mode: model.PartMode(0o644)}); !errors.Is(err, model.ErrModeNotSet) {
		t.Errorf("the write of b.txt: %v, want ErrModeNotSet", err)
	}
	if err := b.(model.PartWriter).CommitPart(ctx, "b.txt", 0o644); err != nil {
		t.Errorf("the commit of b.txt, whose part has its mode: %v", err)
	}
}

// S20: a local disk with no modes (FAT, some network shares) failed the
// commit before its rename.
func TestLocalCommitOnADiskWithNoModes(t *testing.T) {
	was := chmodFile
	chmodFile = func(name string, _ os.FileMode) error {
		return &fs.PathError{Op: "chmod", Path: name, Err: fs.ErrPermission}
	}
	t.Cleanup(func() { chmodFile = was })
	root := t.TempDir()
	b := NewLocalBackend(root)
	ctx := context.Background()
	if err := b.CopyFrom(ctx, model.PartPath("a.txt"), strings.NewReader("hello"), model.PutParams{Size: 5, Mode: model.PartMode(0o640)}); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitPart(ctx, "a.txt", 0o640); !errors.Is(err, model.ErrModeNotSet) || !errors.Is(err, fs.ErrPermission) {
		t.Errorf("the commit: %v, want ErrModeNotSet with the chmod's error", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "a.txt")); err != nil || string(got) != "hello" {
		t.Errorf("a.txt holds %q, %v; want the file", got, err)
	}
	if _, err := os.Lstat(filepath.Join(root, model.PartPath("a.txt"))); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the part: %v, want it gone", err)
	}
}

// S20: touch -d with an ISO time is GNU's and the newer BSDs'. busybox and
// older Unix refuse it: SetTimes failed, the times stayed those of the
// copy, and the next compare found every file different. The shell falls
// back to POSIX's touch -t in UTC, to the second.
func TestShellSetTimesWithoutTouchD(t *testing.T) {
	mtime := time.Date(2021, 3, 4, 5, 6, 7, 891000000, time.UTC)
	atime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, fake := range []bool{false, true} {
		name := "this system's touch"
		if fake {
			name = "a touch with no -d"
		}
		t.Run(name, func(t *testing.T) {
			if fake {
				touch, err := exec.LookPath("touch")
				if err != nil {
					t.Skip("no touch")
				}
				fakeCommand(t, "touch", `for a in "$@"; do [ "$a" = -d ] && { echo "touch: invalid date" >&2; exit 1; }; done`+"\nexec "+shellQuote(touch)+` "$@"`)
				// touch -t reads the time in the zone TZ names: three
				// hours east of UTC here, unless the command says UTC.
				t.Setenv("TZ", "XXX-3")
			}
			root := t.TempDir()
			writeFile(t, filepath.Join(root, "f.txt"), "x")
			b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			ctx := context.Background()
			if err := b.SetTimes(ctx, "f.txt", mtime, atime, time.Time{}); err != nil {
				t.Fatalf("SetTimes: %v", err)
			}
			e, err := NewLocalBackend(root).Stat(ctx, "f.txt")
			if err != nil {
				t.Fatal(err)
			}
			// To the nanosecond with -d, to the second with -t.
			ok := func(got, want time.Time) bool {
				return got.Equal(want.Truncate(time.Second)) || !fake && got.Equal(want)
			}
			if !ok(e.ModTime, mtime) || !ok(e.ATime.Time(), atime) {
				t.Errorf("mtime %v, atime %v; want %v and %v", e.ModTime.UTC(), e.ATime.Time().UTC(), mtime, atime)
			}
		})
	}
}

// S20: a call that did its work and then could not set a mode is not tried
// again, and is no FAIL: another try would do the work again, for the same
// answer.
func TestRetryStopsAtAModeNotSet(t *testing.T) {
	SetMaxRetries(2)
	defer SetMaxRetries(5)
	mark := len(Log.Lines())
	calls := 0
	err := Retry(context.Background(), "test", "write x.txt", func(context.Context) error {
		calls++
		return fmt.Errorf("test: chmod 0644 x.txt: %w: %w", model.ErrModeNotSet, errors.New("refused"))
	})
	if !errors.Is(err, model.ErrModeNotSet) || calls != 1 {
		t.Errorf("Retry gave %v after %d calls, want ErrModeNotSet after 1", err, calls)
	}
	if loggedSince(mark, "write x.txt") {
		t.Error("Retry logged the call, as a RETRY or a FAIL")
	}
}
