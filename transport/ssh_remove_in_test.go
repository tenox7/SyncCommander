package transport

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/internal/sshtest"
	"sc/model"
)

// entriesOf lists the names in dir.
func entriesOf(t *testing.T, dir string) string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return strings.Join(names, "|")
}

// openBatch opens url with the lazy wrapper, as sc does, which retries
// what is not final.
func openBatch(t *testing.T, url string) model.BatchRemover {
	t.Helper()
	b, err := TryOpenBackend(url, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { CloseBackend(b) })
	br, ok := b.(model.BatchRemover)
	if !ok {
		t.Fatalf("%T is no model.BatchRemover", b)
	}
	return br
}

// R16: over an ssh shell each delete was a command, a round trip each.
// RemoveIn removes a folder's entries with one command, sh with the names
// on its input: a name a shell would read as more than a name is removed
// as it is, a link is removed and not what it leads to, and the rest of
// the folder stays.
func TestShellRemoveInIsOneCommand(t *testing.T) {
	for _, tc := range []struct {
		scheme string
		o      sshtest.Options
	}{{"scp", sshtest.Options{}}, {"sftp", sshtest.Options{SFTP: true}}} {
		t.Run(tc.scheme, func(t *testing.T) {
			addr := sshtest.StartWith(t, tc.o)
			root := t.TempDir()
			d := filepath.Join(root, "d")
			files := []string{"-rf", "a b", "it's", "*", "$(touch pwned)", "x\ny", "...", "--", `back\slash`,
				"plain.txt", `say "hi"`, "tab\there", "~home", "; touch pwned2"}
			for _, n := range files {
				writeTestFile(t, d, n, "x")
			}
			writeTestFile(t, d, "sub/deep/f", "x")
			writeTestFile(t, d, "-dir/g", "x")
			writeTestFile(t, d, "keep.txt", "k")
			writeTestFile(t, d, "keepdir/k", "k")
			writeTestFile(t, root, "outside/stays.txt", "s")
			for _, l := range []string{"lf", "ld"} {
				if err := os.Symlink(filepath.Join("..", "outside"), filepath.Join(d, l)); err != nil {
					t.Fatal(err)
				}
			}
			br := openBatch(t, tc.scheme+"://x:pw@"+addr+root)
			var items []model.RemoveItem
			for _, n := range append(files, "not-there.txt", "lf") {
				items = append(items, model.RemoveItem{Name: n})
			}
			for _, n := range []string{"sub", "-dir", "ld", "gone-dir"} {
				items = append(items, model.RemoveItem{Name: n, IsDir: true})
			}
			mark := len(Log.Lines())
			if err := br.RemoveIn(context.Background(), "d", items); err != nil {
				t.Fatal(err)
			}
			if got := entriesOf(t, d); got != "keep.txt|keepdir" {
				t.Errorf("d holds %q; want keep.txt and keepdir", got)
			}
			if _, err := os.Stat(filepath.Join(root, "outside", "stays.txt")); err != nil {
				t.Errorf("what a link led to went: %v", err)
			}
			for _, dir := range []string{d, root, filepath.Join(root, "outside")} {
				for _, p := range []string{"pwned", "pwned2"} {
					if _, err := os.Lstat(filepath.Join(dir, p)); err == nil {
						t.Errorf("%s was made: a name ran as a command", filepath.Join(dir, p))
					}
				}
			}
			if n, rms := countSent(mark, tc.scheme, ">>> REMOVE "), countSent(mark, tc.scheme, " rm "); n != 1 || rms != 0 {
				t.Errorf("%d scripts and %d rm commands sent; want 1 and 0: %q", n, rms, sentSince(mark, tc.scheme))
			}
		})
	}
}

// A name that is no entry of the folder is refused, final, before anything
// runs, as the daemon's are (they were tried six times); so is a folder
// that is not there.
func TestShellRemoveInRefusals(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "d/a.txt", "x")
	br := openBatch(t, "scp://x:pw@"+startExecSSHServer(t)+root)
	mark := len(Log.Lines())
	for _, bad := range []string{"..", ".", "", "a/../../x", "x\x00y", `..\x`} {
		finalAt(t, fmt.Sprintf("%q", bad), fs.ErrInvalid, func(ctx context.Context) error {
			return br.RemoveIn(ctx, "d", []model.RemoveItem{{Name: "a.txt"}, {Name: bad}})
		})
	}
	if n := countSent(mark, "scp", ">>> REMOVE ") + countSent(mark, "scp", " rm "); n != 0 {
		t.Errorf("%d commands sent for refused names: %q", n, sentSince(mark, "scp"))
	}
	if _, err := os.Stat(filepath.Join(root, "d", "a.txt")); err != nil {
		t.Errorf("a.txt went with a refused name: %v", err)
	}
	finalAt(t, "a folder not there", fs.ErrNotExist, func(ctx context.Context) error {
		return br.RemoveIn(ctx, "nope", []model.RemoveItem{{Name: "a.txt"}, {Name: "b", IsDir: true}})
	})
}

// The daemon refuses such a name at once too.
func TestRsyncRemoveInRefusalIsFinal(t *testing.T) {
	port, _ := startRsyncDaemon(t)
	url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod/", port)
	b := NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
	defer CloseBackend(b)
	finalAt(t, "..", fs.ErrInvalid, func(ctx context.Context) error {
		return b.(model.BatchRemover).RemoveIn(ctx, "", []model.RemoveItem{{Name: "a.txt"}, {Name: ".."}})
	})
}

// quoted finds the names an rm line of rmScript's gives, simple ones.
var quoted = regexp.MustCompile(`'([^']*)'`)

// Each rm of a script is given rmArgMax of names, and a script holds
// rmScriptMax of them, and at least one.
func TestRmScriptSplits(t *testing.T) {
	defer func(a, s int) { rmArgMax, rmScriptMax = a, s }(rmArgMax, rmScriptMax)
	var files, dirs []string
	for i := range 20 {
		files = append(files, fmt.Sprintf("n%02d.txt", i)) // 7 bytes, 16 counted
	}
	dirs = []string{"d1", "d2", "d3"}
	for _, c := range []struct {
		argMax, scriptMax int
		wantF, wantD      int
		perRm             int
	}{
		{64, 1 << 20, 20, 3, 4},
		{1 << 10, 1 << 20, 20, 3, 20},
		{64, 100, 7, 0, 4},
		{64, 1, 1, 0, 1},
	} {
		rmArgMax, rmScriptMax = c.argMax, c.scriptMax
		script, nf, nd := rmScript("/base/x", files, dirs)
		if nf != c.wantF || nd != c.wantD {
			t.Errorf("max %d, %d: took %d files and %d folders; want %d and %d", c.argMax, c.scriptMax, nf, nd, c.wantF, c.wantD)
		}
		if !strings.HasPrefix(script, "{\n") || !strings.HasSuffix(script, "\n}\n") {
			t.Errorf("max %d, %d: the script is not one compound command:\n%s", c.argMax, c.scriptMax, script)
		}
		var named []string
		for _, line := range strings.Split(script, "\n") {
			if !strings.HasPrefix(line, "rm ") {
				continue
			}
			names := quoted.FindAllStringSubmatch(line, -1)
			if len(names) == 0 || len(names) > c.perRm {
				t.Errorf("max %d, %d: an rm of %d names; want 1 to %d: %s", c.argMax, c.scriptMax, len(names), c.perRm, line)
			}
			for _, m := range names {
				named = append(named, m[1])
			}
		}
		want := append(append([]string(nil), files[:nf]...), dirs[:nd]...)
		if strings.Join(named, ",") != strings.Join(want, ",") {
			t.Errorf("max %d, %d: the rms name %q; want %q", c.argMax, c.scriptMax, named, want)
		}
	}
}

// The scripts split so, over a server, and remove all.
func TestShellRemoveInSplits(t *testing.T) {
	defer func(a, s int) { rmArgMax, rmScriptMax = a, s }(rmArgMax, rmScriptMax)
	root := t.TempDir()
	d := filepath.Join(root, "d")
	br := openBatch(t, "scp://x:pw@"+startExecSSHServer(t)+root)
	for _, c := range []struct {
		argMax, scriptMax, scripts int
	}{{40, 1 << 20, 1}, {1 << 10, 48, 3}} {
		var items []model.RemoveItem
		for _, n := range []string{"a", "b", "c", "e", "f", "g", "h"} {
			writeTestFile(t, d, n+"-0.txt", "x") // 7 bytes, 16 counted
			items = append(items, model.RemoveItem{Name: n + "-0.txt"})
		}
		writeTestFile(t, d, "keep.txt", "k")
		rmArgMax, rmScriptMax = c.argMax, c.scriptMax
		mark := len(Log.Lines())
		if err := br.RemoveIn(context.Background(), "d", items); err != nil {
			t.Fatal(err)
		}
		if got := entriesOf(t, d); got != "keep.txt" {
			t.Errorf("max %d, %d: d holds %q; want keep.txt", c.argMax, c.scriptMax, got)
		}
		if n := countSent(mark, "scp", ">>> REMOVE "); n != c.scripts {
			t.Errorf("max %d, %d: %d scripts; want %d", c.argMax, c.scriptMax, n, c.scripts)
		}
	}
}

// An entry that cannot be removed fails the call, final, at once, and the
// entries after it are removed still.
func TestShellRemoveInGoesOnPastAFailure(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root removes from a folder it cannot write")
	}
	defer func(s int) { rmScriptMax = s }(rmScriptMax)
	rmScriptMax = 1 // a script for each entry
	root := t.TempDir()
	d := filepath.Join(root, "d")
	for _, p := range []string{"a.txt", "b.txt", "locked/inner/f", "z-dir/g"} {
		writeTestFile(t, d, p, "x")
	}
	lockDirT(t, filepath.Join(d, "locked", "inner"))
	br := openBatch(t, "scp://x:pw@"+startExecSSHServer(t)+root)
	items := []model.RemoveItem{{Name: "a.txt"}, {Name: "b.txt"}, {Name: "locked", IsDir: true}, {Name: "z-dir", IsDir: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t0 := time.Now()
	err := br.RemoveIn(ctx, "d", items)
	if took := time.Since(t0); err == nil || took > 2*time.Second || !isPermanentError(err) {
		t.Errorf("%v after %v; want a final error, at once", err, took.Round(time.Millisecond))
	}
	if got := entriesOf(t, d); got != "locked" {
		t.Errorf("d holds %q; want locked alone", got)
	}
}

// lockDirT makes dir read-only for the test.
func lockDirT(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
}

// A cancelled RemoveIn stops on the server, between its rms and in one: rm
// here waits a second before it removes anything, and nothing goes.
func TestRemoveInStopsOnTheServerWhenCancelled(t *testing.T) {
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
	defer func(a int) { rmArgMax = a }(rmArgMax)
	rmArgMax = 1 // an rm for each entry
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	for _, scheme := range []string{"scp", "sftp"} {
		t.Run(scheme, func(t *testing.T) {
			root := t.TempDir()
			paths := makeTree(t, root)
			br := openBatch(t, scheme+"://x:pw@"+servers[scheme]+root)
			var items []model.RemoveItem
			for _, p := range paths {
				if filepath.Base(filepath.Dir(p)) == "s0" {
					items = append(items, model.RemoveItem{Name: filepath.Base(p)})
				}
			}
			items = append(items, model.RemoveItem{Name: "s1", IsDir: true})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			time.AfterFunc(300*time.Millisecond, cancel)
			start := time.Now()
			// s0's files, then s1 from d: two folders, so two calls.
			err := br.RemoveIn(ctx, "d/s0", items[:len(items)-1])
			if took := time.Since(start); err == nil || took > 900*time.Millisecond {
				t.Errorf("a cancelled RemoveIn gave %v after %v; want an error at once", err, took)
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

// A server that runs its sftp server for every command, as ForceCommand
// internal-sftp does, or a shell with no rm: the entries go by SFTP
// requests.
func TestSFTPRemoveInWithNoShell(t *testing.T) {
	for _, tc := range []struct {
		name    string
		o       sshtest.Options
		scripts int
		setup   func(t *testing.T)
	}{
		{"sftp only", sshtest.Options{SFTP: true, OnlySFTP: true}, 0, func(*testing.T) {}},
		{"no rm", sshtest.Options{SFTP: true}, 1, func(t *testing.T) {
			fakeCommand(t, "rm", `echo "sh: rm: not found" >&2; exit 127`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)
			root := t.TempDir()
			d := filepath.Join(root, "d")
			for _, p := range []string{"a.txt", "b.txt", "sub/c.txt", "keep.txt"} {
				writeTestFile(t, d, p, "x")
			}
			br := openBatch(t, "sftp://x:pw@"+sshtest.StartWith(t, tc.o)+root)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			mark := len(Log.Lines())
			items := []model.RemoveItem{{Name: "a.txt"}, {Name: "b.txt"}, {Name: "gone.txt"}, {Name: "sub", IsDir: true}}
			if err := br.RemoveIn(ctx, "d", items); err != nil {
				t.Fatal(err)
			}
			if got := entriesOf(t, d); got != "keep.txt" {
				t.Errorf("d holds %q; want keep.txt", got)
			}
			if n := countSent(mark, "sftp", ">>> REMOVE "); n != tc.scripts {
				t.Errorf("%d scripts sent; want %d", n, tc.scripts)
			}
			finalAt(t, "..", fs.ErrInvalid, func(ctx context.Context) error {
				return br.RemoveIn(ctx, "d", []model.RemoveItem{{Name: "keep.txt"}, {Name: ".."}})
			})
		})
	}
}

// What the backend held of the folder goes with what it removed: a listing
// after RemoveIn is not served from the preload, and an rsync+ssh send into
// a removed folder makes it again, not after a failed push.
func TestShellRemoveInDropsWhatItKnew(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"d/a.txt", "d/b.txt", "d/keep.txt", "d/sub/x"} {
		writeTestFile(t, root, p, "x")
	}
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if err := b.(model.RecursivePreloader).PreloadRecursive(ctx, ""); err != nil {
		t.Fatal(err)
	}
	names := func(dir string) string {
		ents, err := b.List(ctx, dir)
		if err != nil {
			return err.Error()
		}
		var n []string
		for _, e := range ents {
			n = append(n, e.Name)
		}
		sort.Strings(n)
		return strings.Join(n, "|")
	}
	if got := names("d/sub"); got != "x" {
		t.Fatalf("d/sub lists %q before; want x", got)
	}
	br, ok := b.(model.BatchRemover)
	if !ok {
		t.Fatalf("%T is no model.BatchRemover", b)
	}
	if err := br.RemoveIn(ctx, "d", []model.RemoveItem{{Name: "a.txt"}, {Name: "gone.txt"}}); err != nil {
		t.Fatal(err)
	}
	if got := names("d"); got != "b.txt|keep.txt|sub" {
		t.Errorf("d lists %q after files went; want b.txt, keep.txt and sub", got)
	}
	if err := br.RemoveIn(ctx, "d", []model.RemoveItem{{Name: "b.txt"}, {Name: "sub", IsDir: true}}); err != nil {
		t.Fatal(err)
	}
	if got := names("d"); got != "keep.txt" {
		t.Errorf("d lists %q after; want keep.txt", got)
	}
	if got := names("d/sub"); got == "x" {
		t.Errorf("d/sub, removed, lists %q", got)
	}

	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	var mu sync.Mutex
	mkdirs, sessions := 0, 0
	addr := sshtest.StartWith(t, sshtest.Options{Hold: func(cmd string) bool {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasPrefix(cmd, "mkdir -p "):
			mkdirs++
		case strings.HasPrefix(cmd, "rsync "):
			sessions++
		}
		return false
	}})
	mod := t.TempDir()
	rb, err := NewRsyncSSHBackend(ctx, "rsync+ssh://x:pw@"+addr+mod, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rb.Close()
	src := t.TempDir()
	writeTestFile(t, src, "f.txt", "new")
	send := func(rel string) error {
		return rb.SendLocalFile(ctx, filepath.Join(src, "f.txt"), rel, model.PutParams{Size: 3, Mode: 0o644})
	}
	step := func(what string, wantMkdirs, wantSessions int, do func() error) {
		t.Helper()
		mu.Lock()
		m0, s0 := mkdirs, sessions
		mu.Unlock()
		if err := do(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if m, s := mkdirs-m0, sessions-s0; m != wantMkdirs || s != wantSessions {
			t.Errorf("%s ran %d mkdir and %d rsync; want %d and %d", what, m, s, wantMkdirs, wantSessions)
		}
	}
	step("the first send into a/b", 1, 1, func() error { return send("a/b/f1.txt") })
	step("a RemoveIn of a and c.txt", 0, 0, func() error {
		br, ok := any(rb).(model.BatchRemover)
		if !ok {
			return fmt.Errorf("%T is no model.BatchRemover", rb)
		}
		return br.RemoveIn(ctx, "", []model.RemoveItem{{Name: "a", IsDir: true}, {Name: "c.txt"}})
	})
	step("a send into a/b after it", 1, 1, func() error { return send("a/b/f2.txt") })
}

// A shell that takes the script and runs nothing, as a restricted one may:
// the deletes are not taken for ones that went through. Over scp the call
// fails; SFTP removes the entries by its requests.
func TestRemoveInSeesAScriptThatDidNotRun(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	bin := t.TempDir()
	fake := "#!" + sh + "\nif [ \"$1\" = -c ] && [ \"$2\" = sh ]; then exit 0; fi\nexec " + sh + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "sh"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	for _, scheme := range []string{"scp", "sftp"} {
		t.Run(scheme, func(t *testing.T) {
			root := t.TempDir()
			d := filepath.Join(root, "d")
			writeTestFile(t, d, "a.txt", "x")
			writeTestFile(t, d, "sub/b.txt", "x")
			b, err := OpenBackend(scheme+"://x:pw@"+servers[scheme]+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			br, ok := b.(model.BatchRemover)
			if !ok {
				t.Fatalf("%T is no model.BatchRemover", b)
			}
			err = br.RemoveIn(context.Background(), "d", []model.RemoveItem{{Name: "a.txt"}, {Name: "sub", IsDir: true}})
			left := entriesOf(t, d)
			switch scheme {
			case "scp":
				if err == nil || left != "a.txt|sub" {
					t.Errorf("RemoveIn gave %v, d holds %q; want an error, and both there", err, left)
				}
			case "sftp":
				if err != nil || left != "" {
					t.Errorf("RemoveIn gave %v, d holds %q; want both gone by SFTP", err, left)
				}
			}
		})
	}
}
