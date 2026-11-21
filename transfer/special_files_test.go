//go:build unix

package transfer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// rowsNamed counts the top rows named name: a file and a link of one name
// are two rows.
func rowsNamed(s *scenario, name string) int {
	n := 0
	s.sc.ReadTree(func(r *model.TreeNode) {
		for _, c := range r.Children {
			if c.Name == name {
				n++
			}
		}
	})
	return n
}

func isType(p string, m os.FileMode) bool {
	fi, err := os.Lstat(p)
	return err == nil && fi.Mode()&m != 0
}

func loggedSince(mark int, text string) bool {
	for _, l := range transport.Log.Lines()[mark:] {
		if strings.Contains(l, text) {
			return true
		}
	}
	return false
}

// L2: the destination has x.txt, a link to a file out of the tree, and
// data, a link to a folder out of the tree. The scan left links out, so
// the tree showed both names as missing on the right, and the copy wrote
// through them: into the file the link names, and into the folder. Now a
// link is a row of its own kind, a twin of the file or folder of its name:
// the copy does not write there without the user's yes, and with it
// replaces the link itself.
func TestCopyOntoDestinationLink(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "x.txt"), "NEW DATA")
	writeFile(t, filepath.Join(left, "data", "f.txt"), "NEW DATA")
	writeFile(t, filepath.Join(root, "target.txt"), "keep me")
	for _, d := range []string{filepath.Join(root, "datadir"), right} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{"x.txt": "target.txt", "data": "datadir"} {
		if err := os.Symlink(filepath.Join(root, target), filepath.Join(right, name)); err != nil {
			t.Fatal(err)
		}
	}
	s := newScenario(t, left, right)
	names := []string{"x.txt", "data"}
	for _, name := range names {
		if n := rowsNamed(s, name); n != 2 {
			t.Errorf("the tree has %d row(s) named %s; want 2: the source's, and the destination's link", n, name)
		}
	}
	outsideUntouched := func(when string) {
		t.Helper()
		if got := readFile(t, filepath.Join(root, "target.txt")); got != "keep me" {
			t.Errorf("%s: the copy wrote into the link's target, out of the tree: %q", when, got)
		}
		if _, err := os.Stat(filepath.Join(root, "datadir", "f.txt")); err == nil {
			t.Errorf("%s: the copy wrote into the folder the link names, out of the tree", when)
		}
	}

	for _, name := range names {
		if p := s.copy(t, name, true, false); p.Failed.Load() == 0 {
			t.Errorf("copy of %s onto a link without the user's yes: no failure", name)
		}
		if !isType(filepath.Join(right, name), os.ModeSymlink) {
			t.Errorf("without the user's yes, the link %s is gone", name)
		}
	}
	outsideUntouched("without the user's yes")

	for _, name := range names {
		if p := s.copy(t, name, true, false, name); p.Failed.Load() != 0 {
			t.Errorf("copy of %s replacing the link, with the user's yes: failed", name)
		}
	}
	outsideUntouched("with the user's yes")
	if isType(filepath.Join(right, "x.txt"), os.ModeSymlink) || readFile(t, filepath.Join(right, "x.txt")) != "NEW DATA" {
		t.Error("with the user's yes, right/x.txt is not the copied file")
	}
	if isType(filepath.Join(right, "data"), os.ModeSymlink) || readFile(t, filepath.Join(right, "data", "f.txt")) != "NEW DATA" {
		t.Error("with the user's yes, right/data is not the copied folder")
	}
}

// L2: a link only on the destination is a row like any other: a mirror
// deletes it, the link itself and not what it names. A link on the source
// is never read: since 2026-11-20 the copy makes a link to the same target
// on the destination, and the file it names, out of the tree, is neither
// copied nor touched.
func TestMirrorAndLinks(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "a")
	writeFile(t, filepath.Join(root, "target.txt"), "keep me")
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "target.txt"), filepath.Join(left, "secret.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "target.txt"), filepath.Join(right, "only-here.txt")); err != nil {
		t.Fatal(err)
	}
	s := newScenario(t, left, right)
	p := s.copy(t, "", true, true)
	wantTree(t, "mirror of a folder with links", right, "a.txt", "secret.txt")
	if p.Failed.Load() != 0 {
		t.Errorf("mirror: %d failed", p.Failed.Load())
	}
	if got, err := os.Readlink(filepath.Join(right, "secret.txt")); err != nil || got != filepath.Join(root, "target.txt") {
		t.Errorf("right/secret.txt: a link to %q (%v); want a link to the source's target", got, err)
	}
	if got := readFile(t, filepath.Join(root, "target.txt")); got != "keep me" {
		t.Errorf("the links' target now holds %q", got)
	}
}

// L9: named pipes were listed as files. A copy or a checksum pass opened
// one, and the open waited in the kernel for the other end: neither cancel
// nor the stall guard reached it, and the run never ended. Now a pipe is
// its own kind of row and never opened: the copy leaves it out, the
// checksum pass skips it, and a pipe on the destination is a twin of the
// file of its name, not written into.
func TestNamedPipeIsNeverOpened(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "a")
	writeFile(t, filepath.Join(left, "p2"), "a file on the source")
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	pipes := []string{filepath.Join(left, "pipe"), filepath.Join(right, "pipe"), filepath.Join(right, "p2")}
	for _, p := range pipes {
		if err := syscall.Mkfifo(p, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s := newScenario(t, left, right)
	ends := func(what string, f func()) {
		t.Helper()
		done := make(chan struct{})
		go func() { defer close(done); f() }()
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
			t.Errorf("%s: still blocked after 10s, on a named pipe", what)
		}
		for _, p := range pipes { // open the other end so the stuck open returns
			if f, err := os.OpenFile(p, os.O_RDWR|syscall.O_NONBLOCK, 0); err == nil {
				defer f.Close()
			}
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}

	ends("checksum pass", func() {
		s.sc.ReadTree(func(*model.TreeNode) {})
		s.sc.ChecksumNode(context.Background(), s.node(t, ""))
	})
	mark := len(transport.Log.Lines())
	var p *Progress
	ends("copy of the top row", func() { p = s.copy(t, "", true, false) })
	if p == nil {
		return
	}
	if got := readFile(t, filepath.Join(right, "a.txt")); got != "a" {
		t.Errorf("a.txt = %q", got)
	}
	if !isType(filepath.Join(right, "p2"), os.ModeNamedPipe) {
		t.Error("the pipe p2 on the destination was replaced without the user's yes")
	}
	if p.Failed.Load() != 1 {
		t.Errorf("failed = %d; want 1: p2, a pipe in the way that needs the user's yes", p.Failed.Load())
	}
	if loggedSince(mark, "COPY pipe ") || !isType(filepath.Join(right, "pipe"), os.ModeNamedPipe) {
		t.Error("the copy took the pipe on both sides for a file")
	}
}
