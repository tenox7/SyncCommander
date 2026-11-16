package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"sc/internal/sshtest"
	"sc/model"
	"sc/transport"
)

// cutSource is a local source whose reads fail after cut bytes, as when a
// connection drops half way through a copy; with cut < 0 it reads to the
// end. It records the offset of every open.
type cutSource struct {
	*transport.LocalBackend
	mu    sync.Mutex
	cut   int64
	opens []int64
}

func (s *cutSource) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return s.OpenAt(ctx, relPath, 0)
}

func (s *cutSource) OpenAt(ctx context.Context, relPath string, off int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.opens = append(s.opens, off)
	cut := s.cut
	s.mu.Unlock()
	rc, err := s.LocalBackend.OpenAt(ctx, relPath, off)
	if err != nil || cut < 0 {
		return rc, err
	}
	return &cutReader{rc: rc, left: cut}, nil
}

func (s *cutSource) reset(cut int64) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	opens := s.opens
	s.cut, s.opens = cut, nil
	return opens
}

type cutReader struct {
	rc   io.ReadCloser
	left int64
}

func (r *cutReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, errors.New("connection reset by peer")
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.rc.Read(p)
	r.left -= int64(n)
	return n, err
}

func (r *cutReader) Close() error { return r.rc.Close() }

// destinations opens right as each destination that writes part files: the
// local folder, and the same folder over sftp and over scp.
func destinations(t *testing.T, right string) map[string]model.Backend {
	t.Helper()
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	ds := map[string]model.Backend{"local": transport.NewLocalBackend(right)}
	for scheme, withSFTP := range map[string]bool{"sftp": true, "scp": false} {
		b, err := transport.OpenBackend(scheme+"://x:pw@"+sshtest.Start(t, withSFTP)+right, true, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { transport.CloseBackend(b) })
		ds[scheme] = b
	}
	return ds
}

// scanned scans src against dst.
func scanned(t *testing.T, left, right string, src, dst model.Backend) *scenario {
	t.Helper()
	s := &scenario{left: left, right: right, lb: src, rb: dst}
	s.sc = model.NewScanner(src, dst, 1, 1, true)
	s.sc.Scan(context.Background(), scenarioOpts)
	return s
}

// logged reports whether a log line since mark holds text.
func logged(mark int, text string) bool {
	return slices.ContainsFunc(transport.Log.Lines()[mark:], func(l string) bool { return strings.Contains(l, text) })
}

func onDisk(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// L14: an overwrite opened the file with O_TRUNC and wrote it in place. A
// copy cut off half way left half the new version under the name and the
// old version gone. Now the copy goes into a part file; the cut-off copy
// leaves the old version, and the next copy resumes the part, after its
// bytes checksum the same as the source's.
func TestCutOffOverwriteKeepsTheOldFile(t *testing.T) {
	defer transport.SetMaxRetries(transport.MaxRetries())
	transport.SetMaxRetries(1)
	newBody := strings.Repeat("new version ", 100_000)
	oldBody := strings.Repeat("old version ", 100_000) // the same size: no resume candidate
	root := t.TempDir()
	for name, dst := range destinations(t, filepath.Join(root, "r")) {
		t.Run(name, func(t *testing.T) {
			left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
			rel := name + ".txt"
			writeFile(t, filepath.Join(left, rel), newBody)
			writeFile(t, filepath.Join(right, rel), oldBody)
			src := &cutSource{LocalBackend: transport.NewLocalBackend(left), cut: 500_000}
			s := scanned(t, left, right, src, dst)
			s.verifyResume = true
			if p := s.copy(t, rel, true, false); p.Failed.Load() != 1 {
				t.Errorf("a copy cut off half way: failed=%d, want 1", p.Failed.Load())
			}
			if got := readFile(t, filepath.Join(right, rel)); got != oldBody {
				t.Fatalf("after a copy cut off at 500000 bytes, %s holds %d bytes starting %q; want the old version", rel, len(got), got[:min(len(got), 12)])
			}
			// The retry resumed the part the first try left, and was cut too.
			if opens := src.reset(-1); !slices.Equal(opens, []int64{0, 500_000}) {
				t.Errorf("the cut-off copy opened the source at %v, want 0, then 500000 to resume", opens)
			}
			part := fileSize(filepath.Join(right, model.PartPath(rel)))
			if part != 1_000_000 {
				t.Errorf("the part holds %d bytes, want 1000000", part)
			}
			s = scanned(t, left, right, src, dst)
			s.verifyResume = true
			mark := len(transport.Log.Lines())
			if p := s.copy(t, rel, true, false); p.Failed.Load() != 0 {
				t.Errorf("the second copy failed")
			}
			if got := readFile(t, filepath.Join(right, rel)); got != newBody {
				t.Errorf("after the second copy %s holds %d bytes, want the new version", rel, len(got))
			}
			if opens := src.reset(-1); !slices.Equal(opens, []int64{part}) || !logged(mark, "COPY "+rel+" OK (resumed)") {
				t.Errorf("the second copy opened the source at %v, logged resumed=%v; want it to resume after the part's %d bytes", opens, logged(mark, "COPY "+rel+" OK (resumed)"), part)
			}
			if part := filepath.Join(right, model.PartPath(rel)); onDisk(part) {
				t.Errorf("the part file is still there after the copy")
			}
		})
	}
}

// The 2026-11-05 incident: notes edited at the source were a little longer
// than the destination's older copies. A shorter file at the destination
// was taken for a cut-off copy and the rest appended onto it: the old start
// plus the new end. With parts, the name never holds a cut-off copy, so a
// shorter file there is an older version and is replaced whole.
func TestShorterOldVersionIsNotAppendedTo(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	body := "# Notes\nThe first line, edited.\nA line added at the end.\n"
	writeFile(t, filepath.Join(left, "notes.md"), body)
	writeFile(t, filepath.Join(right, "notes.md"), "# Notes\nThe first line.\n")
	s := newScenario(t, left, right)
	if p := s.copy(t, "notes.md", true, false); p.Failed.Load() != 0 {
		t.Errorf("copy failed")
	}
	if got := readFile(t, filepath.Join(right, "notes.md")); got != body {
		t.Errorf("notes.md holds %q, want the source's %q", got, body)
	}
}

// L14: a read-only file at the destination could not be opened to be
// written, a permission error that failed the copy. The new version takes
// its name instead.
func TestReadOnlyDestinationFileIsReplaced(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes read-only files")
	}
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "ro.txt"), "the new version")
	writeFile(t, filepath.Join(right, "ro.txt"), "old")
	if err := os.Chmod(filepath.Join(right, "ro.txt"), 0o444); err != nil {
		t.Fatal(err)
	}
	s := newScenario(t, left, right)
	if p := s.copy(t, "ro.txt", true, false); p.Failed.Load() != 0 {
		t.Errorf("the copy over a read-only file failed")
	}
	if got := readFile(t, filepath.Join(right, "ro.txt")); got != "the new version" {
		t.Errorf("ro.txt holds %q", got)
	}
}

// S4: SFTP and SCP wrote into whatever the name was at the time of the
// copy. A link put there after the scan was written through, into the file
// it names, outside the folder. The part is renamed over the name, which
// replaces the link itself.
func TestLinkMadeAfterTheScanIsNotWrittenThrough(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside.txt")
	writeFile(t, outside, "keep me")
	for name, dst := range destinations(t, filepath.Join(root, "r")) {
		left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
		rel := name + ".txt"
		writeFile(t, filepath.Join(left, rel), "copied from the other side")
		writeFile(t, filepath.Join(right, rel), "old")
		s := scanned(t, left, right, transport.NewLocalBackend(left), dst)
		if err := os.Remove(filepath.Join(right, rel)); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(right, rel)); err != nil {
			t.Fatal(err)
		}
		s.copy(t, rel, true, false)
		if got := readFile(t, outside); got != "keep me" {
			t.Errorf("%s: the copy wrote %q through a link made after the scan", name, got)
		}
		if fi, err := os.Lstat(filepath.Join(right, rel)); err != nil || !fi.Mode().IsRegular() || readFile(t, filepath.Join(right, rel)) != "copied from the other side" {
			t.Errorf("%s: %s is %v, %v; want the copied file", name, rel, fi.Mode(), err)
		}
	}
}

// A part whose bytes are not the source's is never resumed, whatever its
// size, and a part as long as the source that sums the same is committed
// without copying anything.
func TestPartIsResumedOnItsChecksumOnly(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	body := strings.Repeat("0123456789", 100)
	for _, f := range []string{"other.txt", "longer.txt", "whole.txt"} {
		writeFile(t, filepath.Join(left, f), body)
	}
	writeFile(t, filepath.Join(right, model.PartPath("other.txt")), strings.Repeat("x", 500))
	writeFile(t, filepath.Join(right, model.PartPath("longer.txt")), body+"more")
	writeFile(t, filepath.Join(right, model.PartPath("whole.txt")), body)
	src := &cutSource{LocalBackend: transport.NewLocalBackend(left), cut: -1}
	s := scanned(t, left, right, src, transport.NewLocalBackend(right))
	for f, want := range map[string][]int64{"other.txt": {0}, "longer.txt": {0}, "whole.txt": nil} {
		src.reset(-1)
		if p := s.copy(t, f, true, false); p.Failed.Load() != 0 {
			t.Errorf("%s: copy failed", f)
		}
		if got := readFile(t, filepath.Join(right, f)); got != body {
			t.Errorf("%s holds %q", f, got)
		}
		if opens := src.reset(-1); !slices.Equal(opens, want) {
			t.Errorf("%s: the source was opened at %v, want %v", f, opens, want)
		}
		if onDisk(filepath.Join(right, model.PartPath(f))) {
			t.Errorf("%s: its part is still there", f)
		}
	}
}

// A part is listed, as what a cut-off copy left, but never copied: copying
// it to the other side would put a half file there under a part's name. A
// mirror deletes one the source does not have, as any other name.
func TestPartsAreNotCopied(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "f.txt"), "the file")
	writeFile(t, filepath.Join(left, "d", model.PartPath("g.txt")), "half of g")
	writeFile(t, filepath.Join(right, "d", model.PartPath("old.txt")), "half of a file long gone")
	s := newScenario(t, left, right)
	if n := s.node(t, "d/"+model.PartPath("g.txt")); !n.IsSpecial || n.Sides[model.SideLeft].Entry.Kind() != "partial copy" {
		t.Errorf("a part is listed as %+v", n.Sides[model.SideLeft].Entry)
	}
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "d", true, true); p.Failed.Load() != 0 {
		t.Errorf("copy failed")
	}
	wantTree(t, "after a mirror copy", right, "d/", "d/f.txt")
	if !logged(mark, "COPY d/"+model.PartPath("g.txt")+": skipped, a partial copy") {
		t.Errorf("the log does not say the part was left out")
	}
}

// One rsync session sends a whole local folder, the parts in it too, as
// files under their part names. A folder that holds one is copied file by
// file, which leaves the parts out.
func TestBatchSendLeavesPartsOut(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "f.txt"), "the file")
	writeFile(t, filepath.Join(left, "d", "e", "g.txt"), "another")
	writeFile(t, filepath.Join(left, "d", "e", model.PartPath("h.txt")), "half of h")
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	dst, err := transport.OpenBackend("rsync+ssh://x:pw@"+sshtest.Start(t, false)+right, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(dst) })
	s := scanned(t, left, right, transport.NewLocalBackend(left), dst)
	if p := s.copy(t, "d", true, false); p.Failed.Load() != 0 {
		t.Errorf("copy failed")
	}
	wantTree(t, "after copying d over rsync", right, "d/", "d/e/", "d/e/g.txt", "d/f.txt")
}
