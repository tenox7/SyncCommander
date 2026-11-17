package transfer

// Scenario tests: two small trees on disk, one user action through the real
// scanner, copy engine and local backend, then the destination is compared
// with what the user asked for.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"sc/model"
	"sc/transport"
)

var scenarioOpts = model.CompareOpts{Size: true, ModTime: true, TimeGrace: true, IgnoreTZDST: true}

// diskTree lists everything under dir, folders with a trailing slash.
func diskTree(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == dir {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			rel += "/"
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func writeFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// scenario is a scanned pair of local folders.
type scenario struct {
	left, right  string
	lb, rb       model.Backend
	sc           *model.Scanner
	verifyResume bool // -verify-resume for the copies
}

func newScenario(t *testing.T, left, right string) *scenario {
	t.Helper()
	return newScenarioDepth(t, left, right, true)
}

// newScenarioDepth scans deep, or only the top level as -deep-scan=false does.
func newScenarioDepth(t *testing.T, left, right string, deep bool) *scenario {
	t.Helper()
	for _, d := range []string{left, right} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := &scenario{left: left, right: right, lb: transport.NewLocalBackend(left), rb: transport.NewLocalBackend(right)}
	s.sc = model.NewScanner(s.lb, s.rb, 1, 1, deep)
	s.sc.Scan(context.Background(), scenarioOpts)
	return s
}

// node finds a scanned node by its relative path; "" is the root.
func (s *scenario) node(t *testing.T, relPath string) *model.TreeNode {
	t.Helper()
	n := s.sc.Tree()
	for _, part := range splitRel(relPath) {
		i := slices.IndexFunc(n.Children, func(c *model.TreeNode) bool { return c.Name == part })
		if i < 0 {
			t.Fatalf("%s not in the tree", relPath)
		}
		n = n.Children[i]
	}
	return n
}

func splitRel(relPath string) []string {
	var parts []string
	for p := relPath; p != ""; p = model.DirOf(p) {
		parts = append(parts, filepath.Base(p))
	}
	slices.Reverse(parts)
	return parts
}

// copy runs one copy as the user agreed to it, on the plan the tree gives
// now (PlanCopy). replace lists the type conflicts the user confirmed;
// mirror deletes what the plan has only on the destination. The user agreed
// to overwrite the newer files: a fixture writes a destination file just
// after the source's, which makes it a second newer when the two writes
// fall on two seconds (F16).
func (s *scenario) copy(t *testing.T, relPath string, leftToRight, mirror bool, replace ...string) *Progress {
	t.Helper()
	src, dst := s.lb, s.rb
	if !leftToRight {
		src, dst = s.rb, s.lb
	}
	node := s.node(t, relPath)
	planned := PlanCopy(s.sc, node, leftToRight, scenarioOpts)
	plan := Plan{Newer: planned.Newer, Deletes: planned.Deletes, Mirror: mirror}
	for _, r := range replace {
		plan.Conflicts = append(plan.Conflicts, Conflict{RelPath: r})
	}
	p := &Progress{}
	Copy(context.Background(), Request{Src: src, Dst: dst, Scanner: s.sc, Node: node, RelPath: relPath,
		LeftToRight: leftToRight, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: plan,
		VerifyResume: s.verifyResume})
	return p
}

func wantTree(t *testing.T, what, dir string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if got := diskTree(t, dir); !slices.Equal(got, want) {
		t.Errorf("%s: %s holds %q, want %q", what, filepath.Base(dir), got, want)
	}
}

// F1: a copy started on the top row must copy the whole tree, and its mirror
// pass must not run without the copies.
func TestScenarioCopyTopRow(t *testing.T) {
	for _, mirror := range []bool{false, true} {
		root := t.TempDir()
		left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
		writeFile(t, filepath.Join(left, "new.txt"), "new file on the source")
		writeFile(t, filepath.Join(left, "sub", "a.txt"), "another new file")
		writeFile(t, filepath.Join(right, "old.txt"), "only on the destination")
		s := newScenario(t, left, right)

		var picked []string
		s.sc.ReadTree(func(r *model.TreeNode) {
			for _, n := range model.CollectCopyFiles(r, &scenarioOpts, true, false) {
				picked = append(picked, n.RelPath)
			}
		})
		sort.Strings(picked)
		if want := []string{"new.txt", "sub/a.txt"}; !slices.Equal(picked, want) {
			t.Errorf("mirror=%v: CollectCopyFiles(root) = %q, want %q", mirror, picked, want)
		}

		p := s.copy(t, "", true, mirror)
		want := []string{"new.txt", "sub/", "sub/a.txt"}
		if !mirror {
			want = append(want, "old.txt")
		}
		wantTree(t, "top-row copy", right, want...)
		if p.Failed.Load() != 0 || p.Done.Load() != 2 || p.Total.Load() != 2 {
			t.Errorf("mirror=%v: done=%d/%d failed=%d, want 2/2 failed=0", mirror, p.Done.Load(), p.Total.Load(), p.Failed.Load())
		}
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// L1: sc with no arguments opens one folder on both sides. A copy there must
// not truncate the file it reads.
func TestScenarioSameFolderBothSides(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "report.txt"), "twelve bytes")
	writeFile(t, filepath.Join(dir, "sub", "a.txt"), "data in a subfolder")
	s := newScenario(t, dir, dir)
	for _, relPath := range []string{"report.txt", "sub", ""} {
		p := s.copy(t, relPath, true, true)
		if p.Failed.Load() != 1 {
			t.Errorf("copy %q onto itself: failed=%d, want 1", relPath, p.Failed.Load())
		}
	}
	if got := readFile(t, filepath.Join(dir, "report.txt")); got != "twelve bytes" {
		t.Errorf("report.txt = %q after copying it onto itself", got)
	}
	if got := readFile(t, filepath.Join(dir, "sub", "a.txt")); got != "data in a subfolder" {
		t.Errorf("sub/a.txt = %q after copying it onto itself", got)
	}

	// The same folder reached through a symlink is the same object too.
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	s = newScenario(t, dir, link)
	if p := s.copy(t, "report.txt", true, false); p.Failed.Load() != 1 {
		t.Errorf("copy through a symlinked base: failed=%d, want 1", p.Failed.Load())
	}
	if got := readFile(t, filepath.Join(dir, "report.txt")); got != "twelve bytes" {
		t.Errorf("report.txt = %q after a copy through a symlinked base", got)
	}
}

// L1: a file can be one object on both sides while its folders differ: a
// hard link, or a destination subfolder that is a symlink into the source.
func TestScenarioSameFileInDifferentFolders(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "big.bin"), "the only copy of this data")
	writeFile(t, filepath.Join(left, "d", "sub", "a.txt"), "data behind a symlink")
	writeFile(t, filepath.Join(right, "d", "other.txt"), "x")
	if err := os.Link(filepath.Join(left, "big.bin"), filepath.Join(right, "big.bin")); err != nil {
		t.Skip(err)
	}
	if err := os.Symlink(filepath.Join(left, "d", "sub"), filepath.Join(right, "d", "sub")); err != nil {
		t.Fatal(err)
	}
	s := newScenario(t, left, right)
	if p := s.copy(t, "big.bin", true, false); p.Failed.Load() != 1 {
		t.Errorf("copy onto a hard link of itself: failed=%d, want 1", p.Failed.Load())
	}
	if got := readFile(t, filepath.Join(left, "big.bin")); got != "the only copy of this data" {
		t.Errorf("source big.bin = %q after the copy", got)
	}
	if p := s.copy(t, "d", true, true); p.Failed.Load() != 1 {
		t.Errorf("folder copy into a symlink to the source: failed=%d, want 1", p.Failed.Load())
	}
	if got := readFile(t, filepath.Join(left, "d", "sub", "a.txt")); got != "data behind a symlink" {
		t.Errorf("source d/sub/a.txt = %q after the copy", got)
	}
	if got := readFile(t, filepath.Join(right, "d", "other.txt")); got != "x" {
		t.Errorf("a failed copy still ran the mirror pass: other.txt = %q", got)
	}
}

// L1: one remote URL on both sides is one object store.
func TestSameObjectRemoteLocation(t *testing.T) {
	open := func(context.Context) (model.Backend, error) { return nil, errors.New("not dialed in this test") }
	a := transport.NewLazyBackend("sftp://u:xxxxx@host/data", open)
	b := transport.NewLazyBackend("sftp://u:xxxxx@host/data/", open)
	c := transport.NewLazyBackend("sftp://u:xxxxx@host/other", open)
	if !sameObject(a, b, "f.txt") {
		t.Error("the same URL on both sides is not seen as one object")
	}
	if sameObject(a, c, "f.txt") {
		t.Error("two different URLs are seen as one object")
	}
}

// F2: copying one file removes only that file's own type conflict, never an
// unrelated folder next to it.
func TestScenarioCopyFileLeavesSiblingConflictAlone(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "the file the user copies")
	writeFile(t, filepath.Join(left, "notes"), "on the source 'notes' is a file")
	writeFile(t, filepath.Join(right, "notes", "2024.txt"), "on the destination 'notes' is a folder")
	writeFile(t, filepath.Join(right, "notes", "2025.txt"), "more data")
	s := newScenario(t, left, right)

	if got := model.CollectTypeCollisions(s.node(t, "a.txt"), true); len(got) != 0 {
		t.Errorf("CollectTypeCollisions(a.txt) = %d nodes, want none", len(got))
	}
	if p := s.copy(t, "a.txt", true, false); p.Failed.Load() != 0 {
		t.Errorf("copy a.txt: failed=%d", p.Failed.Load())
	}
	wantTree(t, "copy of a.txt only", right, "a.txt", "notes/", "notes/2024.txt", "notes/2025.txt")
}

// F2: replacing a folder with a file needs the confirmed path; without it the
// folder stays and the copy fails.
func TestScenarioReplaceFolderWithFileNeedsConfirmation(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "notes"), "a file now")
	writeFile(t, filepath.Join(right, "notes", "2024.txt"), "was a folder")
	s := newScenario(t, left, right)
	var file *model.TreeNode
	for _, c := range s.sc.Tree().Children {
		if c.Name == "notes" && !c.IsDir {
			file = c
		}
	}
	if got := model.CollectTypeCollisions(file, true); len(got) != 1 || got[0].RelPath != "notes" || !got[0].IsDir {
		t.Fatalf("CollectTypeCollisions(notes) = %v, want the folder notes/", got)
	}

	p := &Progress{}
	Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: file, RelPath: "notes",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p})
	if p.Failed.Load() != 1 {
		t.Errorf("unconfirmed replace: failed=%d, want 1", p.Failed.Load())
	}
	wantTree(t, "unconfirmed replace", right, "notes/", "notes/2024.txt")

	p = &Progress{}
	Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: file, RelPath: "notes",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: Plan{Conflicts: []Conflict{{RelPath: "notes"}}}})
	if p.Failed.Load() != 0 {
		t.Errorf("confirmed replace: failed=%d, want 0", p.Failed.Load())
	}
	wantTree(t, "confirmed replace", right, "notes")
	if got := readFile(t, filepath.Join(right, "notes")); got != "a file now" {
		t.Errorf("notes = %q after the confirmed replace", got)
	}
}

// F2: a file on the destination in the way of a source folder is replaced
// when confirmed, and the folder's files land under it.
func TestScenarioReplaceFileWithFolder(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "x", "a.txt"), "one")
	writeFile(t, filepath.Join(left, "x", "b.txt"), "two")
	writeFile(t, filepath.Join(right, "x"), "a file in the way")
	s := newScenario(t, left, right)
	if p := s.copy(t, "", true, false, "x"); p.Failed.Load() != 0 {
		t.Errorf("confirmed replace of file x: failed=%d", p.Failed.Load())
	}
	wantTree(t, "file x replaced by folder x/", right, "x/", "x/a.txt", "x/b.txt")
}

// U3: a conflict the copy only finds after listing an unlisted subtree was
// never in a dialog, so it must not be removed.
func TestScenarioUnconfirmedConflictFoundAfterListing(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "A", "y"), "a file on the source")
	writeFile(t, filepath.Join(left, "A", "z.txt"), "copied")
	writeFile(t, filepath.Join(right, "A", "y", "keep.txt"), "a folder with data on the destination")
	s := newScenarioDepth(t, left, right, false)
	if model.UnlistedDir(s.node(t, "A")) == nil {
		t.Fatal("A should be unlisted after a shallow scan")
	}
	p := s.copy(t, "A", true, false)
	if p.Failed.Load() != 1 {
		t.Errorf("failed=%d, want 1 (the file y)", p.Failed.Load())
	}
	wantTree(t, "copy of A without a dialog", right, "A/", "A/y/", "A/y/keep.txt", "A/z.txt")
}

func caseInsensitive(t *testing.T, dir string) bool {
	t.Helper()
	writeFile(t, filepath.Join(dir, ".case-probe"), "")
	_, err := os.Stat(filepath.Join(dir, ".CASE-PROBE"))
	os.Remove(filepath.Join(dir, ".case-probe"))
	return err == nil
}

// F3: on a case-insensitive destination photo.jpg is written into the
// existing Photo.JPG. The mirror pass must not delete Photo.JPG, the file it
// just wrote, but still deletes what really is only on the destination.
func TestScenarioMirrorCaseInsensitiveDestination(t *testing.T) {
	root := t.TempDir()
	if !caseInsensitive(t, root) {
		t.Skip("this filesystem is case-sensitive")
	}
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "album", "photo.jpg"), "the new picture")
	writeFile(t, filepath.Join(left, "Docs", "a.txt"), "new text")
	writeFile(t, filepath.Join(right, "album", "Photo.JPG"), "the old picture")
	writeFile(t, filepath.Join(right, "album", "old.txt"), "only on the destination")
	writeFile(t, filepath.Join(right, "DOCS", "a.txt"), "old")
	s := newScenario(t, left, right)

	p := s.copy(t, "", true, true)
	if p.Failed.Load() != 0 {
		t.Errorf("failed=%d", p.Failed.Load())
	}
	wantTree(t, "mirror to a case-insensitive disk", right, "DOCS/", "DOCS/a.txt", "album/", "album/Photo.JPG")
	if got := readFile(t, filepath.Join(right, "album", "Photo.JPG")); got != "the new picture" {
		t.Errorf("album/Photo.JPG = %q", got)
	}
	if got := readFile(t, filepath.Join(right, "DOCS", "a.txt")); got != "new text" {
		t.Errorf("DOCS/a.txt = %q", got)
	}
}

// F3: on a case-sensitive destination Photo.JPG and photo.jpg are two files,
// so the mirror pass still removes the old name.
func TestScenarioMirrorCaseSensitiveDestination(t *testing.T) {
	left := t.TempDir()
	writeFile(t, filepath.Join(left, "album", "photo.jpg"), "the picture")
	lb := transport.NewLocalBackend(left)
	rb, err := transport.NewFakeBackend("fake://x?dirs=0&files=0")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := rb.Mkdir(ctx, "album", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := rb.CopyFrom(ctx, "album/Photo.JPG", strings.NewReader("the picture"), model.PutParams{Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	sc := model.NewScanner(lb, rb, 1, 1, true)
	sc.Scan(ctx, scenarioOpts)
	s := &scenario{left: left, lb: lb, rb: rb, sc: sc}
	if p := s.copy(t, "album", true, true); p.Failed.Load() != 0 {
		t.Errorf("failed=%d", p.Failed.Load())
	}
	entries, err := rb.List(ctx, "album")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	if !slices.Equal(names, []string{"photo.jpg"}) {
		t.Errorf("case-sensitive destination album/ holds %q, want [photo.jpg]", names)
	}
}

// formsFolded reports whether dir's file system takes the two Unicode forms
// of a name (é as one letter, NFC, or as e and an accent, NFD) for one.
func formsFolded(t *testing.T, dir string) bool {
	t.Helper()
	writeFile(t, filepath.Join(dir, ".form-probe-e\u0301"), "")
	_, err := os.Stat(filepath.Join(dir, ".form-probe-\u00e9"))
	os.Remove(filepath.Join(dir, ".form-probe-e\u0301"))
	return err == nil
}

// L4: a Mac disk takes the two Unicode forms of café.txt for one file and
// keeps the name it already had. The copy of the source's NFC name went
// into the NFD file on the destination; the tree showed the NFD name as on
// the destination alone, and the mirror pass deleted it: the file just
// copied. A name that is really only on the destination, in another
// folder, is still deleted.
func TestScenarioMirrorUnicodeFormsDestination(t *testing.T) {
	root := t.TempDir()
	if !formsFolded(t, root) {
		t.Skip("this filesystem keeps Unicode forms apart")
	}
	nfc, nfd := "caf\u00e9.txt", "cafe\u0301.txt"
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "album", nfc), "the new text")
	writeFile(t, filepath.Join(right, "album", nfd), "the old text")
	writeFile(t, filepath.Join(right, "gone.txt"), "only on the destination")
	s := newScenario(t, left, right)

	p := s.copy(t, "", true, true)
	if p.Failed.Load() != 0 {
		t.Errorf("failed=%d", p.Failed.Load())
	}
	album, err := os.ReadDir(filepath.Join(right, "album"))
	if err != nil {
		t.Fatal(err)
	}
	if len(album) != 1 {
		t.Errorf("album/ holds %d entries after the mirror, want 1: café.txt", len(album))
	}
	b, err := os.ReadFile(filepath.Join(right, "album", nfc))
	if err != nil || string(b) != "the new text" {
		t.Errorf("album/café.txt after the mirror: %q, %v; want the new text", b, err)
	}
	if onDisk(filepath.Join(right, "gone.txt")) {
		t.Error("gone.txt, only on the destination, was not deleted")
	}
}

// The tree is a snapshot: a name that reached the source after the scan is
// not destination-only any more, and the mirror pass keeps it.
func TestScenarioMirrorKeepsNameThatIsNowOnTheSource(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a", "x.txt"), "x")
	writeFile(t, filepath.Join(right, "a", "late.txt"), "made on both sides after the scan")
	writeFile(t, filepath.Join(right, "a", "gone.txt"), "really only on the destination")
	s := newScenario(t, left, right)
	writeFile(t, filepath.Join(left, "a", "late.txt"), "made on both sides after the scan")
	if p := s.copy(t, "a", true, true); p.Failed.Load() != 0 {
		t.Errorf("failed=%d", p.Failed.Load())
	}
	wantTree(t, "mirror after the source changed", right, "a/", "a/late.txt", "a/x.txt")
}

// L5: a source folder whose names can be read but whose entries cannot
// (mode r--) must not list as empty, or mirror deletes its files on the
// other side.
func TestScenarioUnreadableEntriesFailTheListing(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads everything")
	}
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	for _, side := range []string{left, right} {
		writeFile(t, filepath.Join(side, "d", "a.txt"), "same on both sides")
		writeFile(t, filepath.Join(side, "d", "b.txt"), "same on both sides")
	}
	if err := os.Chmod(filepath.Join(left, "d"), 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(left, "d"), 0o755) })

	if entries, err := transport.NewLocalBackend(left).List(context.Background(), "d"); err == nil {
		t.Errorf("List of an r-- folder = %d entries and no error", len(entries))
	}
	s := newScenario(t, left, right)
	if !s.node(t, "d").ListErr {
		t.Error("the folder is not flagged as a failed listing")
	}
	p := s.copy(t, "d", true, true)
	if p.Failed.Load() == 0 {
		t.Error("a mirror copy of an unreadable folder did not fail")
	}
	wantTree(t, "mirror from an unreadable folder", right, "d/", "d/a.txt", "d/b.txt")
}
