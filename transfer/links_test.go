//go:build unix

package transfer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
	"sc/transport"
)

// linkTo makes p a link to target, with its folder.
func linkTo(t *testing.T, target, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
}

// wantLink checks that p is a link holding target.
func wantLink(t *testing.T, p, target string) {
	t.Helper()
	got, err := os.Readlink(p)
	if err != nil || got != target {
		t.Errorf("%s: a link to %q (%v); want a link to %q", p, got, err, target)
	}
}

// linkRow finds the link row named by relPath; a file or folder of the
// name is another row.
func linkRow(t *testing.T, s *scenario, relPath string) *model.TreeNode {
	t.Helper()
	var found *model.TreeNode
	s.sc.ReadTree(func(*model.TreeNode) {
		for _, c := range s.node(t, model.DirOf(relPath)).Children {
			if c.RelPath == relPath && c.IsLink {
				found = c
			}
		}
	})
	if found == nil {
		t.Fatalf("no link row %s", relPath)
	}
	return found
}

func linkStatus(t *testing.T, s *scenario, relPath string) model.AttrStatus {
	t.Helper()
	n := linkRow(t, s, relPath)
	var st model.AttrStatus
	s.sc.ReadTree(func(*model.TreeNode) { st = model.NodeStatus(n, &scenarioOpts) })
	return st
}

// 2026-11-20: links were left out of every copy. Now a link is copied as a
// link to the same target, the text as it is: relative, absolute, to a
// folder, or to nothing at all on the destination. Nothing is read through
// it. Two links compare by their targets, so one changed to a target of the
// same length differs, and the copy writes the new target.
func TestLinksCopyAsLinks(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	writeFile(t, filepath.Join(left, "a.txt"), "a")
	writeFile(t, filepath.Join(left, "sub", "s.txt"), "s")
	links := map[string]string{
		"rel.lnk":    "a.txt",
		"abs.lnk":    "/nonexistent/sc-link-test",
		"dir.lnk":    "sub",
		"sub/up.lnk": "../a.txt",
	}
	for name, target := range links {
		linkTo(t, target, filepath.Join(left, name))
	}
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	s := newScenario(t, left, right)
	if p := s.copy(t, "", true, false); p.Failed.Load() != 0 {
		t.Fatalf("copy: %d failed", p.Failed.Load())
	}
	for name, target := range links {
		wantLink(t, filepath.Join(right, name), target)
	}
	wantTree(t, "copy of a folder with links", right, "a.txt", "abs.lnk", "dir.lnk", "rel.lnk", "sub/", "sub/s.txt", "sub/up.lnk")

	s = newScenario(t, left, right)
	for name := range links {
		if st := linkStatus(t, s, name); st != model.AttrEqual {
			t.Errorf("%s after the copy: status %v, want equal", name, st)
		}
	}

	// b.txt is as long as a.txt: the size of the links is the same.
	if err := os.Remove(filepath.Join(left, "rel.lnk")); err != nil {
		t.Fatal(err)
	}
	linkTo(t, "b.txt", filepath.Join(left, "rel.lnk"))
	s = newScenario(t, left, right)
	if st := linkStatus(t, s, "rel.lnk"); st != model.AttrDifferent {
		t.Errorf("rel.lnk with a new target: status %v, want different", st)
	}
	if p := s.copy(t, "", true, false); p.Failed.Load() != 0 {
		t.Fatalf("second copy: %d failed", p.Failed.Load())
	}
	wantLink(t, filepath.Join(right, "rel.lnk"), "b.txt")
}

// A link on the source where the destination has a file of its name: the
// file is a twin in the way. Without the user's yes it stays and the link
// fails; with it, the file goes and the link is made.
func TestLinkReplacesTwinOnlyWithYes(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "left"), filepath.Join(root, "right")
	linkTo(t, "elsewhere", filepath.Join(left, "x"))
	writeFile(t, filepath.Join(right, "x"), "data")
	s := newScenario(t, left, right)
	if n := rowsNamed(s, "x"); n != 2 {
		t.Fatalf("%d rows named x; want 2, the link and the file", n)
	}
	if p := s.copy(t, "", true, false); p.Failed.Load() != 1 {
		t.Errorf("without the user's yes: %d failed, want 1", p.Failed.Load())
	}
	if got := readFile(t, filepath.Join(right, "x")); got != "data" {
		t.Errorf("without the user's yes, right/x holds %q", got)
	}
	if p := s.copy(t, "", true, false, "x"); p.Failed.Load() != 0 {
		t.Errorf("with the user's yes: %d failed", p.Failed.Load())
	}
	wantLink(t, filepath.Join(right, "x"), "elsewhere")
}

// Skip + warn (user's choice, 2026-11-20): a destination that cannot hold
// links gets none. That is no failure, and a mirror goes on.
func TestLinkToDestinationWithoutLinks(t *testing.T) {
	ctx := context.Background()
	left := t.TempDir()
	writeFile(t, filepath.Join(left, "a.txt"), "a")
	linkTo(t, "a.txt", filepath.Join(left, "l"))
	fake := newFake(t, "fake://dst?dirs=0&files=0")
	if err := fake.CopyFrom(ctx, "old.txt", strings.NewReader("old"), model.PutParams{Size: 3, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	s := scanned(t, left, "", transport.NewLocalBackend(left), fake)
	mark := len(transport.Log.Lines())
	p := s.copy(t, "", true, true)
	if p.Failed.Load() != 0 {
		t.Errorf("%d failed; a link the destination cannot hold is no failure", p.Failed.Load())
	}
	if !logged(mark, "COPY l: skipped, a symbolic link (the destination cannot hold links)") {
		t.Error("the copy did not say why it left the link out")
	}
	if got, ok := holds(t, fake, "a.txt"); !ok || got != "a" {
		t.Errorf("a.txt on the destination: %q, %v", got, ok)
	}
	if _, ok := holds(t, fake, "l"); ok {
		t.Error("the destination holds l")
	}
	if _, ok := holds(t, fake, "old.txt"); ok {
		t.Error("the mirror did not delete old.txt")
	}
}

// twinLister lists, in its top folder, a link d besides what the disk
// holds: a server that sends what it likes (L7).
type twinLister struct {
	model.Backend
	target string
}

func (b twinLister) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	list, err := b.Backend.List(ctx, relDir)
	if err == nil && relDir == "" {
		list = append(list, model.FileEntry{RelPath: "d", Name: "d", Mode: os.ModeSymlink | 0o777, Link: model.LinkTo(b.target)})
	}
	return list, err
}

// A source that lists d twice, as a link to a folder out of the tree and
// as a folder: the link made first, the folder's files went through it,
// out of the tree. The link is refused, and the folder is copied.
func TestSourceListingALinkTwiceIsRefused(t *testing.T) {
	root := t.TempDir()
	left, right, outside := filepath.Join(root, "left"), filepath.Join(root, "right"), filepath.Join(root, "outside")
	writeFile(t, filepath.Join(left, "d", "passwd"), "x")
	for _, d := range []string{right, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := scanned(t, left, right, twinLister{Backend: transport.NewLocalBackend(left), target: outside}, transport.NewLocalBackend(right))
	mark := len(transport.Log.Lines())
	p := s.copy(t, "", true, false)
	if p.Failed.Load() != 1 {
		t.Errorf("%d failed; want 1, the link", p.Failed.Load())
	}
	if !logged(mark, "the source lists d twice, as a symbolic link and as a folder") {
		t.Error("the copy did not say why it refused the link")
	}
	if onDisk(filepath.Join(outside, "passwd")) {
		t.Error("the copy wrote out of the tree, through the link")
	}
	if isType(filepath.Join(right, "d"), os.ModeSymlink) || readFile(t, filepath.Join(right, "d", "passwd")) != "x" {
		t.Error("right/d is not the copied folder")
	}
}

// linksNamesAs is a local destination that matches names by rule.
type linksNamesAs struct {
	*transport.LocalBackend
	rule model.NameRule
}

func (b linksNamesAs) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.LocalBackend.Capabilities(ctx)
	caps.Names = b.rule
	return caps, err
}

// D10 with links: a link D to a folder out of the tree and a folder d, on a
// destination that folds letter case, are one name there. Written, the
// folder's files went through the link. Both are refused.
func TestLinkTakesANameWhereNamesFold(t *testing.T) {
	root := t.TempDir()
	left, right, outside := filepath.Join(root, "left"), filepath.Join(root, "right"), filepath.Join(root, "outside")
	writeFile(t, filepath.Join(left, "d", "passwd"), "x")
	for _, d := range []string{right, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	src := capsLinkD{Backend: transport.NewLocalBackend(left), target: outside}
	dst := linksNamesAs{LocalBackend: transport.NewLocalBackend(right), rule: model.NameRule{Case: model.Folded, Forms: model.Folded}}
	s := scanned(t, left, right, src, dst)
	mark := len(transport.Log.Lines())
	p := s.copy(t, "", true, false)
	if p.Failed.Load() != 2 {
		t.Errorf("%d failed; want 2, D and d/passwd", p.Failed.Load())
	}
	if !logged(mark, "the destination takes D and d for one name") {
		t.Error("the copy did not say why it refused the link")
	}
	if onDisk(filepath.Join(outside, "passwd")) {
		t.Error("the copy wrote out of the tree, through the link")
	}
	if onDisk(filepath.Join(right, "D")) || onDisk(filepath.Join(right, "d")) {
		t.Error("the destination holds D or d")
	}
}

// capsLinkD lists, in its top folder, a link D besides what the disk
// holds: a folder of a case-sensitive disk, which a Mac's test folder is
// not.
type capsLinkD struct {
	model.Backend
	target string
}

func (b capsLinkD) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	list, err := b.Backend.List(ctx, relDir)
	if err == nil && relDir == "" {
		list = append(list, model.FileEntry{RelPath: "D", Name: "D", Mode: os.ModeSymlink | 0o777, Link: model.LinkTo(b.target)})
	}
	return list, err
}

// L2 with links: the destination has sub, a link to a folder out of the
// tree, and the source a folder sub with a link in it. Made there, the link
// went through sub into the folder out of the tree. Without the user's yes
// nothing is written; with it, the link sub goes and a folder takes its
// place.
func TestLinkIsNotMadeThroughADestinationLink(t *testing.T) {
	root := t.TempDir()
	left, right, outside := filepath.Join(root, "left"), filepath.Join(root, "right"), filepath.Join(root, "outside")
	linkTo(t, "a.txt", filepath.Join(left, "sub", "x.lnk"))
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	linkTo(t, outside, filepath.Join(right, "sub"))
	s := newScenario(t, left, right)
	if p := s.copy(t, "", true, false); p.Failed.Load() == 0 {
		t.Error("without the user's yes: no failure")
	}
	if onDisk(filepath.Join(outside, "x.lnk")) {
		t.Fatal("without the user's yes, the copy made a link out of the tree, through the link sub")
	}
	if p := s.copy(t, "", true, false, "sub"); p.Failed.Load() != 0 {
		t.Errorf("with the user's yes: %d failed", p.Failed.Load())
	}
	if onDisk(filepath.Join(outside, "x.lnk")) || isType(filepath.Join(right, "sub"), os.ModeSymlink) {
		t.Error("with the user's yes, right/sub is still the link, or the link went through it")
	}
	wantLink(t, filepath.Join(right, "sub", "x.lnk"), "a.txt")
}
