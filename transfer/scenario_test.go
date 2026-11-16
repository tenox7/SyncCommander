package transfer

// Scenario tests: two small trees on disk, one user action through the real
// scanner, copy engine and local backend, then the destination is compared
// with what the user asked for.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sort"
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
	left, right string
	lb, rb      model.Backend
	sc          *model.Scanner
}

func newScenario(t *testing.T, left, right string) *scenario {
	t.Helper()
	for _, d := range []string{left, right} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := &scenario{left: left, right: right, lb: transport.NewLocalBackend(left), rb: transport.NewLocalBackend(right)}
	s.sc = model.NewScanner(s.lb, s.rb, 1, 1, true)
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

func (s *scenario) copy(t *testing.T, relPath string, leftToRight, mirror bool) *Progress {
	t.Helper()
	src, dst := s.lb, s.rb
	if !leftToRight {
		src, dst = s.rb, s.lb
	}
	p := &Progress{}
	Copy(context.Background(), Request{Src: src, Dst: dst, Scanner: s.sc, Node: s.node(t, relPath), RelPath: relPath,
		LeftToRight: leftToRight, Mirror: mirror, Opts: scenarioOpts, Parallel: 1, Progress: p})
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
			for _, n := range model.CollectCopyFiles(r, &scenarioOpts, true) {
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
