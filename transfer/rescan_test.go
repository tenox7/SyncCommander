package transfer

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// listCounter counts the folder listings a backend serves.
type listCounter struct {
	model.Backend
	lists atomic.Int64
}

func (c *listCounter) List(ctx context.Context, dir string) ([]model.FileEntry, error) {
	c.lists.Add(1)
	return c.Backend.List(ctx, dir)
}

// findRel finds a node by its relative path, or nil.
func (s *scenario) findRel(relPath string) *model.TreeNode {
	var found *model.TreeNode
	s.sc.ReadTree(func(root *model.TreeNode) {
		n := root
		for _, part := range splitRel(relPath) {
			i := slices.IndexFunc(n.Children, func(c *model.TreeNode) bool { return c.Name == part })
			if i < 0 {
				return
			}
			n = n.Children[i]
		}
		found = n
	})
	return found
}

// F12: after a copy, the rescan listed the folder it started from again,
// and with a deep scan every folder under it: copying one file at the top
// listed both sides whole. Now it lists the folders the copy changed
// something in, and the tree still shows all the copy did: a file, the
// folders made on the way to it, an empty folder, a mirror delete.
func TestRescanAfterACopyListsOnlyWhatChanged(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	put := func(p, body string) {
		writeFile(t, p, body)
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 6 {
		for j := range 4 {
			for _, side := range []string{left, right} {
				put(filepath.Join(side, fmt.Sprintf("d%d", i), fmt.Sprintf("s%d", j), "f.txt"), "same")
			}
		}
	}
	put(filepath.Join(left, "top.txt"), "new at the top")
	put(filepath.Join(left, "d1", "mid.txt"), "new in d1")
	put(filepath.Join(left, "d2", "n1", "n2", "deep.txt"), "new, two folders down that the right lacks")
	if err := os.MkdirAll(filepath.Join(left, "d3", "s0", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	put(filepath.Join(right, "d4", "s1", "extra.txt"), "only on the right")

	lb := &listCounter{Backend: transport.NewLocalBackend(left)}
	rb := &listCounter{Backend: transport.NewLocalBackend(right)}
	s := scanned(t, left, right, lb, rb)
	for _, tc := range []struct {
		rel    string
		mirror bool
		lists  int64    // at most, both sides together
		both   []string // on both sides after the rescan
		gone   []string // no longer in the tree
	}{
		// The root, listed on both sides.
		{"top.txt", false, 2, []string{"top.txt"}, nil},
		// d1 and the root, to read d1's own entry.
		{"d1/mid.txt", false, 4, []string{"d1/mid.txt"}, nil},
		// The rescan starts at d2, the deepest folder the right had.
		{"d2/n1/n2/deep.txt", false, 8, []string{"d2/n1", "d2/n1/n2", "d2/n1/n2/deep.txt"}, nil},
		// A folder copy: the root, d3, s0 and the new empty folder.
		{"d3", false, 8, []string{"d3/s0/empty"}, nil},
		// A mirror: the root, d4, and s1 where it deleted.
		{"d4", true, 6, nil, []string{"d4/s1/extra.txt"}},
	} {
		node := s.findRel(tc.rel)
		if node == nil {
			t.Fatalf("%s not in the tree", tc.rel)
		}
		plan := PlanCopy(s.sc, node, true, scenarioOpts)
		plan.Mirror = tc.mirror
		p := &Progress{}
		res := Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: tc.rel,
			LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: plan})
		if p.Failed.Load() != 0 {
			t.Fatalf("%s: %d failed", tc.rel, p.Failed.Load())
		}
		before := lb.lists.Load() + rb.lists.Load()
		s.sc.RescanNode(context.Background(), res.RescanRoot, scenarioOpts, res.Changed)
		if got := lb.lists.Load() + rb.lists.Load() - before; got > tc.lists {
			t.Errorf("copy %s: the rescan from %q listed %d folders, want at most %d", tc.rel, res.RescanRoot.RelPath, got, tc.lists)
		}
		for _, rel := range tc.both {
			if n := s.findRel(rel); n == nil || n.Compare.Presence != model.PresenceBoth {
				t.Errorf("copy %s: after the rescan the tree does not show %s on both sides", tc.rel, rel)
			}
		}
		for _, rel := range tc.gone {
			if s.findRel(rel) != nil {
				t.Errorf("copy %s: after the rescan the tree still shows %s", tc.rel, rel)
			}
		}
	}
}

// halfReads is a local source whose reads fail for good after n bytes: no
// try again gets further.
type halfReads struct {
	*transport.LocalBackend
	n int64
}

func (s *halfReads) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return s.OpenAt(ctx, relPath, 0)
}

func (s *halfReads) OpenAt(ctx context.Context, relPath string, off int64) (io.ReadCloser, error) {
	rc, err := s.LocalBackend.OpenAt(ctx, relPath, off)
	if err != nil {
		return nil, err
	}
	return &failAfter{rc: rc, left: s.n}, nil
}

type failAfter struct {
	rc   io.ReadCloser
	left int64
}

func (r *failAfter) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, fmt.Errorf("read: %w", fs.ErrPermission)
	}
	n, err := r.rc.Read(p[:min(int64(len(p)), r.left)])
	r.left -= int64(n)
	return n, err
}

func (r *failAfter) Close() error { return r.rc.Close() }

// F12: what a copy tried to write or remove goes in the changed paths, also
// when it failed half way: the rescan lists only the folders of changed
// paths, and a file written half way is not what the tree held.
func TestAFailedCopyStillCountsAsAChange(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "s", "big.txt"), strings.Repeat("new bytes ", 100))
	writeFile(t, filepath.Join(right, "d", "s", "big.txt"), "old")
	src := &halfReads{LocalBackend: transport.NewLocalBackend(left), n: 10}
	s := scanned(t, left, right, src, &listCounter{Backend: transport.NewLocalBackend(right)})
	node := s.node(t, "d")
	p := &Progress{}
	res := Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: "d",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: PlanCopy(s.sc, node, true, scenarioOpts)})
	if p.Failed.Load() != 1 {
		t.Fatalf("%d failed, want 1: the test needs a copy that fails", p.Failed.Load())
	}
	if got := readFile(t, filepath.Join(right, "d", "s", "big.txt")); got == "old" {
		t.Log("the failed copy left the destination as it was; the test still checks the changed paths")
	}
	if !res.Changed.Paths[model.SideRight]["d/s/big.txt"] {
		t.Errorf("the copy of d/s/big.txt failed and is not in the changed paths: %v", res.Changed.Paths[model.SideRight])
	}
}

// preloadRecorder is a local backend that says it lists recursively in one
// go, as the network backends do, and records each folder it was asked to.
type preloadRecorder struct {
	listCounter
	mu     sync.Mutex
	scopes []string
}

func (p *preloadRecorder) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := p.Backend.Capabilities(ctx)
	caps.Preload = true
	return caps, err
}

func (p *preloadRecorder) PreloadRecursive(_ context.Context, scope string) error {
	p.mu.Lock()
	p.scopes = append(p.scopes, scope)
	p.mu.Unlock()
	return nil
}

func (p *preloadRecorder) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.scopes
	p.scopes = nil
	return s
}

// F12: a deep scan's rescan started a recursive listing of the folder it
// rescans, on the servers that have one: after a copy of one file at the
// top, a listing of both whole trees. Now a rescan after a copy starts one
// only when it lists most folders under it anyway; r still starts one.
func TestRescanListsRecursivelyOnlyWhenMostFoldersChanged(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	put := func(p, body string) {
		writeFile(t, p, body)
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		for j := range 4 {
			for _, side := range []string{left, right} {
				put(filepath.Join(side, fmt.Sprintf("d%d", i), fmt.Sprintf("s%d", j), "f.txt"), "same")
			}
			put(filepath.Join(left, "d0", fmt.Sprintf("s%d", j), "new.txt"), "new in every folder of d0")
		}
	}
	put(filepath.Join(left, "top.txt"), "new at the top")
	lb := &preloadRecorder{listCounter: listCounter{Backend: transport.NewLocalBackend(left)}}
	rb := &preloadRecorder{listCounter: listCounter{Backend: transport.NewLocalBackend(right)}}
	s := scanned(t, left, right, lb, rb)
	lb.take()
	rb.take()
	for _, tc := range []struct {
		rel  string
		want []string // the folders each side listed recursively
	}{
		{"top.txt", nil},
		{"d0", []string{"d0"}},
	} {
		node := s.findRel(tc.rel)
		p := &Progress{}
		res := Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: tc.rel,
			LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: PlanCopy(s.sc, node, true, scenarioOpts)})
		if p.Failed.Load() != 0 {
			t.Fatalf("%s: %d failed", tc.rel, p.Failed.Load())
		}
		lb.take()
		rb.take()
		s.sc.RescanNode(context.Background(), res.RescanRoot, scenarioOpts, res.Changed)
		for side, b := range map[string]*preloadRecorder{"left": lb, "right": rb} {
			if got := b.take(); !slices.Equal(got, tc.want) {
				t.Errorf("copy %s: the rescan listed %q recursively on the %s, want %q", tc.rel, got, side, tc.want)
			}
		}
	}
	s.sc.RescanNode(context.Background(), s.findRel(""), scenarioOpts, nil)
	for side, b := range map[string]*preloadRecorder{"left": lb, "right": rb} {
		if got := b.take(); !slices.Equal(got, []string{""}) {
			t.Errorf("r: the rescan listed %q recursively on the %s, want the whole tree", got, side)
		}
	}
}

// halfRemove is a local destination whose folder removal removes a.txt in
// the folder and then fails.
type halfRemove struct{ *transport.LocalBackend }

func (b halfRemove) RemoveAll(ctx context.Context, relPath string) error {
	_ = b.LocalBackend.Remove(ctx, relPath+"/a.txt")
	return fmt.Errorf("remove %s: %w", relPath, fs.ErrPermission)
}

// F12: a folder in the way of a copied file, removed half way, is a change
// too: the rescan lists its folder again.
func TestAFailedRemovalStillCountsAsAChange(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "x"), "a file on the left")
	writeFile(t, filepath.Join(right, "d", "x", "a.txt"), "a folder on the right")
	writeFile(t, filepath.Join(right, "d", "x", "b.txt"), "a folder on the right")
	s := scanned(t, left, right, transport.NewLocalBackend(left), halfRemove{transport.NewLocalBackend(right)})
	node := s.node(t, "d")
	plan := PlanCopy(s.sc, node, true, scenarioOpts)
	if len(plan.Conflicts) != 1 {
		t.Fatalf("the plan holds %d conflicts, want d/x", len(plan.Conflicts))
	}
	p := &Progress{}
	res := Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: "d",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: plan})
	if p.Failed.Load() != 1 {
		t.Fatalf("%d failed, want 1: the test needs a removal that fails", p.Failed.Load())
	}
	if !res.Changed.Paths[model.SideRight]["d/x"] {
		t.Errorf("the folder d/x, removed half way, is not in the changed paths: %v", res.Changed.Paths[model.SideRight])
	}
}
