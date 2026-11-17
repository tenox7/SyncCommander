package model

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateBackend is a stubBackend that counts each folder's listings. With a
// gate, its first List of that folder waits until release is closed.
type gateBackend struct {
	*stubBackend
	gate    string
	arrived chan struct{} // closed once the first List of gate waits
	release chan struct{}
	mu      sync.Mutex
	lists   map[string]int
}

func newGateBackend(dirs map[string][]FileEntry, gate string) *gateBackend {
	return &gateBackend{stubBackend: &stubBackend{dirs: dirs}, gate: gate,
		arrived: make(chan struct{}), release: make(chan struct{}), lists: map[string]int{}}
}

func (b *gateBackend) List(ctx context.Context, relDir string) ([]FileEntry, error) {
	b.mu.Lock()
	b.lists[relDir]++
	wait := b.gate != "" && relDir == b.gate && b.lists[relDir] == 1
	b.mu.Unlock()
	if wait {
		close(b.arrived)
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.stubBackend.List(ctx, relDir)
}

func (b *gateBackend) listed(relDir string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lists[relDir]
}

// F19: a copy started during a scan lists the folders it copies
// (EnsureSubtreeListed) while the scan lists them too. Here the copy lists
// x and x/y while the scan's listing of x waits. The scan then merged its
// late listing of x into the nodes the copy had filled, without the lock,
// and listed x/y again under them. Now the first to fill a folder wins:
// the scan keeps the copy's nodes and lists nothing under them again.
func TestScanKeepsTheFoldersACopyListedFirst(t *testing.T) {
	dirs := map[string][]FileEntry{
		"":    {dirEntry("x", "x")},
		"x":   {dirEntry("x/y", "y")},
		"x/y": {fileEntry("x/y/f.txt", "f.txt", 10)},
	}
	left, right := newGateBackend(dirs, "x"), newGateBackend(dirs, "")
	opts := CompareOpts{Size: true}
	s := NewScanner(left, right, 1, 2, true)
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		s.Scan(context.Background(), opts)
	}()
	select {
	case <-left.arrived:
	case <-scanned:
		t.Fatal("the scan ended without listing x")
	case <-time.After(30 * time.Second):
		t.Fatal("the scan has not listed x after 30s")
	}

	var x *TreeNode
	s.ReadTree(func(root *TreeNode) { x = findChild(root.Children, "x") })
	if x == nil {
		t.Fatal("the scan has not put x in the tree")
	}
	if !s.EnsureSubtreeListed(context.Background(), x, opts) {
		t.Fatal("the copy could not list x")
	}
	var y *TreeNode
	s.ReadTree(func(*TreeNode) { y = findChild(x.Children, "y") })
	close(left.release)
	// The UI reads the tree while the scan's late listing of x lands. Run
	// with -race: a merge that rewrites the copy's nodes without the lock
	// is a race with this reader.
	for done := false; !done; {
		select {
		case <-scanned:
			done = true
		default:
			s.ReadTree(func(*TreeNode) { _, _ = y.Entries() })
		}
	}

	s.ReadTree(func(root *TreeNode) {
		if got := findChild(findChild(root.Children, "x").Children, "y"); got != y {
			t.Error("the scan replaced the node the copy filled for x/y")
		}
		if y == nil || !y.Listed || findChild(y.Children, "f.txt") == nil {
			t.Error("x/y is not listed with f.txt in it")
		}
	})
	// The folders the copy filled count as listed: the scan ends with no
	// side shown as scanning, and has counted each folder and file once.
	if p := s.Progress(); p.Phase != PhaseDone || p.LeftActive || p.RightActive || p.DirsListed != 3 || p.DirsTotal != 3 || p.TotalDirs != 2 || p.TotalFiles != 1 {
		t.Errorf("after the scan: %+v; want phase done, no side active, 3 of 3 folders listed, 2 folders and 1 file", p)
	}
	for _, b := range []*gateBackend{left, right} {
		if n := b.listed("x/y"); n != 1 {
			t.Errorf("x/y was listed %d times on one side, want once: the scan listed it again under the copy's nodes", n)
		}
	}
}
