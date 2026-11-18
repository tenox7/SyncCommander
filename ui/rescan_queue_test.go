package ui

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"sc/model"
)

// listRecorder records each folder listed.
type listRecorder struct {
	model.Backend
	mu    *sync.Mutex
	lists *[]string
}

func (b listRecorder) List(ctx context.Context, rel string) ([]model.FileEntry, error) {
	b.mu.Lock()
	*b.lists = append(*b.lists, rel)
	b.mu.Unlock()
	return b.Backend.List(ctx, rel)
}

// waiting is the rescans that wait for the running scan.
func waiting(m *Model) []rescanReq { return m.pendingRescans }

func changedAt(paths ...string) *model.ChangedPaths {
	c := &model.ChangedPaths{}
	for s := range c.Paths {
		c.Paths[s] = map[string]bool{}
		for _, p := range paths {
			c.Paths[s][p] = true
		}
	}
	return c
}

// queueModel has a/x.txt, ab/w.txt, b/y.txt and c/z.txt on both sides,
// scanned, and records the folders listed from now on.
func queueModel(t *testing.T) (m *Model, node func(rel string) *model.TreeNode, listed func() []string) {
	t.Helper()
	root := t.TempDir()
	var mu sync.Mutex
	var lists []string
	var sides [2]model.Backend
	for i, side := range []string{"L", "R"} {
		for _, f := range []string{"a/x.txt", "ab/w.txt", "b/y.txt", "c/z.txt"} {
			writeTestFile(t, filepath.Join(root, side, f), "1")
		}
		sides[i] = listRecorder{Backend: openBackendOrSkip(t, filepath.Join(root, side)), mu: &mu, lists: &lists}
	}
	m = newTestModel(t, sides[0], sides[1])
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	settled(m)
	mu.Lock()
	lists = nil
	mu.Unlock()
	node = func(rel string) *model.TreeNode {
		n := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.RelPath == rel })
		if n == nil {
			t.Fatalf("no %q in the tree", rel)
		}
		return n
	}
	listed = func() []string {
		mu.Lock()
		defer mu.Unlock()
		l := slices.Clone(lists)
		slices.Sort(l)
		return slices.Compact(l)
	}
	return m, node, listed
}

// U23: a second rescan queued behind a running scan made both one rescan
// of the whole tree with no changed paths: two small copies re-listed
// everything, and a file rewritten with its size and time kept its old
// checksum. Each now waits with its folder and its changed paths, and
// they run in turn when the scan ends.
func TestQueuedRescansKeepTheirFoldersAndChangedPaths(t *testing.T) {
	m, node, listed := queueModel(t)
	a, b := node("a"), node("b")
	ca, cb := changedAt("a/x.txt"), changedAt("b/y.txt")
	scan := runningOp(m, opScan, nil)
	m.queueRescan(a, ca)
	m.queueRescan(b, cb)
	if len(waiting(m)) != 2 || waiting(m)[0].node != a || waiting(m)[0].changed != ca ||
		waiting(m)[1].node != b || waiting(m)[1].changed != cb {
		t.Errorf("waiting: %+v, want a and b with their changed paths", waiting(m))
	}
	cmd := m.finishOp(opDoneMsg{kind: opScan, gen: scan.gen})
	for i := 0; cmd != nil && i < 5; i++ {
		_, cmd = m.Update(runFirst(cmd))
	}
	// Each lists its folder, and the folder above it for its own row.
	if got := listed(); !slices.Equal(got, []string{"", "a", "b"}) {
		t.Errorf("the rescans listed %q, want a and b, and the top folder for their rows", got)
	}
	if len(waiting(m)) > 0 || m.scanning() {
		t.Errorf("after the rescans: %d waiting, scanning %v", len(waiting(m)), m.scanning())
	}
}

// U23: a rescan that a waiting one takes in joins it, with its changed
// paths, and one that takes in others takes their place.
func TestQueuedRescansJoinWhereOneTakesInAnother(t *testing.T) {
	m, node, _ := queueModel(t)
	a, b, top := node("a"), node("b"), m.scanner.Tree()
	runningOp(m, opScan, nil)
	m.queueRescan(a, changedAt("a/one"))
	m.queueRescan(node("a/x.txt"), changedAt("a/x.txt"))
	if len(waiting(m)) != 1 || waiting(m)[0].node != a || !waiting(m)[0].changed.Paths[model.SideLeft]["a/x.txt"] {
		t.Fatalf("waiting: %+v, want a/x.txt joined into a", waiting(m))
	}
	m.queueRescan(a, nil)
	if len(waiting(m)) != 1 || waiting(m)[0].node != a {
		t.Fatalf("waiting: %+v, want a alone", waiting(m))
	}
	m.queueRescan(node("ab"), nil) // ab is not under a
	if len(waiting(m)) != 2 {
		t.Fatalf("waiting: %+v, want a and ab", waiting(m))
	}
	m.queueRescan(b, changedAt("b/y.txt"))
	m.queueRescan(top, changedAt("top"))
	if len(waiting(m)) != 1 || waiting(m)[0].node != top {
		t.Fatalf("waiting: %+v, want the top folder alone", waiting(m))
	}
	c := waiting(m)[0].changed
	for _, p := range []string{"a/one", "a/x.txt", "b/y.txt", "top"} {
		for s := range c.Paths {
			if !c.Paths[s][p] {
				t.Errorf("the top folder's rescan does not name %s on side %d", p, s)
			}
		}
	}
}

// U23: x marks the folder of each waiting rescan as not listed, as it
// marks the folders the stopped scan was listing.
func TestXMarksEveryWaitingRescan(t *testing.T) {
	m, node, _ := queueModel(t)
	a, b := node("a"), node("b")
	runningOp(m, opScan, nil)
	m.queueRescan(a, nil)
	m.queueRescan(b, nil)
	m.stopScanOps()
	if len(waiting(m)) > 0 {
		t.Errorf("%d rescans still wait after x", len(waiting(m)))
	}
	var marked [2]bool
	m.readTree(func(*model.TreeNode) { marked = [2]bool{a.ListErr, b.ListErr} })
	if !marked[0] || !marked[1] {
		t.Errorf("after x: a marked %v, b marked %v; want both", marked[0], marked[1])
	}
}
