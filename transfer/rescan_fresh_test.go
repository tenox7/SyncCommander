package transfer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// W-H4: after a deep scan, an rsync daemon's listings came from the cache
// the scan's recursive listing filled. r showed the old top level, R showed
// the old top level the first time, and r on a folder showed its old
// contents. Now each rescan shows what the server holds.
func TestRescanShowsWhatTheServerHoldsNow(t *testing.T) {
	url, mod := startRsyncd(t)
	left := t.TempDir()
	for _, side := range []string{mod, left} {
		writeFile(t, filepath.Join(side, "keep.txt"), "x")
		writeFile(t, filepath.Join(side, "sub", "a.txt"), "x")
	}
	s := rsyncScenario(t, left, url, mod) // the daemon is the right side
	ctx := context.Background()
	check := func(step, rel string, want ...string) {
		t.Helper()
		var got []string
		s.sc.ReadTree(func(*model.TreeNode) {
			for _, c := range s.node(t, rel).Children {
				if c.Sides[model.SideRight].Entry != nil {
					got = append(got, c.Name)
				}
			}
		})
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s: the server's folder %q shows %q, want %q", step, rel, got, want)
		}
	}
	check("after the scan", "", "keep.txt", "sub")

	// r with the cursor on the top folder lists the top level again.
	if err := os.Remove(filepath.Join(mod, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mod, "top1.txt"), "new")
	s.sc.RefreshTopLevel(ctx, scenarioOpts)
	check("after r", "", "sub", "top1.txt")

	// R lists the whole tree again.
	writeFile(t, filepath.Join(mod, "top2.txt"), "new")
	writeFile(t, filepath.Join(mod, "sub", "b.txt"), "new")
	s.sc.DeepRescanNode(ctx, s.sc.Tree(), scenarioOpts)
	check("after R", "", "sub", "top1.txt", "top2.txt")
	check("after R", "sub", "a.txt", "b.txt")

	// r with the cursor on sub lists sub again.
	writeFile(t, filepath.Join(mod, "sub", "c.txt"), "new")
	s.sc.RescanNode(ctx, s.node(t, "sub"), scenarioOpts, nil)
	check("after r on sub", "sub", "a.txt", "b.txt", "c.txt")
}

// F12, W-H4: a rescan after a copy that lists a few folders lists each of
// them fresh. With no recursive listing to serve them, a listing from the
// cache the scan filled shows the folder as it was then: here a file put on
// the server since, in a folder the copy wrote to.
func TestRescanAfterACopyListsEachFolderFresh(t *testing.T) {
	url, mod := startRsyncd(t)
	right := t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, side := range []string{mod, right} {
		for _, d := range []string{"s0", "s1", "s2", "s3"} {
			p := filepath.Join(side, "d", d, "a.txt")
			writeFile(t, p, "x")
			if err := os.Chtimes(p, at, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeFile(t, filepath.Join(mod, "d", "s1", "new.txt"), "new")
	lb, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(lb) })
	s := scanned(t, mod, right, lb, transport.NewLocalBackend(right)) // the daemon is the source
	writeFile(t, filepath.Join(mod, "d", "s1", "later.txt"), "put on the server after the scan")
	ctx := context.Background()
	node := s.node(t, "d")
	p := &Progress{}
	res := Copy(ctx, Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: "d",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Plan: PlanCopy(s.sc, node, true, scenarioOpts)})
	if p.Failed.Load() != 0 {
		t.Fatalf("%d failed", p.Failed.Load())
	}
	s.sc.RescanNode(ctx, res.RescanRoot, scenarioOpts, res.Changed)
	if n := s.findRel("d/s1/new.txt"); n == nil || n.Compare.Presence != model.PresenceBoth {
		t.Error("after the rescan the tree does not show d/s1/new.txt on both sides")
	}
	if s.findRel("d/s1/later.txt") == nil {
		t.Error("after the rescan the tree does not show d/s1/later.txt, put on the server after the scan: d/s1 came from the cache")
	}
}
