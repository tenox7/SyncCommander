package transfer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sc/model"
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
