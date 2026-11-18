package transfer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sc/model"
	"sc/transport"
)

// deleteRuns counts the --delete runs do makes in dir: the REMOVE lines the
// log gains that name it. dir is a name no other test uses.
func deleteRuns(dir string, do func()) int {
	count := func() (n int) {
		for _, l := range transport.Log.Lines() {
			if strings.Contains(l, " rsync >>> REMOVE") && strings.Contains(l, dir) {
				n++
			}
		}
		return n
	}
	before := count()
	do()
	return count() - before
}

// R16: a mirror to an rsync daemon deleted each entry only on the
// destination in a --delete run of its own, and each run lists the folder:
// V deletes in a folder of N entries listed it V times. Now a folder's
// deletes go in one run, and each is a changed path, as each delete was
// (F12): the rescan lists what it left.
func TestRsyncMirrorDeletesAFolderInOneRun(t *testing.T) {
	url, mod := startRsyncd(t)
	left := t.TempDir()
	const dir = "r16-mirror"
	writeFile(t, filepath.Join(left, dir, "keep.txt"), "k")
	victims := []string{"x1.txt", "x2.txt", "x 3.txt", "y[1].txt", "sub"}
	for _, rel := range []string{"x1.txt", "x2.txt", "x 3.txt", "y[1].txt", "sub/deep.txt"} {
		writeFile(t, filepath.Join(mod, dir, rel), "d")
	}
	s := rsyncScenario(t, left, url, mod)
	node := s.node(t, dir)
	planned := PlanCopy(s.sc, node, true, scenarioOpts)
	p := &Progress{}
	var res Result
	n := deleteRuns(dir, func() {
		res = Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: dir,
			LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p,
			Plan: Plan{Newer: planned.Newer, Deletes: planned.Deletes, Mirror: true}})
	})
	if n != 1 {
		t.Errorf("the mirror deleted in %d runs, want 1", n)
	}
	if f := p.Failed.Load(); f != 0 {
		t.Errorf("%d failed", f)
	}
	wantTree(t, "after the mirror", filepath.Join(mod, dir), "keep.txt")
	for _, v := range victims {
		if !res.Changed.Paths[model.SideRight][dir+"/"+v] {
			t.Errorf("%s, deleted, is not in the changed paths: %v", v, res.Changed.Paths[model.SideRight])
		}
	}
}

// freshLists is a destination that notes, for each listing of dir, whether
// it asked the server rather than a cache.
type freshLists struct {
	model.Backend
	dir   string
	mu    sync.Mutex
	fresh []bool
}

func (b *freshLists) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if relDir == b.dir {
		b.mu.Lock()
		b.fresh = append(b.fresh, model.WantsFreshListing(ctx))
		b.mu.Unlock()
	}
	return b.Backend.List(ctx, relDir)
}

func (b *freshLists) RemoveIn(ctx context.Context, dir string, items []model.RemoveItem) error {
	return b.Backend.(model.BatchRemover).RemoveIn(ctx, dir, items)
}

// When the run fails, one listing of the folder tells which entries are
// gone: those count as deleted, the rest as failed. Deleting each again in
// a run of its own would be the run per entry the batch saves.
func TestRsyncMirrorCountsWhatAFailedRunLeft(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a folder it may not")
	}
	url, mod := startRsyncd(t)
	left := t.TempDir()
	const dir = "r16-mirror-fails"
	writeFile(t, filepath.Join(left, dir, "keep.txt"), "k")
	for _, rel := range []string{"x1.txt", "x2.txt", "v/f.txt"} {
		writeFile(t, filepath.Join(mod, dir, rel), "d")
	}
	s := rsyncScenario(t, left, url, mod)
	// After the scan: a folder the daemon may not read is one it cannot
	// empty, and it deletes the rest.
	v := filepath.Join(mod, dir, "v")
	if err := os.Chmod(v, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(v, 0o755) })
	dst := &freshLists{Backend: s.rb, dir: dir}
	node := s.node(t, dir)
	planned := PlanCopy(s.sc, node, true, scenarioOpts)
	p := &Progress{}
	n := deleteRuns(dir, func() {
		Copy(context.Background(), Request{Src: s.lb, Dst: dst, Scanner: s.sc, Node: node, RelPath: dir,
			LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p,
			Plan: Plan{Newer: planned.Newer, Deletes: planned.Deletes, Mirror: true}})
	})
	if n != 1 {
		t.Errorf("the mirror deleted in %d runs, want 1", n)
	}
	if k := len(dst.fresh); k == 0 || !dst.fresh[k-1] {
		t.Errorf("the listing that tells what the run left came from a cache: fresh %v", dst.fresh)
	}
	if f := p.Failed.Load(); f != 1 {
		t.Errorf("%d failed, want 1: the folder it could not delete", f)
	}
	if err := os.Chmod(v, 0o755); err != nil {
		t.Fatal(err)
	}
	wantTree(t, "after the mirror", filepath.Join(mod, dir), "keep.txt", "v/", "v/f.txt")
	failed, deleted := false, 0
	for _, l := range transport.Log.Lines() {
		failed = failed || strings.Contains(l, " ERR mirror delete "+dir+"/v: ")
		if strings.Contains(l, " <<< mirror delete "+dir+"/x") {
			deleted++
		}
	}
	if !failed || deleted < 2 {
		t.Errorf("the log says v failed: %v, and the x files went: %d of 2", failed, deleted)
	}
}
