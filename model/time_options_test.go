package model

import (
	"context"
	"path"
	"testing"
	"time"
)

// timedEntry is a file of one byte at at, kept to the nanosecond.
func timedEntry(rel string, at time.Time) FileEntry {
	return FileEntry{RelPath: rel, Name: path.Base(rel), Size: 1, ModTime: at}
}

var timesAt = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

// mtimesOf is how the tree compares the mtimes of rels.
func mtimesOf(t *testing.T, s *Scanner, rels ...string) map[string]AttrStatus {
	t.Helper()
	got := map[string]AttrStatus{}
	s.ReadTree(func(root *TreeNode) {
		for _, rel := range rels {
			n := findNode(root, rel)
			if n == nil {
				t.Fatalf("%s is not in the tree", rel)
			}
			got[rel] = n.Compare.ModTime
		}
	})
	return got
}

// U16: sub-second, grace and TZ/DST were applied as each folder was
// listed only, so a change in Settings showed at the next rescan, and
// nothing said so. SetTimeOptions compares the tree's times again.
func TestSetTimeOptionsComparesTheTreeAgain(t *testing.T) {
	left := &stubBackend{dirs: map[string][]FileEntry{
		"":  {timedEntry("sub.txt", timesAt), timedEntry("grace.txt", timesAt), timedEntry("zone.txt", timesAt), dirEntry("d", "d")},
		"d": {timedEntry("d/sub.txt", timesAt)},
	}}
	right := &stubBackend{dirs: map[string][]FileEntry{
		"": {
			timedEntry("sub.txt", timesAt.Add(500*time.Millisecond)),
			timedEntry("grace.txt", timesAt.Add(time.Second)),
			timedEntry("zone.txt", timesAt.Add(2*time.Hour)),
			dirEntry("d", "d"),
		},
		"d": {timedEntry("d/sub.txt", timesAt.Add(500*time.Millisecond))},
	}}
	s := NewScanner(left, right, 1, 1, true)
	opts := CompareOpts{Size: true, ModTime: true}
	s.Scan(context.Background(), opts)
	E, D := AttrEqual, AttrDifferent
	check := func(what string, sub, grace, zone AttrStatus) {
		t.Helper()
		got := mtimesOf(t, s, "sub.txt", "grace.txt", "zone.txt", "d/sub.txt")
		if got["sub.txt"] != sub || got["grace.txt"] != grace || got["zone.txt"] != zone || got["d/sub.txt"] != sub {
			t.Errorf("%s: sub, grace, zone, d/sub compare %v %v %v %v; want %v %v %v %v", what,
				got["sub.txt"], got["grace.txt"], got["zone.txt"], got["d/sub.txt"], sub, grace, zone, sub)
		}
	}
	check("as scanned", E, D, D)
	rev := s.Rev()
	s.SetTimeOptions(opts)
	if s.Rev() != rev {
		t.Error("the options the scan ran with changed the tree's revision")
	}
	for _, c := range []struct {
		what             string
		set              func(*CompareOpts)
		sub, grace, zone AttrStatus
	}{
		{"sub-second on", func(o *CompareOpts) { o.SubSecond = true }, D, D, D},
		{"grace on", func(o *CompareOpts) { o.TimeGrace = true }, E, E, D},
		{"TZ/DST on", func(o *CompareOpts) { o.IgnoreTZDST = true }, E, E, E},
		{"all off", func(o *CompareOpts) { o.SubSecond, o.TimeGrace, o.IgnoreTZDST = false, false, false }, E, D, D},
	} {
		c.set(&opts)
		rev := s.Rev()
		s.SetTimeOptions(opts)
		if s.Rev() == rev {
			t.Errorf("%s: the tree's revision is the same: the rollups are not counted again", c.what)
		}
		check(c.what, c.sub, c.grace, c.zone)
	}
	rev = s.Rev()
	opts.Size = false
	s.SetTimeOptions(opts)
	if s.Rev() != rev {
		t.Error("the same time options again changed the tree's revision")
	}
}

// A scan that runs as the options change compares what it lists after the
// change with them, as the rest of the tree is compared.
func TestTimeOptionsReachAScanInFlight(t *testing.T) {
	hung := &hungBackend{stubBackend: &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("a", "a"), timedEntry("x.txt", timesAt)},
		"a": {timedEntry("a/f.txt", timesAt)},
	}}, hang: "a", arrived: make(chan struct{}), release: make(chan struct{})}
	right := &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("a", "a"), timedEntry("x.txt", timesAt.Add(500*time.Millisecond))},
		"a": {timedEntry("a/f.txt", timesAt.Add(500*time.Millisecond))},
	}}
	s := NewScanner(hung, right, 1, 1, true)
	opts := CompareOpts{Size: true, ModTime: true}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Scan(context.Background(), opts)
	}()
	select {
	case <-hung.arrived:
	case <-time.After(5 * time.Second):
		close(hung.release)
		t.Fatal("the scan did not list a")
	}
	opts.SubSecond = true
	s.SetTimeOptions(opts)
	close(hung.release)
	<-done
	got := mtimesOf(t, s, "x.txt", "a/f.txt")
	if got["x.txt"] != AttrDifferent || got["a/f.txt"] != AttrDifferent {
		t.Errorf("x.txt, listed before the change, and a/f.txt, after it, compare %v and %v; want both different",
			got["x.txt"], got["a/f.txt"])
	}
}

// The copy and the other operations pass the options they read as they
// started: what each lists or merges after a change is compared with the
// latest time options, not with those.
func TestOldOptionsMergeWithTheLatestTimes(t *testing.T) {
	later := timesAt.Add(500 * time.Millisecond)
	left := &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("a", "a"), timedEntry("x.txt", timesAt), timedEntry("p.txt", timesAt)},
		"a": {timedEntry("a/f.txt", timesAt)},
	}}
	right := &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("a", "a"), timedEntry("x.txt", later), timedEntry("q.txt", later)},
		"a": {timedEntry("a/f.txt", later)},
	}}
	s := NewScanner(left, right, 1, 1, true)
	old := CompareOpts{Size: true, ModTime: true}
	ctx := context.Background()
	s.Scan(ctx, old)
	latest := old
	latest.SubSecond = true
	s.SetTimeOptions(latest)
	node := func(rel string) (n *TreeNode) {
		s.ReadTree(func(root *TreeNode) { n = findNode(root, rel) })
		return n
	}
	for _, c := range []struct {
		what string
		rel  string
		do   func()
	}{
		{"RefreshTopLevel", "x.txt", func() { s.RefreshTopLevel(ctx, old) }},
		{"ListNode", "a/f.txt", func() { s.ListNode(ctx, node("a"), old) }},
		{"RescanNode of a folder", "a/f.txt", func() { s.RescanNode(ctx, node("a"), old, nil) }},
		{"RescanNode of a file", "x.txt", func() { s.RescanNode(ctx, node("x.txt"), old, nil) }},
		{"RelistDir", "a/f.txt", func() { s.RelistDir(ctx, "a", old) }},
		{"EnsureSubtreeListed", "a/f.txt", func() {
			s.MutateTree(func(root *TreeNode) { findNode(root, "a").Listed = false })
			s.EnsureSubtreeListed(ctx, node("a"), old)
		}},
		{"RenameNode onto a twin", "q.txt", func() { s.RenameNode(node("p.txt"), "q.txt", "q.txt", "p.txt", old) }},
	} {
		c.do()
		if got := mtimesOf(t, s, c.rel)[c.rel]; got != AttrDifferent {
			t.Errorf("%s: %s compares %v; want different", c.what, c.rel, got)
		}
	}
}

// A folder the scan merged before the options changed, and puts in the
// tree after: it is compared again as it goes in. White box: the options
// change while the tree's lock holds the scan back.
func TestTimeOptionsReachAFolderMergedBeforeTheChange(t *testing.T) {
	hung := &hungBackend{stubBackend: &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("a", "a")},
		"a": {timedEntry("a/f.txt", timesAt)},
	}}, hang: "a", arrived: make(chan struct{}), release: make(chan struct{})}
	right := &stubBackend{dirs: map[string][]FileEntry{
		"":  {dirEntry("a", "a")},
		"a": {timedEntry("a/f.txt", timesAt.Add(500*time.Millisecond))},
	}}
	s := NewScanner(hung, right, 1, 1, true)
	opts := CompareOpts{Size: true, ModTime: true}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Scan(context.Background(), opts)
	}()
	<-hung.arrived
	s.ReadTree(func(*TreeNode) {
		close(hung.release)
		time.Sleep(300 * time.Millisecond) // a is merged, and waits for the lock
		now := timeOptions{subSecond: true}
		s.times.Store(&now)
	})
	<-done
	if got := mtimesOf(t, s, "a/f.txt")["a/f.txt"]; got != AttrDifferent {
		t.Errorf("a/f.txt compares %v; want different", got)
	}
}
