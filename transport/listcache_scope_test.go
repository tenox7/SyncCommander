package transport

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"sc/model"
)

// waitHit waits until the cache serves dir as complete.
func waitHit(t *testing.T, c *listCache, dir string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, hit, _, _ := c.lookup(dir); hit {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is not served from the cache 5s later", dir)
		}
		time.Sleep(time.Millisecond)
	}
}

func namesOfEntries(es []model.FileEntry) []string {
	var names []string
	for _, e := range es {
		names = append(names, e.Name)
	}
	return names
}

func entry(name string) []model.FileEntry { return []model.FileEntry{{Name: name}} }

// R9, W-M7: the cache held one run, and a preload of another folder while
// it went was ignored: each folder of it was listed by itself. Now it runs
// beside the first, and each run serves its own folders; one that ends
// whole marks none of the other's complete.
func TestListCacheSecondScopeRunsBesideTheFirst(t *testing.T) {
	c := newListCache()
	ga, gb := newRunGate(t), newRunGate(t)
	aStopped := make(chan error, 1)
	c.start(context.Background(), "a", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		select {
		case <-ga.ch:
		case <-ctx.Done():
			aStopped <- ctx.Err()
			return ctx.Err()
		}
		emit("a/y", entry("from-a"))
		complete("a/y")
		return nil
	})
	inA := serveWith(c, context.Background(), "a/y")
	stillWaits(t, inA)
	c.start(context.Background(), "b", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("b/x", entry("x1"))
		select {
		case <-gb.ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		emit("b/x", entry("x2"))
		complete("b/x")
		return nil
	})
	inB := serveWith(c, context.Background(), "b/x")
	stillWaits(t, inB)
	ga.open()
	if o := served(t, inA); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"from-a"}) {
		t.Errorf("a/y: %q, err %v, live %v; want the run of a's [from-a]", namesOfEntries(o.entries), o.err, o.lived)
	}
	stillWaits(t, inB)
	gb.open()
	if o := served(t, inB); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"x1", "x2"}) {
		t.Errorf("b/x: %q, err %v, live %v; want the run of b's [x1 x2]", namesOfEntries(o.entries), o.err, o.lived)
	}
	select {
	case err := <-aStopped:
		t.Errorf("the run of a was stopped: %v", err)
	default:
	}
}

// A preload of a folder that holds a run's scope was ignored too, and its
// folders outside the run were listed by themselves. Now it takes the run
// over: the run is stopped, the folders it listed in full are kept and not
// taken again, and the rest are the new run's.
func TestListCacheScopeHoldingARunTakesItOver(t *testing.T) {
	c := newListCache()
	g := newRunGate(t)
	innerStopped := make(chan error, 1)
	c.start(context.Background(), "a/b", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a/b/c", entry("c1"))
		complete("a/b/c")
		emit("a/b/d", entry("partial"))
		<-ctx.Done()
		innerStopped <- ctx.Err()
		return ctx.Err()
	})
	waitHit(t, c, "a/b/c")
	c.start(context.Background(), "a", gatedRun(g, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", entry("b"))
		complete("a")
		emit("a/b/c", entry("c-again"))
		complete("a/b/c")
		emit("a/b/d", []model.FileEntry{{Name: "d1"}, {Name: "d2"}})
		complete("a/b/d")
		return nil
	}))
	select {
	case err := <-innerStopped:
		if err == nil {
			t.Error("the run of a/b ended with no error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run of a/b was not stopped")
	}
	outer := serveWith(c, context.Background(), "a")
	stillWaits(t, outer)
	g.open()
	if o := served(t, outer); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"b"}) {
		t.Errorf("a: %q, err %v, live %v; want the new run's [b]", namesOfEntries(o.entries), o.err, o.lived)
	}
	waitDone(t, c)
	for dir, want := range map[string][]string{"a/b/c": {"c1"}, "a/b/d": {"d1", "d2"}} {
		if got, hit, _, _ := c.lookup(dir); !hit || !slices.Equal(namesOfEntries(got), want) {
			t.Errorf("%s: %q, hit %v; want %q", dir, namesOfEntries(got), hit, want)
		}
	}
}

// W-H4 stopped any run in flight for a fresh one: a rescan of one folder
// while the scan's whole listing went sent the rest of the scan to list
// each folder by itself. Now the run goes on and leaves the fresh run's
// folder to it: what it sent or sends there is dropped, the rest it feeds
// as before.
func TestListCacheFreshRunInsideARunTakesItsFolder(t *testing.T) {
	c := newListCache()
	gRoot, gFresh := newRunGate(t), newRunGate(t)
	rootStopped := make(chan error, 1)
	c.start(context.Background(), "", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", entry("old"))
		select {
		case <-gRoot.ch:
		case <-ctx.Done():
			rootStopped <- ctx.Err()
			return ctx.Err()
		}
		emit("a", entry("late"))
		complete("a")
		emit("b", entry("b1"))
		complete("b")
		return nil
	})
	c.start(model.WithFreshListing(context.Background()), "a", gatedRun(gFresh, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", entry("new"))
		complete("a")
		return nil
	}))
	inB := serveWith(c, context.Background(), "b")
	stillWaits(t, inB)
	gRoot.open()
	if o := served(t, inB); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"b1"}) {
		t.Errorf("b: %q, err %v, live %v; want the root run's [b1]", namesOfEntries(o.entries), o.err, o.lived)
	}
	select {
	case err := <-rootStopped:
		t.Errorf("the root run was stopped: %v", err)
	default:
	}
	inA := serveWith(c, context.Background(), "a")
	stillWaits(t, inA)
	gFresh.open()
	if o := served(t, inA); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"new"}) {
		t.Errorf("a: %q, err %v, live %v; want the fresh run's [new]", namesOfEntries(o.entries), o.err, o.lived)
	}
}

// A run that failed on the answer keeps nothing of its own; the folders of
// a run beside it stay.
func TestListCacheFailedRunDropsOnlyItsFolders(t *testing.T) {
	c := newListCache()
	ga, gb := newRunGate(t), newRunGate(t)
	c.start(context.Background(), "a", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", entry("a1"))
		complete("a")
		select {
		case <-ga.ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	waitHit(t, c, "a")
	failed := make(chan struct{})
	c.start(context.Background(), "b", gatedRun(gb, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("b", entry("b1"))
		complete("b")
		close(failed)
		return errors.New("the server sent nonsense")
	}))
	gb.open()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the run of b did not run")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, hit, _, _ := c.lookup("b"); !hit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("b, of the run that failed, is still served 5s later")
		}
		time.Sleep(time.Millisecond)
	}
	if got, hit, _, _ := c.lookup("a"); !hit || !slices.Equal(namesOfEntries(got), []string{"a1"}) {
		t.Errorf("a, of the run beside it: %q, hit %v; want [a1]", namesOfEntries(got), hit)
	}
	ga.open()
	waitDone(t, c)
	if _, hit, _, _ := c.lookup("a"); !hit {
		t.Error("a is not served after its run ended")
	}
}

// resume runs each run the old connection cut off again, not the latest
// alone.
func TestListCacheResumeRunsEachRun(t *testing.T) {
	c := newListCache()
	hang := func(ctx context.Context, _ string, _ func(string, []model.FileEntry), _ func(string)) error {
		<-ctx.Done()
		return ctx.Err()
	}
	c.start(context.Background(), "a", hang)
	c.start(context.Background(), "b", hang)
	var mu sync.Mutex
	var scopes []string
	c.resume("test", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
		mu.Lock()
		scopes = append(scopes, scope)
		mu.Unlock()
		emit(scope, entry("again"))
		complete(scope)
		return nil
	})
	waitDone(t, c)
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(scopes)
	if !slices.Equal(scopes, []string{"a", "b"}) {
		t.Errorf("resume ran %q again; want [a b]", scopes)
	}
	for _, dir := range []string{"a", "b"} {
		if _, hit, _, _ := c.lookup(dir); !hit {
			t.Errorf("%s is not served after its run ran again", dir)
		}
	}
}

// A write while two runs go makes each run's view of what it wrote old.
func TestListCacheWriteMarksEachRun(t *testing.T) {
	c := newListCache()
	ga, gb := newRunGate(t), newRunGate(t)
	c.start(context.Background(), "a", gatedRun(ga, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a/x", entry("a1"))
		complete("a/x")
		return nil
	}))
	c.start(context.Background(), "b", gatedRun(gb, func(emit func(string, []model.FileEntry), complete func(string)) error {
		for _, d := range []string{"b/x", "b/y", "b/y/z"} {
			emit(d, entry("old"))
			complete(d)
		}
		return nil
	}))
	c.invalidate("b/x")
	c.invalidateTree("b/y")
	ga.open()
	gb.open()
	waitDone(t, c)
	if _, hit, _, _ := c.lookup("a/x"); !hit {
		t.Error("a/x, which nothing wrote, is not served")
	}
	for _, dir := range []string{"b/x", "b/y", "b/y/z"} {
		if got, hit, _, _ := c.lookup(dir); hit {
			t.Errorf("%s, written while the run of b went, is served as %q", dir, namesOfEntries(got))
		}
	}
}

// A wait for a run ends when the run's ctx ends, though its listing does
// not stop: the folder is then listed live.
func TestListCacheWaitEndsWithTheRunsContext(t *testing.T) {
	c := newListCache()
	runCtx, stop := context.WithCancel(context.Background())
	release := make(chan struct{})
	defer close(release)
	c.start(runCtx, "a", func(context.Context, string, func(string, []model.FileEntry), func(string)) error {
		<-release // a hung listing: it does not see its ctx end
		return nil
	})
	waiter := serveWith(c, context.Background(), "a/x")
	stillWaits(t, waiter)
	stop()
	if o := served(t, waiter); o.err != nil || !o.lived {
		t.Errorf("a/x after the run's ctx ended: err %v, live %v; want a live listing", o.err, o.lived)
	}
}

// A run that runs again on a new connection still leaves to a fresh run
// the folder that run took from it.
func TestListCacheResumeKeepsWhatAFreshRunTook(t *testing.T) {
	c := newListCache()
	hang := func(ctx context.Context, _ string, _ func(string, []model.FileEntry), _ func(string)) error {
		<-ctx.Done()
		return ctx.Err()
	}
	c.start(context.Background(), "", hang)
	c.start(model.WithFreshListing(context.Background()), "a", hang)
	c.resume("test", func(_ context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
		if scope == "" {
			emit("a", entry("from-root"))
			complete("a")
			emit("b", entry("b1"))
			complete("b")
			return nil
		}
		emit("a", entry("from-a"))
		complete("a")
		return nil
	})
	waitDone(t, c)
	for dir, want := range map[string][]string{"a": {"from-a"}, "b": {"b1"}} {
		if got, hit, _, _ := c.lookup(dir); !hit || !slices.Equal(namesOfEntries(got), want) {
			t.Errorf("%s: %q, hit %v; want %q", dir, namesOfEntries(got), hit, want)
		}
	}
}

// A preload of a folder a write moved while a run went was ignored, and
// every folder under it, which the run then leaves, was listed by itself.
// Now a run of its own lists it.
func TestListCacheFolderARunLeftGetsARunOfItsOwn(t *testing.T) {
	c := newListCache()
	gRoot, gA := newRunGate(t), newRunGate(t)
	c.start(context.Background(), "", gatedRun(gRoot, func(func(string, []model.FileEntry), func(string)) error { return nil }))
	c.invalidateTree("a")
	c.start(context.Background(), "a", gatedRun(gA, func(emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a/x", entry("x1"))
		complete("a/x")
		return nil
	}))
	waiter := serveWith(c, context.Background(), "a/x")
	stillWaits(t, waiter)
	gA.open()
	if o := served(t, waiter); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"x1"}) {
		t.Errorf("a/x: %q, err %v, live %v; want the run of a's [x1]", namesOfEntries(o.entries), o.err, o.lived)
	}
}

// A run that another took over completes nothing: a folder it completes
// late may be one the new run has sent part of.
func TestListCacheReplacedRunCompletesNothing(t *testing.T) {
	c := newListCache()
	release, oldDone := make(chan struct{}), make(chan struct{})
	c.start(context.Background(), "", func(_ context.Context, _ string, _ func(string, []model.FileEntry), complete func(string)) error {
		<-release // a listing that does not see its ctx end
		complete("d")
		close(oldDone)
		return nil
	})
	gNew := newRunGate(t)
	c.start(model.WithFreshListing(context.Background()), "", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("d", entry("n1"))
		select {
		case <-gNew.ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		emit("d", entry("n2"))
		complete("d")
		return nil
	})
	waiter := serveWith(c, context.Background(), "d")
	stillWaits(t, waiter)
	close(release)
	<-oldDone
	stillWaits(t, waiter)
	gNew.open()
	if o := served(t, waiter); o.err != nil || o.lived || !slices.Equal(namesOfEntries(o.entries), []string{"n1", "n2"}) {
		t.Errorf("d: %q, err %v, live %v; want the new run's [n1 n2]", namesOfEntries(o.entries), o.err, o.lived)
	}
}

// fill keeps nothing while a run goes: the run may send the folder after.
func TestListCacheFillWaitsForTheRuns(t *testing.T) {
	c := newListCache()
	g := newRunGate(t)
	c.start(context.Background(), "", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("d", entry("r1"))
		select {
		case <-g.ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		emit("d", entry("r2"))
		complete("d")
		return nil
	})
	c.fill("d", entry("f1"))
	if got, hit, _, _ := c.lookup("d"); hit {
		t.Errorf("d is served as %q while the run goes", namesOfEntries(got))
	}
	g.open()
	waitDone(t, c)
	if got, hit, _, _ := c.lookup("d"); !hit || !slices.Equal(namesOfEntries(got), []string{"r1", "r2"}) {
		t.Errorf("d: %q, hit %v; want the run's [r1 r2]", namesOfEntries(got), hit)
	}
}
