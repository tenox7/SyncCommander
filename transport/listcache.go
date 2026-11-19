package transport

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"sync"

	"sc/model"
)

// listCache holds per-directory entries fed by recursive listings running
// in the background, each of one scope: a folder, with all under it. List
// serves a directory once it is complete: marked so by the emitter mid-run,
// or when the run that lists it ended whole. Until then the run in flight
// that lists it is awaited rather than bypassed with a live per-dir call.
// A run's ctx is the scan context the wait follows, so a per-call stall
// timeout on List does not abandon a slow but healthy preload.
//
// The runs are kept by scope (R9, W-M7). A preload of a folder that a run
// in flight lists joins that run. One of another folder runs beside it: the
// cache held one run, a second scope was ignored while it went, and each
// of its folders was listed by itself. One of a folder that holds a run's
// scope takes that run over and keeps the folders it listed in full. Two
// runs never feed one folder: each feeds the folders it owns
// (cacheRun.owns).
//
// A write while a run is going makes the run's view of the written folders
// old: the run may have sent part of such a folder before the write and
// goes on sending the rest, which refilled would be served as the whole
// folder (W-H2). So the cache keeps nothing the run sends for those
// folders, and they are listed live, until a run that starts after the
// write lists them.
type listCache struct {
	mu         sync.Mutex
	cond       *sync.Cond
	entries    map[string][]model.FileEntry
	complete   map[string]bool
	runs       int         // runs started
	held       []*cacheRun // the runs going, and those a cut ended, which resume runs again
	retryProto string      // set: a failed run is tried again (retryingRuns)
	index      folderIndex // the folders entries and complete hold, by the folder each is in (W-M8)
}

// cacheRun is one recursive listing that feeds the cache.
type cacheRun struct {
	scope     string             // the folder it lists, with all under it
	ctx       context.Context    // its caller's: a wait for the run ends with it
	stop      context.CancelFunc // stops its listing
	done      bool               // it ended
	gone      bool               // taken over or stopped: what it still sends is dropped
	cutShort  bool               // it ended cut off (runRetryable): resume runs it again
	stale     map[string]bool    // folders a write changed while it went
	staleTree map[string]bool    // folders a write removed or moved then, or a fresh run inside it took; all under them
	kept      map[string]bool    // folders an earlier try, or a run it took over, listed in full
}

// listCacheCarrier is a backend whose list cache goes on with the backend
// of its next connection: the ssh family, whose backend a lost connection
// drops (lazyBackend.markBrokenIf). The cache and the recursive listing it
// ran went with the backend, and the rest of a scan listed each folder by
// itself (S18). The opener's factory keeps the cache (open.go).
type listCacheCarrier interface {
	heldListCache() *listCache
	takeListCache(*listCache) // before the backend serves a call
}

func newListCache() *listCache {
	c := &listCache{}
	c.cond = sync.NewCond(&c.mu)
	c.clearLocked()
	return c
}

// clearLocked drops all the cache holds.
func (c *listCache) clearLocked() {
	c.entries = make(map[string][]model.FileEntry)
	c.complete = make(map[string]bool)
	c.index = make(folderIndex)
}

// retryingRuns has the cache try a failed run again, with backoff, for a
// backend whose every run opens a connection of its own, so another try
// can work. A run that failed was over: one reset during a long listing,
// and the rest of the scan listed each folder by itself (W-M7). proto
// names the backend in the log.
func (c *listCache) retryingRuns(proto string) *listCache {
	c.retryProto = proto
	return c
}

// cutOff reports a run that ended because its connection did, or went
// silent: what it sent until then is what it would have sent. A run that
// failed on what the server sent is not trusted for any of it.
func cutOff(err error) bool {
	return isConnLost(err) || errors.Is(err, ErrStall) || errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded)
}

// runRetryable reports whether another try of a failed run can do better:
// one its connection cut off. One the server answered with an error, or
// that could not connect, gets the same at once; the connect has its own
// backoff (lazyBackend).
func runRetryable(err error) bool {
	return cutOff(err) && !isPermanentError(err)
}

// keepAfterLocked keeps what r, a run that failed with err, sent for the
// folders it completed, when err cut it off, and drops the rest of its
// folders: a completed folder is whole, as it was when the run released it
// to a waiter, and the next try sends it again for nothing (W-M7). The
// folders of other runs stay. It returns how many folders it kept.
func (c *listCache) keepAfterLocked(r *cacheRun, err error) int {
	keep := cutOff(err)
	r.kept = make(map[string]bool)
	for dir := range c.entries {
		switch {
		case !r.owns(dir):
		case keep && c.complete[dir]:
			r.kept[dir] = true
		default:
			c.drop(dir)
		}
	}
	return len(r.kept)
}

// going reports whether r still sends.
func (r *cacheRun) going() bool { return !r.done && !r.gone }

// covers reports whether dir is r's scope or under it. A folder outside
// the scope is never complete, and a List of one waited for the whole run
// (W-M2).
func (r *cacheRun) covers(dir string) bool {
	scope, d := cleanRel(r.scope), cleanRel(dir)
	return scope == "" || d == scope || strings.HasPrefix(d, scope+"/")
}

// owns reports whether the folder dir is r's to feed: it is in r's scope,
// and neither a write while r went nor a fresh run inside r took it.
func (r *cacheRun) owns(dir string) bool { return r.covers(dir) && !r.staleFor(dir) }

// staleFor reports whether a write while r went changed dir, or removed or
// moved a folder it is in, or a fresh run inside r took a folder it is in.
func (r *cacheRun) staleFor(dir string) bool {
	if r.stale[dir] {
		return true
	}
	if len(r.staleTree) == 0 {
		return false
	}
	for p := dir; ; p = parentDir(p) {
		if r.staleTree[p] {
			return true
		}
		if p == "" {
			return false
		}
	}
}

// leaveTree has r leave the folder p and all under it.
func (r *cacheRun) leaveTree(p string) {
	if r.staleTree == nil {
		r.staleTree = make(map[string]bool)
	}
	r.staleTree[p] = true
}

// runForLocked returns the run in flight that lists dir: of those whose
// scope holds it, the innermost. nil: none does.
func (c *listCache) runForLocked(dir string) *cacheRun {
	var in *cacheRun
	for _, r := range c.held {
		if r.going() && r.covers(dir) && (in == nil || in.covers(r.scope)) {
			in = r
		}
	}
	return in
}

// fedLocked reports whether a run in flight feeds the folder dir.
func (c *listCache) fedLocked(dir string) bool {
	r := c.runForLocked(dir)
	return r != nil && r.owns(dir)
}

// goingLocked reports whether a run is going.
func (c *listCache) goingLocked() bool {
	return slices.ContainsFunc(c.held, (*cacheRun).going)
}

// recursiveLister streams a subtree: emit appends a batch of children to
// parent, complete declares that every child of dir has been emitted.
// Emitters whose order cannot prove that never call complete. A batch is
// the cache's once emitted: the lister keeps no hold of it.
type recursiveLister func(ctx context.Context, scope string, emit func(parent string, entries []model.FileEntry), complete func(dir string)) error

// unreadDirs is the result of a recursive listing that ran to the end but
// could not list some folders in full. The run never released them; the
// cache drops them and keeps the rest, so List lists just these live, where
// the error shows.
type unreadDirs struct {
	dirs []string
	why  string
}

func (e *unreadDirs) Error() string { return e.why }

// start runs a recursive listing of scope in the background, feeding the
// cache through emit, unless a run in flight lists scope already: the
// caller's Lists then wait for that one. A run in flight inside scope is
// stopped, and the folders it listed in full are kept for the new run. A
// run that holds scope goes on and leaves scope to the new run, and runs of
// other folders go on as they were. What the runs that ended left is
// dropped: the new run is a newer listing.
//
// ctx asking for a fresh listing (model.WithFreshListing: a rescan, W-H4)
// starts a run whatever runs: a run in flight may have listed a folder of
// scope before the change the rescan is to show, so the new run keeps none
// of what was listed under scope, and what a run it stopped still sends is
// dropped.
//
// When run fails the partial contents are dropped, so List falls back to
// live per-dir calls instead of trusting a truncated tree. A run its
// connection cut off keeps the folders it completed (keepAfterLocked) and,
// with retryingRuns, is tried again, or runs again on the backend of the
// next connection (resume). An *unreadDirs failure drops only the folders
// it names.
func (c *listCache) start(ctx context.Context, scope string, run recursiveLister) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fresh := model.WantsFreshListing(ctx)
	if r := c.runForLocked(scope); r != nil && !fresh && !r.staleFor(scope) {
		return
	}
	nr := &cacheRun{scope: scope, kept: make(map[string]bool)}
	for _, r := range c.held {
		switch {
		case !r.going():
		case nr.covers(r.scope):
			r.stop()
			if !fresh {
				c.keepAfterLocked(r, model.ErrConnLost)
				maps.Copy(nr.kept, r.kept)
			}
			r.gone = true
		case r.covers(scope):
			r.leaveTree(scope)
		}
	}
	c.held = slices.DeleteFunc(c.held, func(r *cacheRun) bool { return !r.going() })
	if len(c.held) == 0 && len(nr.kept) == 0 {
		c.clearLocked()
	} else {
		for dir := range c.entries {
			if !nr.kept[dir] && !c.fedLocked(dir) {
				c.drop(dir)
			}
		}
	}
	c.launchLocked(ctx, nr, run)
}

// resume runs again with run, the lister of the backend of a new
// connection, each run the old connection cut off, or that still runs
// there: the folders it listed in full are kept, as a retry keeps them,
// and the new run lists the rest. A run that ended whole, or on what the
// server answered, is left as it is, and so is one whose ctx ended (S18).
// proto names the backend in the log.
func (c *listCache) resume(proto string, run recursiveLister) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range slices.Clone(c.held) {
		if r.gone || r.ctx.Err() != nil || r.done && !r.cutShort {
			continue
		}
		r.stop()
		kept := c.keepAfterLocked(r, model.ErrConnLost)
		r.gone = true
		Log.Add(proto, DirRetry, fmt.Sprintf("the recursive listing of %s runs again on the new connection, keeping %d folders it listed in full", path.Join("/", r.scope), kept))
		c.launchLocked(r.ctx, &cacheRun{scope: r.scope, kept: r.kept, stale: r.stale, staleTree: r.staleTree}, run)
	}
	c.held = slices.DeleteFunc(c.held, func(r *cacheRun) bool { return r.gone })
}

// launchLocked starts r, a run of its scope under ctx, in the background.
// mu held.
func (c *listCache) launchLocked(ctx context.Context, r *cacheRun, run recursiveLister) {
	runCtx, stop := context.WithCancel(ctx)
	c.runs++
	r.ctx, r.stop = ctx, stop
	c.held = append(c.held, r)
	c.cond.Broadcast() // a waiter whose run r took over, or took the folder of, waits for r
	unwake := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})

	emit := func(parent string, entries []model.FileEntry) {
		c.mu.Lock()
		if !r.gone {
			c.emitLocked(r, parent, entries)
		}
		c.mu.Unlock()
	}
	complete := func(dir string) {
		c.mu.Lock()
		if !r.gone {
			c.markCompleteLocked(r, dir)
		}
		c.mu.Unlock()
	}
	go func() {
		defer stop()
		defer unwake()
		err := run(runCtx, r.scope, emit, complete)
		for try := 1; c.retryProto != "" && try <= MaxRetries() && runRetryable(err) && runCtx.Err() == nil; try++ {
			c.mu.Lock()
			if r.gone {
				c.mu.Unlock()
				return
			}
			kept := c.keepAfterLocked(r, err)
			c.mu.Unlock()
			d := backoffDelay(try - 1)
			Log.Add(c.retryProto, DirRetry, fmt.Sprintf("the recursive listing of %s: %v (attempt %d/%d in %v, keeping %d folders it listed in full)", path.Join("/", r.scope), err, try+1, MaxRetries()+1, d, kept))
			if sleepCtx(runCtx, d) != nil {
				break
			}
			err = run(runCtx, r.scope, emit, complete)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if r.gone {
			// The run that took it over feeds its folders.
			return
		}
		var unread *unreadDirs
		switch {
		case errors.As(err, &unread):
			for _, d := range unread.dirs {
				c.drop(d)
			}
			c.endWholeLocked(r)
		case err != nil:
			c.keepAfterLocked(r, err)
			r.cutShort = runRetryable(err)
		default:
			c.endWholeLocked(r)
		}
		r.done = true
		if !r.cutShort {
			c.held = slices.DeleteFunc(c.held, func(h *cacheRun) bool { return h == r })
		}
		c.cond.Broadcast()
	}()
}

// endWholeLocked marks complete each folder r fed: it ran to its end, so
// each is whole.
func (c *listCache) endWholeLocked(r *cacheRun) {
	for dir := range c.entries {
		if r.owns(dir) {
			c.complete[dir] = true
		}
	}
}

// serve answers List from the cache: a hit returns at once, an in-flight
// preload is awaited, anything else goes live. A fresh listing
// (model.WithFreshListing) always goes live.
func (c *listCache) serve(ctx context.Context, relDir string, live func(context.Context, string) ([]model.FileEntry, error)) ([]model.FileEntry, error) {
	if model.WantsFreshListing(ctx) {
		return live(ctx, relDir)
	}
	entries, hit, listed := c.lookupRun(relDir)
	if hit {
		return entries, nil
	}
	if !listed {
		return live(ctx, relDir)
	}
	// The wait is not the listing's time: the listing's timeout
	// (model.WithListTimeout) does not end it, and a live listing after it
	// gets that whole timeout. A stop of the caller ends it (W-M2).
	wait, d, stop := model.ListWait(ctx)
	defer stop()
	e, ok, err := c.await(wait, relDir)
	switch {
	case ok:
		return e, nil
	case err != nil:
		return nil, err
	case d > 0:
		lctx, cancel := context.WithTimeout(wait, d)
		defer cancel()
		return live(lctx, relDir)
	}
	return live(ctx, relDir)
}

// emit appends entries for parent without waking awaiters: a parent may
// arrive in several batches, so a single emit can be a partial view. The
// first batch of a parent is kept as it came: it was copied, and a folder
// of 1M files was held twice while it came (W-M8). emit and markComplete
// feed the cache with no run.
func (c *listCache) emit(parent string, entries []model.FileEntry) {
	c.mu.Lock()
	c.emitLocked(nil, parent, entries)
	c.mu.Unlock()
}

// emitLocked appends what r sent for parent, a folder r owns. nil r: no
// run sent it.
func (c *listCache) emitLocked(r *cacheRun, parent string, entries []model.FileEntry) {
	if r != nil && (!r.owns(parent) || r.kept[parent]) {
		return // a folder a write or another run took, or one listed in full already: no second copy
	}
	if cur := c.entries[parent]; len(cur) == 0 {
		c.entries[parent] = entries
	} else {
		c.entries[parent] = append(cur, entries...)
	}
	c.index.add(parent)
}

// fill keeps the whole listing of dir that a live call got while it listed
// something else (restic lists a whole type at once), so a later List of
// dir is a hit until a write or a fresh listing drops it. While a recursive
// listing runs the cache is the runs', and nothing is filled.
func (c *listCache) fill(dir string, entries []model.FileEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.goingLocked() {
		return
	}
	c.entries[dir] = entries
	c.markCompleteLocked(nil, dir)
}

// markComplete releases awaiters of dir and registers it even when nothing
// was emitted, so an empty dir is a hit rather than a live call.
func (c *listCache) markComplete(dir string) {
	c.mu.Lock()
	c.markCompleteLocked(nil, dir)
	c.mu.Unlock()
}

// markCompleteLocked marks dir complete for r, when r owns it. nil r: no
// run sent it.
func (c *listCache) markCompleteLocked(r *cacheRun, dir string) {
	if r == nil || r.owns(dir) {
		c.complete[dir] = true
		if _, ok := c.entries[dir]; !ok {
			c.entries[dir] = nil
		}
	}
	c.index.add(dir)
	c.cond.Broadcast()
}

// lookup is relDir's entries and whether they are complete, whether a run
// ever started, and whether none goes now.
func (c *listCache) lookup(relDir string) (entries []model.FileEntry, hit, active, done bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, hit = c.entries[relDir]
	return entries, hit && c.complete[relDir], c.runs > 0, !c.goingLocked()
}

// lookupRun is relDir's entries, whether they are complete, and whether a
// run in flight lists relDir.
func (c *listCache) lookupRun(relDir string) (entries []model.FileEntry, hit, listed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, hit = c.entries[relDir]
	return entries, hit && c.complete[relDir], c.runForLocked(relDir) != nil
}

// await blocks until relDir is complete, or no run in flight lists it: the
// run that did ended, a write made its view of relDir old, or its ctx
// ended; or until ctx ends: err is then ctx's cause. It ignored ctx: a copy
// that was stopped waited for the scan's whole listing (W-M2).
func (c *listCache) await(ctx context.Context, relDir string) ([]model.FileEntry, bool, error) {
	wake := func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}
	defer context.AfterFunc(ctx, wake)()
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.complete[relDir] {
			return c.entries[relDir], true, nil
		}
		if ctx.Err() != nil {
			return nil, false, context.Cause(ctx)
		}
		if r := c.runForLocked(relDir); r == nil || r.staleFor(relDir) || r.ctx.Err() != nil {
			return nil, false, nil
		}
		c.cond.Wait()
	}
}

// The invalidators are nil-safe: backends without a preload leave the cache
// nil and still call them after every write.

func (c *listCache) drop(dir string) {
	delete(c.entries, dir)
	delete(c.complete, dir)
}

// changedLocked drops dir after a write to it, and marks it for each run
// going: see listCache.
func (c *listCache) changedLocked(dir string) {
	c.drop(dir)
	for _, r := range c.held {
		if r.going() {
			if r.stale == nil {
				r.stale = make(map[string]bool)
			}
			r.stale[dir] = true
		}
	}
	c.cond.Broadcast()
}

func (c *listCache) invalidate(relDir string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.changedLocked(relDir)
	c.mu.Unlock()
}

// invalidateAncestors clears every ancestor of relPath up to and including
// the base: a write may have created intermediate dirs, so each shallower
// listing may have gained a child.
func (c *listCache) invalidateAncestors(relPath string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := parentDir(relPath); ; p = parentDir(p) {
		c.changedLocked(p)
		if p == "" {
			return
		}
	}
}

// forgetRenamed drops the moved subtree and both parents' listings.
func (c *listCache) forgetRenamed(oldRel, newRel string) {
	c.invalidateTree(oldRel)
	c.invalidate(parentDir(oldRel))
	c.invalidate(parentDir(newRel))
}

// forgetRemoved drops the removed subtree and its parent's listing.
func (c *listCache) forgetRemoved(relPath string) {
	c.invalidateTree(relPath)
	c.invalidate(parentDir(relPath))
}

// invalidateTree drops the listing of prefix and those of the folders
// under it, walking those alone: it looked at every folder held, for each
// file deleted or moved too (W-M8).
func (c *listCache) invalidateTree(prefix string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index.cut(prefix, c.drop)
	for _, r := range c.held {
		if r.going() {
			r.leaveTree(prefix)
		}
	}
	c.cond.Broadcast()
}

// parentDir is the folder of relPath, "" for the base. A path that starts
// with "/" ends at "/", which is its own folder: taken for one more level,
// invalidateAncestors went round it forever with the cache locked.
func parentDir(relPath string) string {
	p := path.Dir(relPath)
	if p == "." || p == "/" {
		return ""
	}
	return p
}

// emitGrouper batches consecutive entries sharing a parent and emits each
// batch on parent change. With complete set it also tracks the open
// directories of a depth-first stream: the first entry outside a subtree
// proves that subtree fully streamed, so the dir is flushed and completed
// then. A dir opens with its own entry, or, with lateChildren, with its first
// child: rsync lists a directory's children as one run after the parent's
// run, so the entry alone proves nothing. Directories never completed by the
// stream (empty ones, or all of them without complete) are registered at
// finish, so an empty leaf is a cache hit rather than a live list. While hold
// reports true, a dir the stream leaves is not completed then but at finish:
// the run has reported a problem, and only its end tells which listings are
// whole. With childlessUnknown a dir no child arrived for is never
// completed or registered, so List lists it live: the stream cannot tell an
// empty dir from one the server did not descend into.
type emitGrouper struct {
	emit             func(string, []model.FileEntry)
	complete         func(string)
	hold             func() bool
	skip             map[string]bool // never completed, see finishExcept
	lateChildren     bool
	childlessUnknown bool
	withChildren     map[string]struct{}
	current          string
	have             bool
	batch            []model.FileEntry
	dirs             map[string]struct{}
	open             []string
}

func (g *emitGrouper) add(e model.FileEntry) {
	g.closeOutside(e.RelPath, g.hold == nil || !g.hold())
	parent := parentDir(e.RelPath)
	if g.childlessUnknown {
		if g.withChildren == nil {
			g.withChildren = make(map[string]struct{})
		}
		g.withChildren[parent] = struct{}{}
	}
	if g.lateChildren && parent != "" && (len(g.open) == 0 || g.open[len(g.open)-1] != parent) {
		g.push(parent)
	}
	if !g.have || parent != g.current {
		g.flush()
		g.current, g.have = parent, true
	}
	g.batch = append(g.batch, e)
	if !e.IsDir {
		return
	}
	g.addDir(e.RelPath)
	if !g.lateChildren {
		g.push(e.RelPath)
	}
}

func (g *emitGrouper) push(dir string) {
	if g.complete != nil {
		g.open = append(g.open, dir)
	}
}

// closeOutside closes every open dir that rel is not under; "" closes all.
// A closed dir is completed now if release is set, else at finish.
func (g *emitGrouper) closeOutside(rel string, release bool) {
	for len(g.open) > 0 {
		d := g.open[len(g.open)-1]
		if strings.HasPrefix(rel, d+"/") {
			return
		}
		g.flush()
		g.open = g.open[:len(g.open)-1]
		if !release || g.skip[d] {
			g.addDir(d)
			continue
		}
		g.done(d)
		delete(g.dirs, d)
	}
}

// done completes d, unless the stream cannot vouch for it (childlessUnknown).
func (g *emitGrouper) done(d string) {
	if g.unknown(d) {
		return
	}
	g.complete(d)
}

func (g *emitGrouper) unknown(d string) bool {
	if !g.childlessUnknown {
		return false
	}
	_, ok := g.withChildren[d]
	return !ok
}

func (g *emitGrouper) addDir(d string) {
	if g.dirs == nil {
		g.dirs = make(map[string]struct{})
	}
	g.dirs[d] = struct{}{}
}

func (g *emitGrouper) flush() {
	if !g.have {
		return
	}
	g.emit(g.current, g.batch)
	g.batch, g.have = nil, false
}

func (g *emitGrouper) finish() { g.finishExcept(nil) }

// finishExcept is finish for a run that could not list the dirs in skip in
// full: they are never completed, and the caller has the cache drop them.
func (g *emitGrouper) finishExcept(skip map[string]bool) {
	g.skip = skip
	g.flush()
	g.closeOutside("", true)
	for d := range g.dirs {
		if skip[d] || g.unknown(d) {
			continue
		}
		if g.complete != nil {
			g.complete(d)
			continue
		}
		g.emit(d, nil)
	}
}
