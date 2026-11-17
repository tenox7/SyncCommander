package transport

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"

	"sc/model"
)

// listCache holds per-directory entries fed by one recursive listing running
// in the background. List serves a directory once it is complete: marked so
// by the emitter mid-run, or implicitly when the whole run has finished.
// Until then an in-flight preload is awaited rather than bypassed with a
// live per-dir call. preloadCtx is the scan context the wait follows, so a
// per-call stall timeout on List does not abandon a slow but healthy preload.
//
// A write while a run is going makes the run's view of the written folders
// old: the run may have sent part of such a folder before the write and
// goes on sending the rest, which refilled would be served as the whole
// folder (W-H2). So the cache keeps nothing the run sends for those
// folders, and they are listed live, until the next run starts.
type listCache struct {
	mu         sync.Mutex
	cond       *sync.Cond
	entries    map[string][]model.FileEntry
	complete   map[string]bool
	active     bool
	done       bool
	preloadCtx context.Context
	runs       int                // runs started; only the latest one feeds the cache
	stopRun    context.CancelFunc // stops the latest run's listing
	stale      map[string]bool    // folders a write changed while the run was going
	staleTree  map[string]bool    // folders a write removed or moved then, and all under them
}

func newListCache() *listCache {
	c := &listCache{}
	c.cond = sync.NewCond(&c.mu)
	c.reset()
	return c
}

func (c *listCache) reset() {
	c.entries = make(map[string][]model.FileEntry)
	c.complete = make(map[string]bool)
	c.stale, c.staleTree = nil, nil
}

// running reports whether a run is still sending.
func (c *listCache) running() bool { return c.active && !c.done }

// staleLocked reports whether a write during the run changed dir, or removed
// or moved a folder it is in.
func (c *listCache) staleLocked(dir string) bool {
	if c.stale[dir] {
		return true
	}
	if len(c.staleTree) == 0 {
		return false
	}
	for p := dir; ; p = parentDir(p) {
		if c.staleTree[p] {
			return true
		}
		if p == "" {
			return false
		}
	}
}

// recursiveLister streams a subtree: emit appends a batch of children to
// parent, complete declares that every child of dir has been emitted.
// Emitters whose order cannot prove that never call complete.
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

// start runs one recursive listing in the background, feeding the cache
// through emit. A preload already in flight is left alone, unless ctx asks
// for a fresh listing (model.WithFreshListing: a rescan, W-H4): then the
// cache starts empty, the old run is stopped, and what it still sends is
// dropped, since it may have listed a folder before the change the rescan
// is to show. When run fails the partial contents are dropped, so List
// falls back to live per-dir calls instead of trusting a truncated tree; an
// *unreadDirs failure drops only the folders it names.
func (c *listCache) start(ctx context.Context, scope string, run recursiveLister) {
	c.mu.Lock()
	if c.active && !c.done && !model.WantsFreshListing(ctx) {
		c.mu.Unlock()
		return
	}
	if c.stopRun != nil {
		c.stopRun()
	}
	runCtx, stop := context.WithCancel(ctx)
	c.runs++
	id := c.runs
	c.stopRun = stop
	c.reset()
	c.active, c.done = true, false
	c.preloadCtx = ctx
	c.mu.Unlock()

	emit := func(parent string, entries []model.FileEntry) {
		c.mu.Lock()
		if c.runs == id {
			c.emitLocked(parent, entries)
		}
		c.mu.Unlock()
	}
	complete := func(dir string) {
		c.mu.Lock()
		if c.runs == id {
			c.markCompleteLocked(dir)
		}
		c.mu.Unlock()
	}
	go func() {
		defer stop()
		err := run(runCtx, scope, emit, complete)
		c.mu.Lock()
		if c.runs != id {
			// A fresh run replaced this one: the cache is that run's.
			c.mu.Unlock()
			return
		}
		var unread *unreadDirs
		switch {
		case errors.As(err, &unread):
			for _, d := range unread.dirs {
				c.drop(d)
			}
		case err != nil:
			c.reset()
		}
		c.done = true
		c.cond.Broadcast()
		c.mu.Unlock()
	}()
}

// serve answers List from the cache: a hit returns at once, an in-flight
// preload is awaited, anything else goes live. A fresh listing
// (model.WithFreshListing) always goes live.
func (c *listCache) serve(ctx context.Context, relDir string, live func(context.Context, string) ([]model.FileEntry, error)) ([]model.FileEntry, error) {
	if model.WantsFreshListing(ctx) {
		return live(ctx, relDir)
	}
	entries, hit, active, done := c.lookup(relDir)
	if hit {
		return entries, nil
	}
	if active && !done {
		if e, ok := c.await(relDir); ok {
			return e, nil
		}
	}
	return live(ctx, relDir)
}

// emit appends entries for parent without waking awaiters: a parent may
// arrive in several batches, so a single emit can be a partial view.
func (c *listCache) emit(parent string, entries []model.FileEntry) {
	c.mu.Lock()
	c.emitLocked(parent, entries)
	c.mu.Unlock()
}

func (c *listCache) emitLocked(parent string, entries []model.FileEntry) {
	if c.staleLocked(parent) {
		return
	}
	c.entries[parent] = append(c.entries[parent], entries...)
}

// fill keeps the whole listing of dir that a live call got while it listed
// something else (restic lists a whole type at once), so a later List of
// dir is a hit until a write or a fresh listing drops it. While a recursive
// listing runs the cache is that run's, and nothing is filled.
func (c *listCache) fill(dir string, entries []model.FileEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active && !c.done {
		return
	}
	c.entries[dir] = entries
	c.markCompleteLocked(dir)
}

// markComplete releases awaiters of dir and registers it even when nothing
// was emitted, so an empty dir is a hit rather than a live call.
func (c *listCache) markComplete(dir string) {
	c.mu.Lock()
	c.markCompleteLocked(dir)
	c.mu.Unlock()
}

func (c *listCache) markCompleteLocked(dir string) {
	if !c.staleLocked(dir) {
		c.complete[dir] = true
		if _, ok := c.entries[dir]; !ok {
			c.entries[dir] = nil
		}
	}
	c.cond.Broadcast()
}

func (c *listCache) ready(relDir string) bool { return c.done || c.complete[relDir] }

func (c *listCache) lookup(relDir string) (entries []model.FileEntry, hit, active, done bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, hit = c.entries[relDir]
	return entries, hit && c.ready(relDir), c.active, c.done
}

// await blocks until relDir is complete, the preload finishes or its context
// ends, or a write makes the run's view of relDir old. It deliberately
// ignores the caller's per-call context, whose stall timeout is unsuited to
// one long recursive listing.
func (c *listCache) await(relDir string) ([]model.FileEntry, bool) {
	c.mu.Lock()
	pctx := c.preloadCtx
	c.mu.Unlock()

	notify := make(chan struct{})
	defer close(notify)
	if pctx != nil {
		go func() {
			select {
			case <-pctx.Done():
				c.mu.Lock()
				c.cond.Broadcast()
				c.mu.Unlock()
			case <-notify:
			}
		}()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for !c.ready(relDir) && !c.staleLocked(relDir) && (pctx == nil || pctx.Err() == nil) {
		c.cond.Wait()
	}
	entries, ok := c.entries[relDir]
	return entries, ok && c.ready(relDir)
}

// The invalidators are nil-safe: backends without a preload leave the cache
// nil and still call them after every write.

func (c *listCache) drop(dir string) {
	delete(c.entries, dir)
	delete(c.complete, dir)
}

// changedLocked drops dir after a write to it. While a run is going, dir is
// also marked: see listCache.
func (c *listCache) changedLocked(dir string) {
	c.drop(dir)
	if c.running() {
		if c.stale == nil {
			c.stale = make(map[string]bool)
		}
		c.stale[dir] = true
		c.cond.Broadcast()
	}
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

func (c *listCache) invalidateTree(prefix string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		if k == prefix || strings.HasPrefix(k, prefix+"/") {
			c.drop(k)
		}
	}
	if c.running() {
		if c.staleTree == nil {
			c.staleTree = make(map[string]bool)
		}
		c.staleTree[prefix] = true
		c.cond.Broadcast()
	}
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
