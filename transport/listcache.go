package transport

import (
	"context"
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
type listCache struct {
	mu         sync.Mutex
	cond       *sync.Cond
	entries    map[string][]model.FileEntry
	complete   map[string]bool
	active     bool
	done       bool
	preloadCtx context.Context
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
}

// recursiveLister streams a subtree: emit appends a batch of children to
// parent, complete declares that every child of dir has been emitted.
// Emitters whose order cannot prove that never call complete.
type recursiveLister func(ctx context.Context, scope string, emit func(parent string, entries []model.FileEntry), complete func(dir string)) error

// start runs one recursive listing in the background, feeding the cache
// through emit. A preload already in flight is left alone. When run fails
// the partial contents are dropped, so List falls back to live per-dir calls
// instead of trusting a truncated tree.
func (c *listCache) start(ctx context.Context, scope string, run recursiveLister) {
	c.mu.Lock()
	if c.active && !c.done {
		c.mu.Unlock()
		return
	}
	c.reset()
	c.active, c.done = true, false
	c.preloadCtx = ctx
	c.mu.Unlock()

	go func() {
		err := run(ctx, scope, c.emit, c.markComplete)
		c.mu.Lock()
		if err != nil {
			c.reset()
		}
		c.done = true
		c.cond.Broadcast()
		c.mu.Unlock()
	}()
}

// serve answers List from the cache: a hit returns at once, an in-flight
// preload is awaited, anything else goes live.
func (c *listCache) serve(ctx context.Context, relDir string, live func(context.Context, string) ([]model.FileEntry, error)) ([]model.FileEntry, error) {
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
	c.entries[parent] = append(c.entries[parent], entries...)
	c.mu.Unlock()
}

// markComplete releases awaiters of dir and registers it even when nothing
// was emitted, so an empty dir is a hit rather than a live call.
func (c *listCache) markComplete(dir string) {
	c.mu.Lock()
	c.complete[dir] = true
	if _, ok := c.entries[dir]; !ok {
		c.entries[dir] = nil
	}
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *listCache) ready(relDir string) bool { return c.done || c.complete[relDir] }

func (c *listCache) lookup(relDir string) (entries []model.FileEntry, hit, active, done bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, hit = c.entries[relDir]
	return entries, hit && c.ready(relDir), c.active, c.done
}

// await blocks until relDir is complete, the preload finishes or its context
// ends. It deliberately ignores the caller's per-call context, whose stall
// timeout is unsuited to one long recursive listing.
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
	for !c.ready(relDir) && (pctx == nil || pctx.Err() == nil) {
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

func (c *listCache) invalidate(relDir string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.drop(relDir)
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
		c.drop(p)
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
}

func parentDir(relPath string) string {
	p := path.Dir(relPath)
	if p == "." {
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
// finish, so an empty leaf is a cache hit rather than a live list.
type emitGrouper struct {
	emit         func(string, []model.FileEntry)
	complete     func(string)
	lateChildren bool
	current      string
	have         bool
	batch        []model.FileEntry
	dirs         map[string]struct{}
	open         []string
}

func (g *emitGrouper) add(e model.FileEntry) {
	g.closeOutside(e.RelPath)
	parent := parentDir(e.RelPath)
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
	if g.dirs == nil {
		g.dirs = make(map[string]struct{})
	}
	g.dirs[e.RelPath] = struct{}{}
	if !g.lateChildren {
		g.push(e.RelPath)
	}
}

func (g *emitGrouper) push(dir string) {
	if g.complete != nil {
		g.open = append(g.open, dir)
	}
}

// closeOutside completes every open dir that rel is not under; "" closes all.
func (g *emitGrouper) closeOutside(rel string) {
	for len(g.open) > 0 {
		d := g.open[len(g.open)-1]
		if strings.HasPrefix(rel, d+"/") {
			return
		}
		g.flush()
		g.complete(d)
		delete(g.dirs, d)
		g.open = g.open[:len(g.open)-1]
	}
}

func (g *emitGrouper) flush() {
	if !g.have {
		return
	}
	g.emit(g.current, g.batch)
	g.batch, g.have = nil, false
}

func (g *emitGrouper) finish() {
	g.flush()
	g.closeOutside("")
	for d := range g.dirs {
		if g.complete != nil {
			g.complete(d)
			continue
		}
		g.emit(d, nil)
	}
}
