package transport

import (
	"context"
	"path"
	"strings"
	"sync"

	"sc/model"
)

// listCache holds per-directory entries fed by one recursive listing running
// in the background. List serves hits from it and, while the preload is in
// flight, waits for it to finish rather than issuing a live per-dir call.
// preloadCtx is the scan context the wait follows, so a per-call stall
// timeout on List does not abandon a slow but healthy preload.
type listCache struct {
	mu         sync.Mutex
	cond       *sync.Cond
	entries    map[string][]model.FileEntry
	active     bool
	done       bool
	preloadCtx context.Context
}

func newListCache() *listCache {
	c := &listCache{entries: make(map[string][]model.FileEntry)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

type recursiveLister func(ctx context.Context, scope string, emit func(parent string, entries []model.FileEntry)) error

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
	c.entries = make(map[string][]model.FileEntry)
	c.active, c.done = true, false
	c.preloadCtx = ctx
	c.mu.Unlock()

	go func() {
		err := run(ctx, scope, c.emit)
		c.mu.Lock()
		if err != nil {
			c.entries = make(map[string][]model.FileEntry)
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

// emit appends entries for parent without waking awaiters: recursive
// listings revisit a parent several times, so any single emit may be a
// partial view. Only the end of the run broadcasts.
func (c *listCache) emit(parent string, entries []model.FileEntry) {
	c.mu.Lock()
	c.entries[parent] = append(c.entries[parent], entries...)
	c.mu.Unlock()
}

func (c *listCache) lookup(relDir string) (entries []model.FileEntry, hit, active, done bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, hit = c.entries[relDir]
	return entries, hit, c.active, c.done
}

// await blocks until the preload finishes or its context ends, then returns
// whatever the cache holds for relDir. It deliberately ignores the caller's
// per-call context, whose stall timeout is unsuited to one long recursive
// listing.
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
	for !c.done && (pctx == nil || pctx.Err() == nil) {
		c.cond.Wait()
	}
	entries, ok := c.entries[relDir]
	return entries, ok
}

// The invalidators are nil-safe: backends without a preload leave the cache
// nil and still call them after every write.

func (c *listCache) invalidate(relDir string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, relDir)
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
		delete(c.entries, p)
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
			delete(c.entries, k)
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
// batch on parent change. finish flushes the last batch and registers every
// directory seen, so an empty leaf is a cache hit rather than a live list.
type emitGrouper struct {
	emit    func(string, []model.FileEntry)
	current string
	have    bool
	batch   []model.FileEntry
	dirs    []string
}

func (g *emitGrouper) add(e model.FileEntry) {
	parent := parentDir(e.RelPath)
	if !g.have || parent != g.current {
		g.flush()
		g.current, g.have = parent, true
	}
	g.batch = append(g.batch, e)
	if e.IsDir {
		g.dirs = append(g.dirs, e.RelPath)
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
	for _, d := range g.dirs {
		g.emit(d, nil)
	}
}
