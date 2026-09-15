package transfer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"sc/model"
	"sc/transport"
)

// Request describes one copy: the compared node (a file or a subtree) from
// Src to Dst. Node is only read under the scanner's lock; RelPath is the
// caller's snapshot of its path.
type Request struct {
	Src, Dst     model.Backend
	Scanner      *model.Scanner
	Node         *model.TreeNode
	RelPath      string
	LeftToRight  bool
	Mirror       bool // delete destination-only files once every copy landed
	Opts         model.CompareOpts
	Parallel     int
	ParallelMax  int
	Batch        bool // one rsync session for a local subtree when Dst supports it
	VerifyResume bool
	Progress     *Progress
}

// Result tells the caller what to rescan afterwards and which destination
// paths changed, so their cached checksums are dropped.
type Result struct {
	RescanRoot *model.TreeNode
	Changed    *model.ChangedPaths
}

// item and entry are the snapshots a copy works from: the live tree is only
// readable under the scanner's lock, which a transfer cannot hold. Entry
// pointers are safe to keep: a rescan or rename replaces a node's entry with a
// fresh one rather than rewriting the old.
type item struct {
	relPath  string
	src, dst *model.FileEntry
}

type entry struct {
	relPath string
	isDir   bool
}

type copier struct {
	req        Request
	ctx        context.Context
	p          *Progress
	src, dst   model.Backend
	mu         sync.Mutex
	dstChanged map[string]bool
}

// Copy runs the whole transfer and returns once every file has been tried.
func Copy(ctx context.Context, req Request) Result {
	p := req.Progress
	ctx = transport.ContextWithProgress(ctx, &p.Bytes)
	ctx = transport.ContextWithBaseProgress(ctx, &p.BaseBytes)
	isDir := req.Node.IsDir

	// Copy and mirror-delete enumerate the in-memory tree, which shows an
	// unlisted dir as empty. List the whole subtree first or the copy
	// silently skips it and mirror under-counts what to delete.
	p.Listing.Store(true)
	listed := !isDir || req.Scanner.EnsureSubtreeListed(ctx, req.Node, req.Opts)
	p.Listing.Store(false)
	if !listed {
		transport.Log.Add("copy", "ERR", "aborted "+req.RelPath+": subtree could not be fully listed")
		return Result{}
	}

	c := &copier{req: req, ctx: ctx, p: p, src: req.Src, dst: req.Dst, dstChanged: map[string]bool{}}
	collisions, files, totalBytes := req.enumerate()
	c.clearCollisions(collisions)
	p.Total.Store(int64(len(files)))
	p.TotalBytes.Store(totalBytes)
	p.Start.Store(time.Now().UnixNano())

	batched := isDir && req.Batch && c.batch(files, totalBytes)
	if !batched {
		c.runParallel(files)
	}

	res := Result{Changed: &model.ChangedPaths{}}
	if isDir {
		c.mirrorDelete()
		res.RescanRoot = req.Node
	} else {
		res.RescanRoot = req.Scanner.FindNearestDestNode(model.DirOf(req.RelPath), req.LeftToRight)
	}
	if failed := p.Failed.Load(); failed > 0 {
		transport.Log.Add("copy", "ERR", fmt.Sprintf("COPY finished with %d failure(s) of %d", failed, p.Total.Load()))
	}
	// Batch rewrites every file in the subtree, not just the diff set, so the
	// whole subtree's cached checksums go rather than the diff paths alone.
	var changedDirs []string
	if batched {
		changedDirs = []string{req.RelPath}
	}
	_, dst := model.CopySides(req.LeftToRight)
	res.Changed.Paths[dst], res.Changed.Dirs[dst] = c.dstChanged, changedDirs
	return res
}

// enumerate snapshots everything the transfer needs from the live tree in one
// locked pass; the transfer never touches a node again.
func (req Request) enumerate() (collisions []entry, files []item, totalBytes int64) {
	src, dst := model.CopySides(req.LeftToRight)
	req.Scanner.ReadTree(func(*model.TreeNode) {
		for _, c := range model.CollectTypeCollisions(req.Node, req.LeftToRight) {
			if dstEntry := c.Sides[dst].Entry; dstEntry != nil {
				collisions = append(collisions, entry{relPath: c.RelPath, isDir: dstEntry.IsDir})
			}
		}
		nodes := []*model.TreeNode{req.Node}
		if req.Node.IsDir {
			nodes = model.CollectCopyFiles(req.Node, &req.Opts, req.LeftToRight)
		}
		files = make([]item, 0, len(nodes))
		for _, f := range nodes {
			it := item{relPath: f.RelPath, src: f.Sides[src].Entry, dst: f.Sides[dst].Entry}
			if it.src != nil {
				totalBytes += it.src.Size
			}
			files = append(files, it)
		}
	})
	return collisions, files, totalBytes
}

// clearCollisions removes destination entries whose type (file vs dir) clashes
// with the source, since a copy could not land over them.
func (c *copier) clearCollisions(collisions []entry) {
	for _, e := range collisions {
		if c.ctx.Err() != nil {
			return
		}
		if err := c.remove(e); err != nil {
			c.p.Failed.Add(1)
			transport.Log.Add("copy", "ERR", "type-collision cleanup "+e.relPath+": "+err.Error())
			continue
		}
		transport.Log.Add("copy", "<<<", "type-collision cleanup "+e.relPath)
	}
}

func (c *copier) remove(e entry) error {
	if e.isDir {
		return c.dst.RemoveAll(c.ctx, e.relPath)
	}
	return c.dst.Remove(c.ctx, e.relPath)
}

func (c *copier) markChanged(relPath string) {
	c.mu.Lock()
	c.dstChanged[relPath] = true
	c.mu.Unlock()
}

// batch sends a local subtree in one rsync session when the destination
// supports it. Reports false when it did not apply or failed, so the
// per-file path runs instead.
func (c *copier) batch(files []item, totalBytes int64) bool {
	bs, ok := c.dst.(model.BatchSender)
	lp, isLocal := c.src.(model.LocalFS)
	if !ok || !isLocal {
		return false
	}
	srcRoot := lp.LocalPath("")
	var batchFiles, batchBytes int64
	_ = filepath.WalkDir(filepath.Join(srcRoot, c.req.RelPath), func(_ string, d fs.DirEntry, werr error) error {
		if werr != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			batchFiles++
			batchBytes += info.Size()
		}
		return nil
	})
	if batchFiles == 0 {
		return false
	}
	p := c.p
	p.Total.Store(batchFiles)
	p.TotalBytes.Store(batchBytes)
	p.InFlight.Store(1)
	p.Parallel.Store(1)
	p.Batched.Store(true)
	transport.Log.Add("copy", ">>>", fmt.Sprintf("BATCH %s (%d files, %s)", c.req.RelPath, batchFiles, model.FormatSize(batchBytes)))
	err := bs.SendLocalTree(transport.ContextWithFileSize(c.ctx, batchBytes), srcRoot, c.req.RelPath, func(name string) {
		p.File.Store(name)
		if p.Done.Load() < p.Total.Load() {
			p.Done.Add(1)
		}
	})
	p.InFlight.Store(0)
	p.Batched.Store(false)
	if err != nil {
		if !errors.Is(err, transport.ErrUnsupported) {
			transport.Log.Add("copy", "ERR", "BATCH "+c.req.RelPath+": "+err.Error())
		}
		p.Total.Store(int64(len(files)))
		p.TotalBytes.Store(totalBytes)
		p.Parallel.Store(int64(max(c.req.Parallel, 1)))
		for _, ctr := range []*atomic.Int64{&p.Done, &p.Bytes, &p.CompletedBytes, &p.CompletedBaseBytes} {
			ctr.Store(0)
		}
		return false
	}
	p.Done.Store(p.Total.Load())
	p.CompletedBytes.Store(p.Bytes.Load())
	p.CompletedBaseBytes.Store(p.BaseBytes.Load())
	transport.Log.Add("copy", "<<<", "BATCH "+c.req.RelPath+" OK")
	return true
}

// runParallel copies files with up to Parallel workers; the semaphore is
// published so the UI can resize it mid-copy.
func (c *copier) runParallel(files []item) {
	parallel := max(c.req.Parallel, 1)
	if parallel > 1 {
		transport.Log.Add("copy", ">>>", fmt.Sprintf("parallel=%d", parallel))
	}
	sem := NewDynSem(parallel)
	c.p.Sem.Store(sem)
	defer c.p.Sem.Store(nil)
	var wg sync.WaitGroup
	for _, f := range files {
		if sem.Acquire(c.ctx) != nil {
			break
		}
		wg.Add(1)
		c.p.InFlight.Add(1)
		go func(f item) {
			defer wg.Done()
			defer sem.Release()
			defer c.p.InFlight.Add(-1)
			c.copyOne(f)
		}(f)
	}
	wg.Wait()
}

// copyOne moves one item: directories are created, type mismatches on the
// destination cleared, and files go through resume, direct transfer and
// finally a retried streaming copy.
func (c *copier) copyOne(f item) {
	ctx, p := c.ctx, c.p
	defer p.Done.Add(1)
	srcEntry, dstEntry := f.src, f.dst
	if srcEntry == nil {
		return
	}
	if srcEntry.IsDir {
		p.File.Store(f.relPath)
		p.beginFile(0)
		if err := c.dst.Mkdir(ctx, f.relPath, srcEntry.Mode); err != nil {
			p.Failed.Add(1)
			transport.Log.Add("copy", "ERR", "mkdir "+f.relPath+": "+err.Error())
			return
		}
		transport.Log.Add("copy", "<<<", "mkdir "+f.relPath)
		return
	}
	if dstEntry != nil && dstEntry.IsDir != srcEntry.IsDir {
		if err := c.remove(entry{relPath: f.relPath, isDir: dstEntry.IsDir}); err != nil {
			p.Failed.Add(1)
			transport.Log.Add("copy", "ERR", "clear dst type-mismatch "+f.relPath+": "+err.Error())
			return
		}
		transport.Log.Add("copy", "<<<", "cleared dst type-mismatch "+f.relPath)
		dstEntry = nil
	}
	slot := p.claimSlot()
	defer p.releaseSlot(slot)
	slotBytes, slotBase := &p.Bytes, &p.BaseBytes
	if slot != nil {
		slot.File.Store(f.relPath)
		slot.Size.Store(srcEntry.Size)
		slot.Start.Store(time.Now().UnixNano())
		slotBytes, slotBase = &slot.Bytes, &slot.BaseBytes
	}
	p.File.Store(f.relPath)
	p.beginFile(srcEntry.Size)
	transport.Log.Add("copy", ">>>", fmt.Sprintf("COPY %s (%s)", f.relPath, model.FormatSize(srcEntry.Size)))

	fileCtx := transport.ContextWithProgress(ctx, slotBytes)
	fileCtx = transport.ContextWithBaseProgress(fileCtx, slotBase)
	fileCtx = transport.ContextWithFileSize(fileCtx, srcEntry.Size)
	fileCtx = transport.ContextWithModTime(fileCtx, srcEntry.ModTime)

	verify := resumeVerifier(c.req.VerifyResume, c.req.Scanner, c.src, c.dst, f.relPath, srcEntry.Size)
	finish := func(how string) {
		if err := c.dst.SetTimes(fileCtx, f.relPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime); err != nil {
			transport.Log.Add("copy", "ERR", "settimes "+f.relPath+": "+err.Error())
		}
		c.markChanged(f.relPath)
		transport.Log.Add("copy", "<<<", "COPY "+f.relPath+" OK"+how)
	}
	guarded := func(op func(context.Context) bool) (ok bool) {
		_ = transport.WithStallGuard(fileCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
			ok = op(attemptCtx)
			return nil
		})
		return ok
	}

	// Resume first when a partial dst body exists: append the missing tail
	// rather than overwrite the whole file. tryResumeCopy self-gates (no-op
	// for absent/full/oversized dst); the appended prefix is never read back,
	// so verify checks it afterwards.
	if guarded(func(a context.Context) bool {
		return tryResumeCopy(a, c.src, c.dst, f.relPath, srcEntry, dstEntry, slotBytes, slotBase, verify)
	}) {
		finish(" (resumed)")
		return
	}
	if guarded(func(a context.Context) bool { return tryDirectTransfer(a, c.src, c.dst, f.relPath, srcEntry) }) {
		finish("")
		return
	}

	attempt := 0
	err := transport.Retry(fileCtx, "copy", "copy "+f.relPath, func() error {
		return transport.WithStallGuard(fileCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
			attempt++
			if attempt > 1 {
				// A retry resumes whatever the failed attempt left behind.
				if offset := peekDstSize(attemptCtx, c.dst, f.relPath); offset > 0 && offset < srcEntry.Size {
					err := resumeAttempt(attemptCtx, c.src, c.dst, f.relPath, srcEntry, offset, slotBytes, slotBase, verify)
					if err == nil || (!errors.Is(err, transport.ErrUnsupported) && !errors.Is(err, errResumeMismatch)) {
						return err
					}
				}
			}
			return fullCopyAttempt(attemptCtx, c.src, c.dst, f.relPath, srcEntry, slotBytes)
		})
	})
	if err != nil {
		p.Failed.Add(1)
		transport.Log.Add("copy", "ERR", "COPY "+f.relPath+": "+err.Error())
		return
	}
	finish("")
}

// mirrorDelete removes destination-only entries. It must only run once every
// copy landed: a partial copy plus a full delete pass would destroy data the
// source still holds.
func (c *copier) mirrorDelete() {
	failed := c.p.Failed.Load()
	switch {
	case !c.req.Mirror:
		return
	case c.ctx.Err() != nil:
		transport.Log.Add("copy", "ERR", "mirror delete skipped: copy canceled")
		return
	case failed > 0:
		transport.Log.Add("copy", "ERR", fmt.Sprintf("mirror delete skipped: %d file(s) failed to copy", failed))
		return
	}
	var deletes []entry
	c.req.Scanner.ReadTree(func(*model.TreeNode) {
		for _, d := range model.CollectMirrorDeletes(c.req.Node, c.req.LeftToRight) {
			deletes = append(deletes, entry{relPath: d.RelPath, isDir: d.IsDir})
		}
	})
	for _, d := range deletes {
		if c.ctx.Err() != nil {
			return
		}
		if err := c.remove(d); err != nil {
			transport.Log.Add("copy", "ERR", "mirror delete "+d.relPath+": "+err.Error())
			continue
		}
		transport.Log.Add("copy", "<<<", "mirror delete "+d.relPath)
	}
}
