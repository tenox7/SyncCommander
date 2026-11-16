package transfer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
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
	Parallel     int  // start value; Progress.Parallel holds the live one
	Batch        bool // one rsync session for a local subtree when Dst supports it
	VerifyResume bool
	Progress     *Progress
	// Replace holds the destination paths of file/folder conflicts the user
	// agreed to replace in the confirm dialog. A conflict the copy finds that
	// is not in it stays, and copying its path fails.
	Replace []string
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
	kind    string // for messages: "file", "folder", "symbolic link", ...
}

type copier struct {
	req        Request
	ctx        context.Context
	p          *Progress
	src, dst   model.Backend
	mu         sync.Mutex
	dstChanged map[string]bool
	twins      map[string]*twin // by destination path
}

// twin is a destination entry of the other type (a folder where a file is
// copied, or the reverse). It is removed once, right before the first write
// at or under its path, and only when the user confirmed it.
type twin struct {
	entry
	confirmed bool
	once      sync.Once
	err       error
}

// locator is a remote backend that can name the place it opens.
type locator interface{ Location() string }

// sameObject reports whether relPath is one object on both sides: one folder
// opened on both sides (sc with no arguments), a symlinked or bind-mounted
// base, a hard link, or one remote URL on both sides. Writing the destination
// would truncate the source it reads from.
func sameObject(src, dst model.Backend, relPath string) bool {
	sl, sok := src.(model.LocalFS)
	dl, dok := dst.(model.LocalFS)
	if sok && dok {
		si, serr := os.Stat(sl.LocalPath(relPath))
		di, derr := os.Stat(dl.LocalPath(relPath))
		return serr == nil && derr == nil && os.SameFile(si, di)
	}
	sloc, sok := src.(locator)
	dloc, dok := dst.(locator)
	return sok && dok && sloc.Location() == dloc.Location()
}

// displayPath names relPath in the log; the top row is "/".
func displayPath(relPath string) string {
	if relPath == "" {
		return "/"
	}
	return relPath
}

// Copy runs the whole transfer and returns once every file has been tried.
func Copy(ctx context.Context, req Request) Result {
	p := req.Progress
	ctx = transport.ContextWithProgress(ctx, &p.Bytes)
	ctx = transport.ContextWithBaseProgress(ctx, &p.BaseBytes)
	isDir := req.Node.IsDir

	if sameObject(req.Src, req.Dst, req.RelPath) {
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+displayPath(req.RelPath)+": refused, source and destination are the same "+map[bool]string{true: "folder", false: "file"}[isDir])
		return Result{}
	}

	// Copy and mirror-delete enumerate the in-memory tree, which shows an
	// unlisted dir as empty. List the whole subtree first or the copy
	// silently skips it and mirror under-counts what to delete.
	p.Listing.Store(true)
	listed := !isDir || req.Scanner.EnsureSubtreeListed(ctx, req.Node, req.Opts)
	p.Listing.Store(false)
	if !listed {
		// Counted as a failure so the popup and status bar show it, not just
		// the log.
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirFail, "aborted "+req.RelPath+": subtree could not be fully listed")
		return Result{}
	}

	c := &copier{req: req, ctx: ctx, p: p, src: req.Src, dst: req.Dst, dstChanged: map[string]bool{}}
	collisions, files, totalBytes := req.enumerate()
	c.twins = req.twinSet(collisions)
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
		transport.Log.Add("copy", transport.DirFail, fmt.Sprintf("COPY finished with %d failure(s) of %d", failed, p.Total.Load()))
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
				collisions = append(collisions, entry{relPath: c.RelPath, isDir: dstEntry.IsDir, kind: dstEntry.Kind()})
			}
		}
		nodes := []*model.TreeNode{req.Node}
		if req.Node.IsDir {
			nodes = model.CollectCopyFiles(req.Node, &req.Opts, req.LeftToRight)
		}
		files = make([]item, 0, len(nodes))
		for _, f := range nodes {
			it := item{relPath: f.RelPath, src: f.Sides[src].Entry, dst: f.Sides[dst].Entry}
			if it.src != nil && !it.src.IsSpecial() {
				totalBytes += it.src.Size
			}
			files = append(files, it)
		}
	})
	return collisions, files, totalBytes
}

// twinSet indexes the conflicts the copy found and marks the ones the user
// confirmed.
func (req Request) twinSet(collisions []entry) map[string]*twin {
	if len(collisions) == 0 {
		return nil
	}
	confirmed := make(map[string]bool, len(req.Replace))
	for _, p := range req.Replace {
		confirmed[p] = true
	}
	twins := make(map[string]*twin, len(collisions))
	for _, e := range collisions {
		twins[e.relPath] = &twin{entry: e, confirmed: confirmed[e.relPath]}
	}
	return twins
}

// clearTwin removes the twin in the way of relPath, at that path or above
// it, the first time a write needs it gone. An unconfirmed twin is never
// removed: the write fails instead.
func (c *copier) clearTwin(relPath string) error {
	var tw *twin
	for p := relPath; tw == nil && len(c.twins) > 0; p = model.DirOf(p) {
		tw = c.twins[p]
		if p == "" {
			break
		}
	}
	if tw == nil {
		return nil
	}
	tw.once.Do(func() {
		kind := tw.kind
		if !tw.confirmed {
			tw.err = fmt.Errorf("the destination has a %s %s in the way, not replaced without confirmation (copy again to see the dialog)", kind, tw.relPath)
			return
		}
		if err := c.remove(tw.entry); err != nil {
			tw.err = fmt.Errorf("could not remove the %s %s in the way: %w", kind, tw.relPath, err)
			return
		}
		transport.Log.Add("copy", transport.DirIn, "removed "+kind+" "+tw.relPath+" to put the copy there")
	})
	return tw.err
}

// clearAllTwins removes every confirmed twin at once, for a batch transfer
// that writes the whole subtree in one session. False when any twin stays.
func (c *copier) clearAllTwins() bool {
	ok := true
	for p := range c.twins {
		if c.clearTwin(p) != nil {
			ok = false
		}
	}
	return ok
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
		if werr != nil || !d.Type().IsRegular() {
			return nil // rsync skips links, pipes and devices without -l and -D
		}
		if info, err := d.Info(); err == nil {
			batchFiles++
			batchBytes += info.Size()
		}
		return nil
	})
	if batchFiles == 0 || !c.clearAllTwins() {
		return false
	}
	p := c.p
	p.Total.Store(batchFiles)
	p.TotalBytes.Store(batchBytes)
	p.InFlight.Store(1)
	p.Parallel.Store(1)
	p.Batched.Store(true)
	transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("BATCH %s (%d files, %s)", c.req.RelPath, batchFiles, model.FormatSize(batchBytes)))
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
			transport.Log.Add("copy", transport.DirErr, "BATCH "+c.req.RelPath+": "+err.Error())
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
	transport.Log.Add("copy", transport.DirIn, "BATCH "+c.req.RelPath+" OK")
	return true
}

// runParallel copies files with up to Parallel workers; the semaphore is
// published so the UI can resize it mid-copy. Lowering it never stops a
// running file: the first ones to finish are just not replaced.
func (c *copier) runParallel(files []item) {
	parallel := max(c.req.Parallel, 1)
	sem := NewDynSem(parallel)
	// A +/- pressed while the subtree was being listed only reached
	// p.Parallel. Under parallelMu a press either lands before this and is
	// read here, or after and finds sem to resize.
	c.p.parallelMu.Lock()
	if n := int(c.p.Parallel.Load()); n > 0 && n != parallel {
		parallel = n
		sem.Resize(n)
	}
	c.p.Sem.Store(sem)
	c.p.parallelMu.Unlock()
	defer c.p.Sem.Store(nil)
	if parallel > 1 {
		transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("parallel=%d", parallel))
	}
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
		// Nothing to copy is not a success: counted as done, it would let the
		// mirror pass delete while nothing was written.
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": not on the source side")
		return
	}
	// A link, pipe or device is never read or copied (L2, L9). It goes before
	// the twin: nothing is written at its name, so nothing there is removed.
	if srcEntry.IsSpecial() {
		transport.Log.Add("copy", transport.DirWarn, "COPY "+f.relPath+": skipped, a "+srcEntry.Kind()+" (sc copies files and folders only)")
		return
	}
	// A file can be one object on both sides even when its folders are not:
	// a hard link, or a destination folder that links into the source.
	if !srcEntry.IsDir && sameObject(c.src, c.dst, f.relPath) {
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": refused, source and destination are the same file")
		return
	}
	if err := c.clearTwin(f.relPath); err != nil {
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": "+err.Error())
		return
	}
	if srcEntry.IsDir {
		p.File.Store(f.relPath)
		p.beginFile(0)
		if err := c.dst.Mkdir(ctx, f.relPath, srcEntry.Mode); err != nil {
			p.Failed.Add(1)
			transport.Log.Add("copy", transport.DirErr, "mkdir "+f.relPath+": "+err.Error())
			return
		}
		transport.Log.Add("copy", transport.DirIn, "mkdir "+f.relPath)
		return
	}
	slot := p.claimSlot()
	defer p.releaseSlot(slot)
	slot.File.Store(f.relPath)
	slot.Size.Store(srcEntry.Size)
	slot.Start.Store(time.Now().UnixNano())
	slotBytes, slotBase := &slot.Bytes, &slot.BaseBytes
	p.File.Store(f.relPath)
	p.beginFile(srcEntry.Size)
	transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("COPY %s (%s)", f.relPath, model.FormatSize(srcEntry.Size)))

	fileCtx := transport.ContextWithProgress(ctx, slotBytes)
	fileCtx = transport.ContextWithBaseProgress(fileCtx, slotBase)
	fileCtx = transport.ContextWithFileSize(fileCtx, srcEntry.Size)
	fileCtx = transport.ContextWithModTime(fileCtx, srcEntry.ModTime)

	verify := resumeVerifier(c.req.VerifyResume, c.req.Scanner, c.src, c.dst, f.relPath, srcEntry.Size)
	finish := func(how string) {
		if err := c.dst.SetTimes(fileCtx, f.relPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime); err != nil {
			transport.Log.Add("copy", transport.DirErr, "settimes "+f.relPath+": "+err.Error())
		}
		c.markChanged(f.relPath)
		transport.Log.Add("copy", transport.DirIn, "COPY "+f.relPath+" OK"+how)
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
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": "+err.Error())
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
		transport.Log.Add("copy", transport.DirErr, "mirror delete skipped: copy canceled")
		return
	case failed > 0:
		transport.Log.Add("copy", transport.DirErr, fmt.Sprintf("mirror delete skipped: %d file(s) failed to copy", failed))
		return
	}
	var deletes []entry
	c.req.Scanner.ReadTree(func(*model.TreeNode) {
		for _, d := range model.CollectMirrorDeletes(c.req.Node, c.req.LeftToRight) {
			deletes = append(deletes, entry{relPath: d.RelPath, isDir: d.IsDir})
		}
	})
	// The tree is a snapshot from before the copy. Each folder is listed
	// again on both sides and a name is deleted only while it is still on the
	// destination alone; see stillDestOnly.
	var dirs []string
	byDir := map[string][]entry{}
	for _, d := range deletes {
		dir := model.DirOf(d.relPath)
		if _, ok := byDir[dir]; !ok {
			dirs = append(dirs, dir)
		}
		byDir[dir] = append(byDir[dir], d)
	}
	for _, dir := range dirs {
		if c.ctx.Err() != nil {
			return
		}
		keep, err := c.stillDestOnly(dir)
		if err != nil {
			transport.Log.Add("copy", transport.DirErr, "mirror delete in "+displayPath(dir)+" skipped: "+err.Error())
			continue
		}
		for _, d := range byDir[dir] {
			if c.ctx.Err() != nil {
				return
			}
			if why := keep(path.Base(d.relPath)); why != "" {
				transport.Log.Add("copy", transport.DirWarn, "mirror delete "+d.relPath+" skipped: "+why)
				continue
			}
			if err := c.remove(d); err != nil {
				transport.Log.Add("copy", transport.DirErr, "mirror delete "+d.relPath+": "+err.Error())
				continue
			}
			transport.Log.Add("copy", transport.DirIn, "mirror delete "+d.relPath)
		}
	}
}

// stillDestOnly lists dir on both sides now, after the copy, on the servers
// rather than from the scan's cached listings, and returns the check a
// mirror delete in dir must pass: "" to delete name, else why not.
//
// Names are matched byte for byte, but a case-insensitive destination (macOS,
// Windows, SMB, FAT) stores photo.jpg in the existing Photo.JPG: the copy
// just wrote into the "destination-only" file. So a name that differs from a
// source name only in letter case is kept, unless the destination lists both
// names (then they are two files). And when a file copied into dir is not
// listed under its own name or a case variant of it, the destination renames
// what it stores (Unicode normalization, for one): nothing in dir is deleted.
func (c *copier) stillDestOnly(dir string) (func(name string) string, error) {
	ctx := model.WithFreshListing(c.ctx) // not the scan's cached listing
	srcList, err := c.src.List(ctx, dir)
	if err != nil {
		return nil, fmt.Errorf("listing the source: %w", err)
	}
	dstList, err := c.dst.List(ctx, dir)
	if err != nil {
		return nil, fmt.Errorf("listing the destination: %w", err)
	}
	onSrc := make(map[string]bool, len(srcList))
	srcFold := make(map[string]string, len(srcList))
	for _, e := range srcList {
		onSrc[e.Name] = true
		srcFold[strings.ToLower(e.Name)] = e.Name
	}
	onDst := make(map[string]bool, len(dstList))
	dstFold := make(map[string]bool, len(dstList))
	for _, e := range dstList {
		onDst[e.Name] = true
		dstFold[strings.ToLower(e.Name)] = true
	}
	c.mu.Lock()
	var renamed string
	for p := range c.dstChanged {
		if name := path.Base(p); model.DirOf(p) == dir && !onDst[name] && !dstFold[strings.ToLower(name)] {
			renamed = name
			break
		}
	}
	c.mu.Unlock()
	return func(name string) string {
		switch {
		case renamed != "":
			return "the destination does not list the copied " + renamed + " under that name, so names here cannot be matched"
		case !onDst[name]:
			return "no longer on the destination"
		case onSrc[name]:
			return "now on the source too"
		}
		if twin, ok := srcFold[strings.ToLower(name)]; ok && !onDst[twin] {
			return "on this destination it is the same file as " + twin + " (the names differ only in letter case)"
		}
		return ""
	}, nil
}
