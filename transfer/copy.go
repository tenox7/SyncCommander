package transfer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
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
	Opts         model.CompareOpts
	Parallel     int  // start value; Progress.Parallel holds the live one
	Batch        bool // one rsync session for a local subtree when Dst supports it
	VerifyResume bool
	Progress     *Progress
	// Plan is what the copy may destroy or write over: what the user was
	// shown and agreed to (PlanCopy). The zero Plan agrees to nothing: a
	// conflict or a newer file fails its path, a file not compared yet is
	// left out, and nothing is deleted.
	Plan Plan
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
	part     bool   // the scan listed a part file of it on the destination
	clash    string // why the destination cannot hold it apart from another source name; "" when it can
	newer    string // the destination's file is newer or larger, and the user was not asked: not written
}

type entry struct {
	relPath string
	isDir   bool
	kind    string // for messages: "file", "folder", "symbolic link", ...
}

type copier struct {
	req        Request
	ctx        context.Context
	stop       context.CancelFunc // ends the whole copy
	stopped    atomic.Bool
	cut        atomic.Int64 // files the user's cancel cut off: counted as done, not failed
	p          *Progress
	src, dst   model.Backend
	mu         sync.Mutex
	dstChanged map[string]bool
	twins      map[string]*twin // by destination path
	linker     model.Linker     // the destination's, when it makes links; else nil
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
// would truncate the source it reads from. A link is written by its name and
// what it leads to is never read: link asks about the links themselves, and
// two links to one file are two objects.
func sameObject(src, dst model.Backend, relPath string, link bool) bool {
	sl, sok := src.(model.LocalFS)
	dl, dok := dst.(model.LocalFS)
	if sok && dok {
		stat := os.Stat
		if link {
			stat = os.Lstat
		}
		si, serr := stat(sl.LocalPath(relPath))
		di, derr := stat(dl.LocalPath(relPath))
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

	if sameObject(req.Src, req.Dst, req.RelPath, req.Node.IsLink) {
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
		// The user's x while the subtree was listed: nothing was copied, and
		// nothing failed.
		if transport.UserCanceled(ctx) {
			transport.Log.Add("copy", transport.DirIn, "COPY canceled while listing "+displayPath(req.RelPath))
			return Result{}
		}
		// Counted as a failure so the popup and status bar show it, not just
		// the log.
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirFail, "aborted "+req.RelPath+": subtree could not be fully listed")
		return Result{}
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()
	c := &copier{req: req, ctx: ctx, stop: stop, p: p, src: req.Src, dst: req.Dst, dstChanged: map[string]bool{}}
	var names model.NameRule
	if caps, err := req.Dst.Capabilities(ctx); err == nil {
		names = caps.Names
	}
	// Asked first: a remote's wrapper has every optional method (D2).
	c.linker, _ = capable[model.Linker](ctx, req.Dst, func(caps model.Caps) bool { return caps.Links })
	collisions, files, totalBytes, leftOut, keptNewer := req.enumerate(names, c.linker != nil)
	c.twins = req.twinSet(collisions)
	p.Total.Store(int64(len(files)))
	p.TotalBytes.Store(totalBytes)
	p.Start.Store(time.Now().UnixNano())
	if leftOut > 0 {
		transport.Log.Add("copy", transport.DirWarn, fmt.Sprintf("COPY %s: left out %d files on both sides that are not compared yet", displayPath(req.RelPath), leftOut))
	}
	if keptNewer > 0 {
		transport.Log.Add("copy", transport.DirWarn, fmt.Sprintf("COPY %s: left out %d files that are newer or larger on %s", displayPath(req.RelPath), keptNewer, dstSideName(req.LeftToRight)))
	}

	// One rsync session sends every file in the folder: not when some are to
	// be left out or refused.
	batched := isDir && req.Batch && leftOut == 0 && keptNewer == 0 && !slices.ContainsFunc(files, func(f item) bool { return f.clash != "" || f.newer != "" }) && c.batch(files, totalBytes)
	if !batched {
		c.runParallel(files)
	}

	res := Result{Changed: &model.ChangedPaths{}}
	failed := p.Failed.Load() // the copies'; the mirror's deletes count after
	if isDir {
		c.mirrorDelete()
		res.RescanRoot = req.Node
	} else {
		res.RescanRoot = req.Scanner.FindNearestDestNode(model.DirOf(req.RelPath), req.LeftToRight)
	}
	switch {
	case c.stopped.Load():
		transport.Log.Add("copy", transport.DirFail, fmt.Sprintf("COPY stopped: %d of %d tried, %d of them failed", p.Done.Load(), p.Total.Load(), failed))
	case c.canceled():
		// No FAIL: what failed before the cancel has its own ERR line, and
		// what the cancel cut off did not fail.
		msg := fmt.Sprintf("COPY canceled: %d of %d done", p.Done.Load()-c.cut.Load(), p.Total.Load())
		if failed > 0 {
			msg += fmt.Sprintf(", %d of them failed", failed)
		}
		transport.Log.Add("copy", transport.DirIn, msg)
	case failed > 0:
		transport.Log.Add("copy", transport.DirFail, fmt.Sprintf("COPY finished with %d failure(s) of %d", failed, p.Total.Load()))
	}
	if n := p.TimesNotSet.Load(); n > 0 {
		// WARN: the copies went through; each file's own ERR line says why.
		transport.Log.Add("copy", transport.DirWarn, fmt.Sprintf("COPY: %d file(s) copied without their times: they show as different, and the next copy sends them again", n))
	}
	if n := p.ModesNotSet.Load(); n > 0 {
		transport.Log.Add("copy", transport.DirWarn, fmt.Sprintf("COPY: %d file(s) copied without their modes: the destination would not take them", n))
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
// locked pass; the transfer never touches a node again. leftOut counts the
// files not compared yet that a folder copy leaves out (not in Plan.Unknown),
// and keptNewer the files newer or larger on the destination it leaves as
// they are (Plan.KeepNewer). names is how the destination matches names, for
// the clashes it would make; links, whether it makes links.
func (req Request) enumerate(names model.NameRule, links bool) (collisions []entry, files []item, totalBytes int64, leftOut, keptNewer int) {
	src, dst := model.CopySides(req.LeftToRight)
	unknown := pathSet(req.Plan.Unknown, func(p string) string { return p })
	overwrite := pathSet(req.Plan.Newer, func(n Newer) string { return n.RelPath })
	req.Scanner.ReadTree(func(*model.TreeNode) {
		for _, c := range model.CollectTypeCollisions(req.Node, req.LeftToRight, links) {
			if dstEntry := c.Sides[dst].Entry; dstEntry != nil {
				collisions = append(collisions, entry{relPath: c.RelPath, isDir: dstEntry.IsDir, kind: dstEntry.Kind()})
			}
		}
		// A file the user copies is copied; a folder copy writes what differs.
		var nodes []*model.TreeNode
		if !req.Node.IsDir {
			nodes = []*model.TreeNode{req.Node}
		} else {
			model.WalkCopyFiles(req.Node, &req.Opts, req.LeftToRight, func(n *model.TreeNode, notCompared bool) {
				if notCompared && !unknown[n.RelPath] {
					leftOut++
					return
				}
				nodes = append(nodes, n)
			})
		}
		files = make([]item, 0, len(nodes))
		parts := partsListed(dst)
		clash := nameClashes(src, names, links)
		twinned := sourceTwins(src)
		for _, f := range nodes {
			newer := model.OverwritesNewer(f, req.LeftToRight, req.Opts)
			switch {
			case newer == "" || overwrite[f.RelPath]:
				newer = ""
			case req.Plan.KeepNewer:
				keptNewer++
				continue
			}
			it := item{relPath: f.RelPath, src: f.Sides[src].Entry, dst: f.Sides[dst].Entry, part: parts(f), clash: clash(f), newer: newer}
			if it.clash == "" && links && f.IsLink {
				it.clash = twinned(f)
			}
			if it.src != nil && !it.src.IsSpecial() {
				totalBytes += it.src.Size
			}
			files = append(files, it)
		}
	})
	return collisions, files, totalBytes, leftOut, keptNewer
}

// dstSideName is the side a copy writes to, as the panels name it.
func dstSideName(leftToRight bool) string {
	if leftToRight {
		return "right"
	}
	return "left"
}

// nameClashes returns, for a node to copy, why the destination cannot hold
// it: its name, or a folder's above it, is one name there with another
// name on the source side (D10). A folding destination (a Mac, Windows)
// took A.txt and a.txt for one file: the copy wrote it twice, the second
// over the first, and both were "OK". Every source name counts, not only
// those copied: a.txt copied next to an A.txt already there is written
// over it.
//
// Letter case is folded only where the destination is known to fold it:
// names that differ only in case are common where case counts (Linux
// sources), and no remote's rule is known. Unicode forms are folded unless
// the destination is known to keep them apart: two names that differ only
// in form are one word typed on two systems, almost never two files kept
// on purpose, and Macs and many servers store them as one.
//
// A name the destination cannot hold as it is (NameRule.Refuses) is
// refused too: on Windows, a\b landed on the file b in a folder a, and
// NUL on the device, and the copy said OK (L25).
//
// A link takes a name where the destination makes links: a link D to /etc
// and a folder d, written on a Mac, put the folder's files in /etc.
func nameClashes(src model.Side, rule model.NameRule, links bool) func(*model.TreeNode) string {
	guard := rule
	if guard.Forms == model.FoldUnknown {
		guard.Forms = model.Folded
	}
	if !guard.Folds() && !rule.Windows {
		return func(*model.TreeNode) string { return "" }
	}
	byDir := map[*model.TreeNode]map[string][]string{} // a folder's source names by guard.Key
	clashOf := func(n *model.TreeNode) string {
		dir := n.Parent
		if dir == nil {
			return ""
		}
		if why := rule.Refuses(n.Name); why != "" {
			return fmt.Sprintf("the destination cannot hold the name %#q: %s", n.Name, why)
		}
		if !guard.Folds() {
			return ""
		}
		keys, ok := byDir[dir]
		if !ok {
			keys = map[string][]string{}
			for _, c := range dir.Children {
				// A pipe is never written, so it takes no name.
				if e := c.Sides[src].Entry; e != nil && c.Writes(links) {
					k := guard.Key(c.Name)
					keys[k] = append(keys[k], c.Name)
				}
			}
			byDir[dir] = keys
		}
		for _, other := range keys[guard.Key(n.Name)] {
			if other == n.Name {
				continue
			}
			pair := n.RelPath + " and " + path.Join(model.DirOf(n.RelPath), other)
			if rule.Key(n.Name) == rule.Key(other) {
				return "the destination takes " + pair + " for one name"
			}
			return "the destination may take " + pair + " for one name: it is not known whether it keeps Unicode forms apart"
		}
		return ""
	}
	dirWhy := map[*model.TreeNode]string{}
	var check func(*model.TreeNode) string
	check = func(n *model.TreeNode) string {
		if why := clashOf(n); why != "" || n.Parent == nil {
			return why
		}
		why, ok := dirWhy[n.Parent]
		if !ok {
			why = check(n.Parent)
			dirWhy[n.Parent] = why
		}
		return why
	}
	return check
}

// sourceTwins returns, for a link to copy, why it is refused: the source
// lists its name twice, as the link and as another kind, which one
// listing of a folder cannot hold. A server sends what it likes (L7), and
// the link written first, the other's files were written through it: a
// link d to /etc and a folder d holding passwd put passwd in /etc. Every
// other kind is written as it is: nothing leads out of the tree through it.
func sourceTwins(src model.Side) func(*model.TreeNode) string {
	byDir := map[*model.TreeNode]map[string]string{} // a folder's source names that are no link, to their kinds
	return func(n *model.TreeNode) string {
		dir := n.Parent
		if dir == nil {
			return ""
		}
		kinds, ok := byDir[dir]
		if !ok {
			for _, c := range dir.Children {
				if e := c.Sides[src].Entry; e != nil && !c.IsLink {
					if kinds == nil {
						kinds = map[string]string{}
					}
					kinds[c.Name] = e.Kind()
				}
			}
			byDir[dir] = kinds
		}
		if kind, ok := kinds[n.Name]; ok {
			return "the source lists " + n.Name + " twice, as a symbolic link and as a " + kind
		}
		return ""
	}
}

// partsListed returns whether the scan listed a part file of a node on the
// dst side, looking at each folder's rows once.
func partsListed(dst model.Side) func(*model.TreeNode) bool {
	byDir := map[*model.TreeNode]map[string]bool{}
	return func(n *model.TreeNode) bool {
		dir := n.Parent
		if dir == nil || n.IsDir {
			return false
		}
		names, ok := byDir[dir]
		if !ok {
			for _, c := range dir.Children {
				if e := c.Sides[dst].Entry; e != nil && e.IsPart() {
					if names == nil {
						names = map[string]bool{}
					}
					names[c.Name] = true
				}
			}
			byDir[dir] = names
		}
		return names[model.PartName(n.Name)]
	}
}

// twinSet indexes the conflicts the copy found and marks the ones the user
// confirmed.
func (req Request) twinSet(collisions []entry) map[string]*twin {
	if len(collisions) == 0 {
		return nil
	}
	confirmed := pathSet(req.Plan.Conflicts, func(c Conflict) string { return c.RelPath })
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
		c.markChanged(tw.relPath)
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

// markChanged records a destination path the copy wrote or removed, or
// tried to: the rescan lists the folders that hold such paths, and no others
// (F12), and their cached checksums go.
func (c *copier) markChanged(relPath string) {
	c.mu.Lock()
	c.dstChanged[relPath] = true
	c.mu.Unlock()
}

// batch sends a local subtree in one rsync session when the destination
// supports it. Reports false when it did not apply or failed, so the
// per-file path runs instead.
func (c *copier) batch(files []item, totalBytes int64) bool {
	lp, isLocal := c.src.(model.LocalFS)
	if !isLocal {
		return false
	}
	// Asked first: a remote's wrapper has every optional method (D2).
	bs, ok := capable[model.BatchSender](c.ctx, c.dst, func(caps model.Caps) bool { return caps.Batch })
	if !ok {
		return false
	}
	// The session sends what the copy would send file by file, and only
	// that: not the rest of the folder, and nothing when nothing differs
	// (F20). Links, devices and the parts cut-off copies left are not sent:
	// copyOne skips them, and so does the batch. The links are made after
	// it, one by one.
	var paths []string
	var skipped []item
	var batchFiles, batchBytes int64
	for _, f := range files {
		switch {
		case f.src == nil:
			return false // file by file, it counts as failed
		case lp.LocalPath(f.relPath) == "":
			// No local path: a name this system cannot hold, which rsync
			// would read as another file or a device (L25). File by file,
			// it fails with the reason.
			return false
		case f.src.IsSpecial():
			skipped = append(skipped, f)
			continue
		case !f.src.IsDir:
			batchFiles++
			batchBytes += f.src.Size
		}
		paths = append(paths, f.relPath)
	}
	if batchFiles == 0 || !c.clearAllTwins() {
		return false
	}
	p := c.p
	p.Total.Store(batchFiles + int64(len(skipped)))
	p.TotalBytes.Store(batchBytes)
	p.InFlight.Store(1)
	p.Parallel.Store(1)
	p.Batched.Store(true)
	transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("BATCH %s (%d files, %s)", c.req.RelPath, batchFiles, model.FormatSize(batchBytes)))
	err := bs.SendLocalTree(c.ctx, lp.LocalPath(""), c.req.RelPath, paths, func(name string) {
		p.File.Store(name)
		if p.Done.Load() < p.Total.Load() {
			p.Done.Add(1)
		}
	})
	p.InFlight.Store(0)
	p.Batched.Store(false)
	if err != nil && c.canceled() {
		// The user's x: nothing goes file by file, and what the session
		// sent stays counted (a failed session counts from 0 again, below).
		return true
	}
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
	p.Done.Store(batchFiles)
	p.CompletedBytes.Store(p.Bytes.Load())
	p.CompletedBaseBytes.Store(p.BaseBytes.Load())
	transport.Log.Add("copy", transport.DirIn, "BATCH "+c.req.RelPath+" OK")
	for _, f := range skipped {
		// Only now: a batch that failed goes file by file, which says so.
		// A link is made now, or skipped with the reason.
		c.copyOne(f)
	}
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

// canceled reports whether the user canceled the copy. The copier's own
// stop (a failure no file gets through) is not the user's.
func (c *copier) canceled() bool { return transport.UserCanceled(c.ctx) }

// failed counts one failure and logs it. A failure after which no other file
// can get through stops the whole copy: a refused login, a side that
// stopped connecting after its connects failed in a row, a destination
// that is full (transport.GivesUp). The other files would each fail the
// same way after their own tries (F7). A file that fails once the user
// canceled is no failure: the cancel cut it off, and the next copy sends it.
func (c *copier) failed(what string, err error) {
	if c.canceled() {
		c.cut.Add(1)
		transport.Log.Add("copy", transport.DirIn, what+": canceled")
		return
	}
	c.p.Failed.Add(1)
	transport.Log.Add("copy", transport.DirErr, what+": "+err.Error())
	if transport.GivesUp(err) && c.stopped.CompareAndSwap(false, true) {
		transport.Log.Add("copy", transport.DirFail, "COPY stopped: no file can get through: "+err.Error())
		c.stop()
	}
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
	// A pipe or device is never read or copied (L2, L9), nor is a part file
	// (L14). A link is made as a link to the same target, never read or
	// followed, where the destination makes links. It goes before the twin:
	// nothing is written at its name, so nothing there is removed.
	target, link := "", false
	if srcEntry.IsLink() && c.linker != nil {
		target, link = srcEntry.Target()
	}
	if srcEntry.IsSpecial() && !link {
		c.logSkipped(f)
		return
	}
	// Before the twin too: nothing is written, so nothing is removed.
	if f.clash != "" {
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": refused, "+f.clash)
		return
	}
	// Nor over a newer or larger file the user was not asked about: the
	// dialog counted it only when the tree held it (F16).
	if f.newer != "" {
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": refused, it is "+f.newer+" on "+dstSideName(c.req.LeftToRight)+": copy it again to be asked")
		return
	}
	// A file can be one object on both sides even when its folders are not:
	// a hard link, or a destination folder that links into the source.
	if !srcEntry.IsDir && sameObject(c.src, c.dst, f.relPath, link) {
		p.Failed.Add(1)
		transport.Log.Add("copy", transport.DirErr, "COPY "+f.relPath+": refused, source and destination are the same file")
		return
	}
	if err := c.clearTwin(f.relPath); err != nil {
		c.failed("COPY "+f.relPath, err)
		return
	}
	// From here the destination path may change, also when the copy then
	// fails half way: the rescan lists its folder again, and its cached
	// checksum goes (F12).
	c.markChanged(f.relPath)
	if srcEntry.IsDir {
		p.File.Store(f.relPath)
		p.beginFile(0)
		if err := c.dst.Mkdir(ctx, f.relPath, srcEntry.Mode); err != nil {
			c.failed("mkdir "+f.relPath, err)
			return
		}
		transport.Log.Add("copy", transport.DirIn, "mkdir "+f.relPath)
		return
	}
	if link {
		p.File.Store(f.relPath)
		p.beginFile(0)
		if err := c.linker.Symlink(ctx, target, f.relPath); err != nil {
			c.failed("COPY "+f.relPath, err)
			return
		}
		transport.Log.Add("copy", transport.DirIn, "COPY "+f.relPath+" OK (a symbolic link to "+target+")")
		return
	}
	slot := p.claimSlot(f.relPath, srcEntry.Size)
	defer p.releaseSlot(slot)
	slotBytes, slotBase := &slot.Bytes, &slot.BaseBytes
	p.File.Store(f.relPath)
	p.beginFile(srcEntry.Size)
	transport.Log.Add("copy", transport.DirOut, fmt.Sprintf("COPY %s (%s)", f.relPath, model.FormatSize(srcEntry.Size)))

	fileCtx := transport.ContextWithProgress(ctx, slotBytes)
	fileCtx = transport.ContextWithBaseProgress(fileCtx, slotBase)
	fileCtx = transport.ContextWithProgressBudget(fileCtx, srcEntry.Size)

	verify := resumeVerifier(c.req.VerifyResume, c.req.Scanner, c.src, c.dst, f.relPath, srcEntry.Size)
	// A copy into a part keeps its own note.
	fileCtx, modes := withModeNote(fileCtx)
	finish := func(how string) {
		c.countMode(f.relPath, modes)
		// A destination that keeps no times (restic) says so for every file.
		if hasTime(srcEntry) {
			if err := c.dst.SetTimes(fileCtx, f.relPath, srcEntry.ModTime, srcEntry.ATime.Time(), srcEntry.BirthTime.Time()); err != nil && !errors.Is(err, transport.ErrUnsupported) {
				p.TimesNotSet.Add(1)
				// After the user's x, no ERR line: the WARN line at the
				// end still counts the file.
				if !c.canceled() {
					transport.Log.Add("copy", transport.DirErr, "settimes "+f.relPath+": "+err.Error())
				}
			}
		}
		transport.Log.Add("copy", transport.DirIn, "COPY "+f.relPath+" OK"+how)
	}
	guarded := func(op func(context.Context) bool) (ok bool) {
		_ = transport.WithStallGuard(fileCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
			ok = op(attemptCtx)
			return nil
		})
		return ok
	}

	// A destination that writes part files takes the copy into one and
	// renames it into place (L14). Others write the file itself.
	pw, err := partWriter(fileCtx, c.dst)
	if err != nil {
		c.failed("COPY "+f.relPath, err)
		return
	}
	// Without parts, resume first when a partial dst body exists: append the
	// missing tail rather than overwrite the whole file (rsync's way, kept
	// by decision). tryResumeCopy self-gates (no-op for absent/full/oversized
	// dst); the appended prefix is never read back, so verify checks it
	// afterwards.
	if pw == nil && guarded(func(a context.Context) bool {
		return tryResumeCopy(a, c.src, c.dst, f.relPath, srcEntry, dstEntry, slotBytes, slotBase, verify)
	}) {
		finish(" (resumed)")
		return
	}
	if guarded(func(a context.Context) bool { return tryDirectTransfer(a, c.src, c.dst, f.relPath, srcEntry) }) {
		finish("")
		return
	}
	if pw != nil {
		resumed, err := c.copyIntoPart(fileCtx, f, pw, slotBytes, slotBase)
		if err != nil {
			c.failed("COPY "+f.relPath, err)
			return
		}
		how := ""
		if resumed {
			how = " (resumed)"
		}
		transport.Log.Add("copy", transport.DirIn, "COPY "+f.relPath+" OK"+how)
		return
	}

	attempt := 0
	err = transport.Retry(fileCtx, "copy", "copy "+f.relPath, func(tryCtx context.Context) error {
		return transport.WithStallGuard(tryCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
			attempt++
			if attempt > 1 && appends(attemptCtx, c.dst) {
				// A retry resumes whatever the failed attempt left behind.
				if offset := peekDstSize(attemptCtx, c.dst, f.relPath); offset > 0 && offset < srcEntry.Size {
					err := resumeAttempt(attemptCtx, c.src, c.dst, f.relPath, f.relPath, srcEntry.Mode, srcEntry, offset, slotBytes, slotBase, verify)
					if err == nil || (!errors.Is(err, transport.ErrUnsupported) && !errors.Is(err, errResumeMismatch)) {
						return err
					}
				}
			}
			return fullCopyAttempt(attemptCtx, c.src, c.dst, f.relPath, f.relPath, srcEntry.Mode, srcEntry, slotBytes)
		})
	})
	if err != nil {
		c.failed("COPY "+f.relPath, err)
		return
	}
	finish("")
}

// hasTime reports whether e has a time to give its copy. A listing may give
// a file none (an FTP or HTTP server), and its ModTime is zero then: set
// as it was, it made the copy's time year 1, as FTP's MFMT 00010101000000
// (L25). The copy keeps the time it was written instead.
func hasTime(e *model.FileEntry) bool { return !e.ModTime.IsZero() }

// logSkipped says why f, a link, device or part, is not copied. No skip is
// a failure: a mirror goes on.
func (c *copier) logSkipped(f item) {
	why := "sc copies files, folders and links only"
	switch {
	case f.src.IsPart():
		why = "what a cut-off copy left, never copied itself"
	case f.src.IsLink() && c.linker == nil:
		why = "the destination cannot hold links"
	case f.src.IsLink():
		why = "the source did not say where it leads"
	}
	transport.Log.Add("copy", transport.DirWarn, "COPY "+f.relPath+": skipped, a "+f.src.Kind()+" ("+why+")")
}

// mirrorDelete removes the entries only on the destination that the plan
// holds. It must only run once every copy landed: a partial copy plus a full
// delete pass would destroy data the source still holds. It deletes what the
// user was shown, and only while it is still as shown: it read what to
// delete from the tree after the copy, which by then held what the copy had
// listed as it ran, and deleted that too, unseen (D6). What the plan does not
// hold is kept, and the next copy asks about it.
func (c *copier) mirrorDelete() {
	failed := c.p.Failed.Load()
	switch {
	case !c.req.Plan.Mirror:
		return
	case c.stopped.Load():
		transport.Log.Add("copy", transport.DirErr, "mirror delete skipped: the copy stopped")
		return
	case c.ctx.Err() != nil:
		// The user's own x is no error.
		level := transport.DirErr
		if c.canceled() {
			level = transport.DirIn
		}
		transport.Log.Add("copy", level, "mirror delete skipped: copy canceled")
		return
	case failed > 0:
		transport.Log.Add("copy", transport.DirErr, fmt.Sprintf("mirror delete skipped: %d file(s) failed to copy", failed))
		return
	}
	// Each folder is listed again on both sides, and an entry is deleted
	// only while it is on the destination alone, as the plan has it; see
	// stillDestOnly.
	var dirs []string
	byDir := map[string][]Delete{}
	for _, d := range c.req.Plan.Deletes {
		dir := model.DirOf(d.RelPath)
		if _, ok := byDir[dir]; !ok {
			dirs = append(dirs, dir)
		}
		byDir[dir] = append(byDir[dir], d)
	}
	kept := 0 // still only on the destination: the next copy asks
	// A delete that failed, or that an error kept from running, counts as a
	// failure where the user sees it: the popup, the status bar (F17). One
	// that the user's x cut off, or kept from running, is no failure.
	delFailed := 0
	finished := false
	defer func() {
		if delFailed > 0 {
			c.p.Failed.Add(int64(delFailed))
		}
		switch {
		case !finished && c.canceled():
			// What failed before the cancel has its own ERR line.
			transport.Log.Add("copy", transport.DirIn, "mirror delete canceled")
		case delFailed > 0:
			transport.Log.Add("copy", transport.DirFail, fmt.Sprintf("mirror delete finished with %d failure(s) of %d", delFailed, len(c.req.Plan.Deletes)))
		}
	}()
	// A destination that removes a folder's entries in one round trip gets
	// each folder's in one call (R16).
	br, batches := capable[model.BatchRemover](c.ctx, c.dst, func(caps model.Caps) bool { return caps.BatchRemove })
	for _, dir := range dirs {
		if c.ctx.Err() != nil {
			return
		}
		keep, err := c.stillDestOnly(dir)
		if err != nil {
			if c.canceled() {
				return
			}
			delFailed += len(byDir[dir])
			transport.Log.Add("copy", transport.DirErr, "mirror delete in "+displayPath(dir)+" skipped: "+err.Error())
			continue
		}
		var victims []Delete
		for _, d := range byDir[dir] {
			if why, changed := keep(d); why != "" {
				transport.Log.Add("copy", transport.DirWarn, "mirror delete "+d.RelPath+" skipped: "+why)
				if changed {
					kept++
				}
				continue
			}
			victims = append(victims, d)
		}
		if batches && len(victims) > 1 {
			delFailed += c.removeIn(br, dir, victims)
			if c.canceled() {
				return
			}
			continue
		}
		for _, d := range victims {
			if c.ctx.Err() != nil {
				return
			}
			c.markChanged(d.RelPath)
			if err := c.remove(entry{relPath: d.RelPath, isDir: d.IsDir}); err != nil {
				if c.canceled() {
					return
				}
				delFailed++
				transport.Log.Add("copy", transport.DirErr, "mirror delete "+d.RelPath+": "+err.Error())
				continue
			}
			transport.Log.Add("copy", transport.DirIn, "mirror delete "+d.RelPath)
		}
	}
	finished = true
	for _, p := range c.unplannedDeletes() {
		transport.Log.Add("copy", transport.DirWarn, "mirror delete "+p+" skipped: the dialog did not show it")
		kept++
	}
	if kept > 0 {
		transport.Log.Add("copy", transport.DirErr, fmt.Sprintf("mirror delete: kept %d on %s that the dialog did not show as they are now: copy again to be asked", kept, dstSideName(c.req.LeftToRight)))
	}
}

// removeIn deletes victims, entries of dir, in one call to a destination
// that removes a folder's entries in one round trip (R16), and returns how
// many it could not. When the call fails, a listing of dir tells which are
// gone: deleting each again on its own would be the round trip per entry
// that the call saves. When that listing fails too, none counts as gone.
func (c *copier) removeIn(br model.BatchRemover, dir string, victims []Delete) (failed int) {
	items := make([]model.RemoveItem, len(victims))
	for i, d := range victims {
		c.markChanged(d.RelPath)
		items[i] = model.RemoveItem{Name: path.Base(d.RelPath), IsDir: d.IsDir}
	}
	err := br.RemoveIn(c.ctx, dir, items)
	if err != nil && c.canceled() {
		return 0 // the user's x cut the call off: the rescan shows what is gone
	}
	gone := map[string]bool{} // after a failed call, what dir no longer lists
	if err != nil {
		if list, lerr := c.dst.List(model.WithFreshListing(c.ctx), dir); lerr == nil {
			listed := make(map[string]bool, len(list))
			for _, e := range list {
				listed[e.Name] = true
			}
			for _, it := range items {
				gone[it.Name] = !listed[it.Name]
			}
		}
	}
	for _, d := range victims {
		if err != nil && !gone[path.Base(d.RelPath)] {
			failed++
			transport.Log.Add("copy", transport.DirErr, "mirror delete "+d.RelPath+": "+err.Error())
			continue
		}
		transport.Log.Add("copy", transport.DirIn, "mirror delete "+d.RelPath)
	}
	return failed
}

// unplannedDeletes are the entries only on the destination that the tree
// holds now and the plan does not: in a folder the copy listed as it ran, or
// found by a rescan after the dialog. A part file of a source file is none:
// the copy wrote into it, or left it for the next copy to ask about.
func (c *copier) unplannedDeletes() []string {
	planned := pathSet(c.req.Plan.Deletes, func(d Delete) string { return d.RelPath })
	src, dst := model.CopySides(c.req.LeftToRight)
	var extra []string
	c.req.Scanner.ReadTree(func(*model.TreeNode) {
		for _, d := range model.CollectMirrorDeletes(c.req.Node, c.req.LeftToRight) {
			// The part of a file the copy wrote is gone, and is no delete.
			if !planned[d.RelPath] && !partOfSourceFile(d, src, dst) {
				extra = append(extra, d.RelPath)
			}
		}
	})
	return extra
}

// changedSince says how the entry the destination lists now differs from
// the one the plan read, or "" when it is the same: the same type and, for a
// file, the same size and time to the second. What a folder holds is not
// listed again: a folder is deleted with what it holds now.
func changedSince(d Delete, now model.FileEntry) string {
	switch {
	case now.IsDir != d.IsDir:
		return "it is not the " + map[bool]string{true: "folder", false: "file"}[d.IsDir] + " the dialog showed"
	case d.IsDir:
		return ""
	case now.Size != d.Size || !now.ModTime.Truncate(time.Second).Equal(d.ModTime.Truncate(time.Second)):
		return "it changed since the dialog showed it"
	}
	return ""
}

// stillDestOnly lists dir on both sides now, after the copy, on the servers
// rather than from the scan's cached listings, and returns the check a
// mirror delete in dir must pass: "" to delete d, else why not, and whether
// d is kept because it changed since the plan read it (changedSince).
//
// Names are matched byte for byte, but a case-insensitive destination (macOS,
// Windows, SMB, FAT) stores photo.jpg in the existing Photo.JPG: the copy
// just wrote into the "destination-only" file. So a name that differs from a
// source name only in letter case is kept, unless the destination lists both
// names (then they are two files). And when a file copied into dir is not
// listed under its own name or a case variant of it, the destination renames
// what it stores (Unicode normalization, for one): nothing in dir is deleted.
func (c *copier) stillDestOnly(dir string) (func(d Delete) (why string, changed bool), error) {
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
	onDst := make(map[string]model.FileEntry, len(dstList))
	dstFold := make(map[string]bool, len(dstList))
	for _, e := range dstList {
		onDst[e.Name] = e
		dstFold[strings.ToLower(e.Name)] = true
	}
	c.mu.Lock()
	var renamed string
	for p := range c.dstChanged {
		name := path.Base(p)
		if _, listed := onDst[name]; model.DirOf(p) == dir && !listed && !dstFold[strings.ToLower(name)] {
			renamed = name
			break
		}
	}
	c.mu.Unlock()
	return func(d Delete) (string, bool) {
		name := path.Base(d.RelPath)
		now, listed := onDst[name]
		switch {
		case renamed != "":
			return "the destination does not list the copied " + renamed + " under that name, so names here cannot be matched", false
		case !listed:
			return "no longer on the destination", false
		case onSrc[name]:
			return "now on the source too", false
		}
		if twin, ok := srcFold[strings.ToLower(name)]; ok {
			if _, twinListed := onDst[twin]; !twinListed {
				return "on this destination it is the same file as " + twin + " (the names differ only in letter case)", false
			}
		}
		if why := changedSince(d, now); why != "" {
			return why, true
		}
		return "", false
	}, nil
}
