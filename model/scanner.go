package model

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LogFn routes scanner-level errors into the shared operation log. model can't
// import transport (transport imports model), so main wires it up.
var LogFn func(proto, direction, msg string)

// ListTimeout caps one directory listing on backends that do not manage their
// own liveness; zero disables the cap. Read when a Scanner is created.
var ListTimeout = 120 * time.Second

func logScanErr(err error) {
	if err == nil || LogFn == nil || errors.Is(err, context.Canceled) {
		return
	}
	LogFn("scan", "ERR", err.Error())
}

// Scan phases as shown in the status bar.
const (
	PhaseScanning     = "scanning..."
	PhaseChecksumming = "checksumming..."
	PhaseDone         = "done"
)

type ScanProgress struct {
	TotalFiles     int64
	TotalDirs      int64
	DirsListed     int64
	DirsTotal      int64
	FilesEqual     int64
	FilesDifferent int64
	FilesLeftOnly  int64
	FilesRightOnly int64
	ChecksumFiles  int64
	ChecksumDone   int64
	Phase          string
	LeftActive     bool
	RightActive    bool
}

// phaseProgress is the minimal progress for phase, both sides shown active
// until done.
func phaseProgress(phase string) ScanProgress {
	active := phase != PhaseDone
	return ScanProgress{Phase: phase, LeftActive: active, RightActive: active}
}

type Scanner struct {
	mu          sync.RWMutex // guards tree, cancel and the two backends
	left        Backend
	right       Backend
	tree        *TreeNode
	cancel      context.CancelFunc
	concurrency int
	listWidth   int
	listTimeout time.Duration
	maxDepth    int
	progress    atomic.Value
	rev         atomic.Uint64
	cksumOnce   sync.Once
	cksumMu     sync.RWMutex // guards the probe results below
	cksumOK     bool
	cksumAlgo   string
	cksumProbed bool
	cksumLeft   []string
	cksumRight  []string
}

// concurrency caps per-side checksum parallelism; listWidth caps how many
// directories are listed at once (both sides of one directory always go in
// parallel, so the backends see up to 2*listWidth concurrent List calls).
func NewScanner(left, right Backend, concurrency, listWidth int, deepScan bool) *Scanner {
	maxDepth := 0
	if !deepScan {
		maxDepth = 1
	}
	return &Scanner{
		left:        left,
		right:       right,
		concurrency: max(concurrency, 1),
		listWidth:   max(listWidth, 1),
		listTimeout: ListTimeout,
		maxDepth:    maxDepth,
	}
}

func (s *Scanner) MaxDepth() int { return s.maxDepth }

func (s *Scanner) Progress() ScanProgress {
	v := s.progress.Load()
	if v == nil {
		return ScanProgress{}
	}
	return v.(ScanProgress)
}

func (s *Scanner) setProgress(p ScanProgress) { s.progress.Store(p) }

func (s *Scanner) Tree() *TreeNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tree
}

// backends snapshots the two sides; SwapSides may exchange them at any time.
func (s *Scanner) backends() (left, right Backend) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.left, s.right
}

func (s *Scanner) backend(side Side) Backend {
	left, right := s.backends()
	if side == SideLeft {
		return left
	}
	return right
}

// Rev counts tree mutations. The UI compares it against the revision it last
// rendered to decide whether the O(tree) rollup walk has to run again; on a
// million-node tree that walk is far too expensive to repeat per frame.
func (s *Scanner) Rev() uint64 { return s.rev.Load() }

// lockTree guards a tree mutation; unlockTree bumps the revision counter on the
// way out. Every mutating section goes through this pair, so no change can slip
// past the UI's staleness check.
func (s *Scanner) lockTree() { s.mu.Lock() }

func (s *Scanner) unlockTree() {
	s.rev.Add(1)
	s.mu.Unlock()
}

// ReadTree runs fn with the tree pinned under the shared read lock, so scanner
// goroutines cannot mutate it while fn walks. Readers do not exclude each
// other, so fn may only write the rollup and guide fields that PropagateStatus
// and FlattenTree own; those belong to the single UI goroutine and the scanner
// never touches them. Not reentrant: fn must not call another Scanner method
// that locks.
func (s *Scanner) ReadTree(fn func(root *TreeNode)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.tree)
}

// MutateTree runs fn under the exclusive lock and bumps the revision, for the
// edits the UI makes itself (expand/collapse, side swap). Same reentrancy rule
// as ReadTree.
func (s *Scanner) MutateTree(fn func(root *TreeNode)) {
	s.lockTree()
	defer s.unlockTree()
	fn(s.tree)
}

// Cancel aborts the scan started by Scan.
func (s *Scanner) Cancel() {
	s.mu.RLock()
	cancel := s.cancel
	s.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

// Scan lists both trees from the root and, when opts.Checksum is set,
// checksums every file present on both sides.
func (s *Scanner) Scan(ctx context.Context, opts CompareOpts) {
	ctx, cancel := context.WithCancel(ctx)
	root := NewRootNode()
	s.lockTree()
	s.tree, s.cancel = root, cancel
	s.unlockTree()

	var preloadOnce sync.Once
	var stats struct {
		totalFiles, totalDirs                        atomic.Int64
		dirsListed, dirsTotal                        atomic.Int64
		filesEqual, filesDiff, filesLeft, filesRight atomic.Int64
		leftPending, rightPending                    atomic.Int64
	}
	stats.dirsTotal.Store(1)
	stats.leftPending.Store(1)
	stats.rightPending.Store(1)

	progress := func(phase string) ScanProgress {
		return ScanProgress{
			Phase:          phase,
			TotalFiles:     stats.totalFiles.Load(),
			TotalDirs:      stats.totalDirs.Load(),
			DirsListed:     stats.dirsListed.Load(),
			DirsTotal:      stats.dirsTotal.Load(),
			FilesEqual:     stats.filesEqual.Load(),
			FilesDifferent: stats.filesDiff.Load(),
			FilesLeftOnly:  stats.filesLeft.Load(),
			FilesRightOnly: stats.filesRight.Load(),
			LeftActive:     stats.leftPending.Load() > 0,
			RightActive:    stats.rightPending.Load() > 0,
		}
	}
	s.setProgress(progress(PhaseScanning))

	walkDirs(ctx, s.listWidth, []dirJob{{relDir: "", parent: root, depth: 1, listLeft: true, listRight: true}},
		func(job dirJob) []dirJob {
			leftEntries, rightEntries, listErr := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)
			if job.listLeft {
				stats.leftPending.Add(-1)
			}
			if job.listRight {
				stats.rightPending.Add(-1)
			}
			if listErr != nil {
				logScanErr(listErr)
				s.lockTree()
				job.parent.ListErr = true
				s.unlockTree()
				return nil
			}

			// Fresh nodes only: the parent has no children yet, so the merge
			// touches nothing a reader could see and may run unlocked.
			children := MergeChildren(job.parent, leftEntries, rightEntries, job.depth, opts, nil)
			s.lockTree()
			job.parent.Children = children
			job.parent.Listed = true
			job.parent.ListErr = false
			s.unlockTree()

			stats.dirsListed.Add(1)
			descend := s.maxDepth == 0 || job.depth+1 <= s.maxDepth
			for _, child := range children {
				if child.IsDir {
					stats.totalDirs.Add(1)
					stats.dirsTotal.Add(1)
					if descend {
						if child.Compare.Presence != PresenceRightOnly {
							stats.leftPending.Add(1)
						}
						if child.Compare.Presence != PresenceLeftOnly {
							stats.rightPending.Add(1)
						}
					}
					continue
				}
				stats.totalFiles.Add(1)
				switch child.Compare.Presence {
				case PresenceLeftOnly:
					stats.filesLeft.Add(1)
				case PresenceRightOnly:
					stats.filesRight.Add(1)
				case PresenceBoth:
					if child.Compare.Size == AttrEqual && child.Compare.ModTime == AttrEqual {
						stats.filesEqual.Add(1)
					} else {
						stats.filesDiff.Add(1)
					}
				}
			}

			var next []dirJob
			if descend {
				next = childJobs(children, job.depth+1, nil)
			}
			// After listing the root, kick off the recursive preload in the
			// background so the user sees the top-level entries first while
			// the deep listing fills the cache for subsequent dirs.
			if job.parent == root && s.maxDepth == 0 {
				preloadOnce.Do(func() { s.preloadRecursive(ctx, "", true, true) })
			}
			s.setProgress(progress(PhaseScanning))
			return next
		})

	if ctx.Err() != nil {
		return
	}
	if !opts.Checksum || !s.negotiateChecksum() {
		s.setProgress(progress(PhaseDone))
		return
	}

	groups := s.groupFiles(root, false)
	checksumTotal := countGroupFiles(groups)
	s.resetChecksumPhase(groups)
	var checksumDone atomic.Int64
	update := func() {
		p := progress(PhaseChecksumming)
		p.LeftActive, p.RightActive = true, true
		p.ChecksumFiles, p.ChecksumDone = checksumTotal, checksumDone.Load()
		s.setProgress(p)
	}
	update()
	s.runChecksumSides(ctx, groups, func() {
		checksumDone.Add(1)
		update()
	})

	p := progress(PhaseDone)
	p.ChecksumFiles, p.ChecksumDone = checksumTotal, checksumDone.Load()
	s.setProgress(p)
}

// childJobs queues the listable directories among children, in reverse so
// the LIFO queue walks them in display order. Outside depth, only subtrees
// touched by changed are queued.
func childJobs(children []*TreeNode, depth int, filter func(*TreeNode) bool) []dirJob {
	var next []dirJob
	for i := len(children) - 1; i >= 0; i-- {
		child := children[i]
		if !child.IsDir || (filter != nil && !filter(child)) {
			continue
		}
		if job, ok := listJob(child, depth); ok {
			next = append(next, job)
		}
	}
	return next
}

// listJob builds the job listing n on whichever sides hold a directory; false
// when neither does.
func listJob(n *TreeNode, depth int) (dirJob, bool) {
	l, r := n.Entries()
	listLeft := l != nil && l.IsDir && n.Compare.Presence != PresenceRightOnly
	listRight := r != nil && r.IsDir && n.Compare.Presence != PresenceLeftOnly
	if n.RelPath == "" {
		listLeft, listRight = true, true
	}
	if !listLeft && !listRight {
		return dirJob{}, false
	}
	return dirJob{relDir: n.RelPath, parent: n, depth: depth, listLeft: listLeft, listRight: listRight}, true
}

func countGroupFiles(groups []checksumGroup) int64 {
	var n int64
	for _, g := range groups {
		n += int64(len(g.files))
	}
	return n
}

func (s *Scanner) RescanNode(ctx context.Context, node *TreeNode, opts CompareOpts, changed *ChangedPaths) {
	if node.IsDir {
		s.rescanDir(ctx, node, opts, s.maxDepth, changed)
		return
	}
	s.rescanFile(ctx, node, opts, changed)
}

// DeepRescanNode rescans a node recursively regardless of the scanner's
// maxDepth setting. Used by the explicit "deep scan" key.
func (s *Scanner) DeepRescanNode(ctx context.Context, node *TreeNode, opts CompareOpts) {
	if node.IsDir {
		s.rescanDir(ctx, node, opts, 0, nil)
		return
	}
	s.rescanFile(ctx, node, opts, nil)
}

// RefreshTopLevel re-lists root's immediate children without descending.
// Reuses existing TreeNodes by (name, isDir) so already-listed subtrees keep
// their Children/Listed/Expanded state; new entries are added, gone entries
// dropped. Used by the "r" key so new top-level files appear regardless of
// where the cursor sits.
func (s *Scanner) RefreshTopLevel(ctx context.Context, opts CompareOpts) {
	root := s.Tree()
	if root == nil {
		return
	}
	s.setProgress(phaseProgress(PhaseScanning))
	defer s.setProgress(phaseProgress(PhaseDone))
	leftEntries, rightEntries, err := s.listBoth(ctx, "")
	if err != nil {
		logScanErr(err)
		s.lockTree()
		root.ListErr = true
		s.unlockTree()
		return
	}
	s.lockTree()
	root.Children = MergeChildren(root, leftEntries, rightEntries, root.Depth+1, opts, nil)
	root.Listed = true
	root.ListErr = false
	s.unlockTree()
}

// ListNode lists a single directory's immediate children without descending.
// Used by the lazy-expand-on-Enter UI path when a dir was left unlisted by an
// initial shallow scan.
func (s *Scanner) ListNode(ctx context.Context, node *TreeNode, opts CompareOpts) {
	if !node.IsDir {
		return
	}
	s.setProgress(phaseProgress(PhaseScanning))
	defer s.setProgress(phaseProgress(PhaseDone))
	job, ok := listJob(node, node.Depth+1)
	if !ok {
		return
	}
	leftEntries, rightEntries, err := s.listDir(ctx, node.RelPath, job.listLeft, job.listRight)
	if err != nil {
		logScanErr(err)
		s.lockTree()
		node.ListErr = true
		s.unlockTree()
		return
	}
	// The preserving merge reuses and rewrites the live child nodes, so it is a
	// mutation like the assignment that follows it; both go under the lock.
	s.lockTree()
	oldExpanded := make(map[string]bool)
	for _, child := range node.Children {
		collectExpanded(child, oldExpanded)
	}
	children := MergeChildren(node, leftEntries, rightEntries, node.Depth+1, opts, nil)
	restoreExpanded(children, oldExpanded)
	node.Children = children
	node.Listed = true
	node.ListErr = false
	s.unlockTree()
}

// refreshEntries re-reads node's own entries from its parent listing so its
// presence reflects the current state of both backends.
func (s *Scanner) refreshEntries(ctx context.Context, node *TreeNode, opts CompareOpts, changed *ChangedPaths) error {
	leftEntries, rightEntries, err := s.listBoth(ctx, DirOf(node.RelPath))
	if err != nil {
		return err
	}
	s.lockTree()
	node.Sides[SideLeft].Entry = findEntry(leftEntries, node.Name)
	node.Sides[SideRight].Entry = findEntry(rightEntries, node.Name)
	compareNode(node, opts)
	revalidateChecksum(node, changed)
	s.unlockTree()
	return nil
}

func findEntry(entries []FileEntry, name string) *FileEntry {
	for i := range entries {
		if entries[i].Name == name {
			return &entries[i]
		}
	}
	return nil
}

func (s *Scanner) rescanFile(ctx context.Context, node *TreeNode, opts CompareOpts, changed *ChangedPaths) {
	p := phaseProgress(PhaseScanning)
	s.setProgress(p)
	defer func() {
		p.Phase, p.LeftActive, p.RightActive = PhaseDone, false, false
		s.setProgress(p)
	}()
	if err := s.refreshEntries(ctx, node, opts, changed); err != nil {
		logScanErr(err)
		return
	}
	p.TotalFiles = 1
	s.setProgress(p)

	needCk := node.Compare.Presence == PresenceBoth && node.Compare.Checksum == AttrUnknown
	if needCk && !opts.Checksum && !node.oneSideSummed() {
		needCk = false
	}
	if needCk {
		p.Phase, p.ChecksumFiles = PhaseChecksumming, 1
		s.setProgress(p)
		s.checksumNode(ctx, node)
		p.ChecksumDone = 1
	}
}

func (s *Scanner) rescanDir(ctx context.Context, node *TreeNode, opts CompareOpts, depthLimit int, changed *ChangedPaths) {
	var preloadOnce sync.Once
	var dirsListed, totalFiles, ckDone atomic.Int64
	var ckTotal int64
	setp := func(phase string) {
		p := phaseProgress(phase)
		p.DirsListed, p.TotalFiles = dirsListed.Load(), totalFiles.Load()
		p.ChecksumFiles, p.ChecksumDone = ckTotal, ckDone.Load()
		s.setProgress(p)
	}
	setp(PhaseScanning)
	defer setp(PhaseDone)

	// The root node has no parent to list; every other node re-reads itself.
	if node.RelPath != "" {
		if err := s.refreshEntries(ctx, node, opts, nil); err != nil {
			logScanErr(err)
			return
		}
	}

	s.mu.RLock()
	oldExpanded := make(map[string]bool)
	for _, child := range node.Children {
		collectExpanded(child, oldExpanded)
	}
	seed, ok := listJob(node, node.Depth+1)
	s.mu.RUnlock()
	if !ok {
		return
	}

	walkDirs(ctx, s.listWidth, []dirJob{seed}, func(job dirJob) []dirJob {
		leftEntries, rightEntries, listErr := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)
		if listErr != nil {
			logScanErr(listErr)
			s.lockTree()
			job.parent.ListErr = true
			s.unlockTree()
			return nil
		}
		s.lockTree()
		children := MergeChildren(job.parent, leftEntries, rightEntries, job.depth, opts, changed)
		restoreExpanded(children, oldExpanded)
		job.parent.Children = children
		job.parent.Listed = true
		job.parent.ListErr = false
		s.unlockTree()

		dirsListed.Add(1)
		for _, child := range children {
			if !child.IsDir {
				totalFiles.Add(1)
			}
		}
		setp(PhaseScanning)

		var filter func(*TreeNode) bool
		if depthLimit != 0 && job.depth+1 > depthLimit {
			filter = func(n *TreeNode) bool { return changed.touchesSubtree(n.RelPath) }
		}
		next := childJobs(children, job.depth+1, filter)
		// After listing the rescan root, kick off the recursive preload in
		// the background so the user sees this dir's entries first while
		// the deep listing fills the cache for the rest of the subtree.
		if job.parent == node && depthLimit == 0 {
			preloadOnce.Do(func() { s.preloadRecursive(ctx, node.RelPath, seed.listLeft, seed.listRight) })
		}
		return next
	})

	if ctx.Err() != nil {
		return
	}
	groups := s.groupFiles(node, true)
	if !opts.Checksum {
		groups = filterPartialCRCGroups(groups)
	}
	if len(groups) == 0 || !s.negotiateChecksum() {
		return
	}
	ckTotal = countGroupFiles(groups)
	s.resetChecksumPhase(groups)
	setp(PhaseChecksumming)
	s.runChecksumSides(ctx, groups, func() {
		ckDone.Add(1)
		setp(PhaseChecksumming)
	})
}

func (s *Scanner) ChecksumNode(ctx context.Context, node *TreeNode) {
	if !s.negotiateChecksum() {
		return
	}
	var groups []checksumGroup
	if node.IsDir {
		groups = s.groupFiles(node, false)
	} else if node.Compare.Presence == PresenceBoth {
		groups = []checksumGroup{{files: []*TreeNode{node}}}
	}
	s.resetChecksumPhase(groups)

	base := s.Progress()
	base.Phase, base.ChecksumFiles = PhaseChecksumming, countGroupFiles(groups)
	base.LeftActive, base.RightActive = true, true
	var done atomic.Int64
	update := func() {
		p := base
		p.ChecksumDone = done.Load()
		s.setProgress(p)
	}
	update()
	s.runChecksumSides(ctx, groups, func() {
		done.Add(1)
		update()
	})
	base.Phase, base.ChecksumDone = PhaseDone, done.Load()
	base.LeftActive, base.RightActive = false, false
	s.setProgress(base)
}

func (s *Scanner) preloadRecursive(ctx context.Context, scope string, leftIsDir, rightIsDir bool) {
	left, right := s.backends()
	if p, ok := left.(RecursivePreloader); ok && leftIsDir {
		p.PreloadRecursive(ctx, scope)
	}
	if p, ok := right.(RecursivePreloader); ok && rightIsDir {
		p.PreloadRecursive(ctx, scope)
	}
}

func (s *Scanner) ListBothDir(ctx context.Context, relDir string) ([]FileEntry, []FileEntry, error) {
	e, r, err := s.listBoth(ctx, relDir)
	logScanErr(err)
	return e, r, err
}

// NegotiateChecksum picks a checksum algorithm both backends support and
// configures them to use it. Reports whether one was found.
func (s *Scanner) NegotiateChecksum() bool { return s.negotiateChecksum() }

// EnsureSubtreeListed lists every directory under node that the scan left
// unlisted (shallow scan, collapsed dir, earlier list failure). Copy and
// mirror-delete walk the in-memory tree only, so an unlisted dir silently
// reads as empty: the subtree is skipped and mirror under-counts deletes.
// Reports whether the whole subtree is now listed.
func (s *Scanner) EnsureSubtreeListed(ctx context.Context, node *TreeNode, opts CompareOpts) bool {
	if node == nil || !node.IsDir || s.subtreeListed(node) {
		return true
	}
	// Callers hold no lock; every read of the live tree below takes its own.
	s.mu.RLock()
	seed, ok := listJob(node, node.Depth+1)
	s.mu.RUnlock()
	if !ok {
		return true
	}
	s.preloadRecursive(ctx, node.RelPath, seed.listLeft, seed.listRight)

	var failed atomic.Bool
	walkDirs(ctx, s.listWidth, []dirJob{seed}, func(job dirJob) []dirJob {
		parent := job.parent
		s.mu.RLock()
		needList := !parent.Listed || parent.ListErr
		s.mu.RUnlock()
		if needList {
			left, right, err := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)
			if err != nil {
				logScanErr(err)
				failed.Store(true)
				s.lockTree()
				parent.ListErr = true
				s.unlockTree()
				return nil
			}
			s.lockTree()
			parent.Children = MergeChildren(parent, left, right, job.depth, opts, nil)
			parent.Listed = true
			parent.ListErr = false
			s.unlockTree()
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		return childJobs(parent.Children, job.depth+1, nil)
	})
	// Re-check rather than trust the walk: the caller is about to enumerate
	// this subtree for a destructive operation, and a dir still unlisted here
	// would enumerate as empty.
	return !failed.Load() && ctx.Err() == nil && s.subtreeListed(node)
}

func (s *Scanner) subtreeListed(node *TreeNode) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return UnlistedDir(node) == nil
}

// FindNearestDestNode walks up from relPath and returns the deepest tree node
// that has an entry on the copy destination side. Used to find the correct
// rescan root when intermediate directories are created during a copy.
func (s *Scanner) FindNearestDestNode(relPath string, leftToRight bool) *TreeNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.tree == nil {
		return nil
	}
	_, dst := CopySides(leftToRight)
	for path := relPath; path != ""; path = DirOf(path) {
		if n := findNode(s.tree, path); n != nil && n.Sides[dst].Entry != nil {
			return n
		}
	}
	return s.tree
}

func (s *Scanner) RefreshDir(parentDir string, left, right []FileEntry, opts CompareOpts) {
	s.lockTree()
	defer s.unlockTree()
	if s.tree == nil {
		return
	}
	parent := findNode(s.tree, parentDir)
	if parent == nil {
		return
	}
	parent.Children = MergeChildren(parent, left, right, parent.Depth+1, opts, nil)
	parent.Listed = true
}

func collectExpanded(node *TreeNode, m map[string]bool) {
	if node.Expanded {
		m[node.RelPath] = true
	}
	for _, child := range node.Children {
		collectExpanded(child, m)
	}
}

func restoreExpanded(nodes []*TreeNode, m map[string]bool) {
	for _, node := range nodes {
		if m[node.RelPath] {
			node.Expanded = true
		}
		restoreExpanded(node.Children, m)
	}
}

// findNode descends from root one path component at a time, so a lookup
// costs the siblings along the way rather than a walk of the whole tree.
func findNode(root *TreeNode, relPath string) *TreeNode {
	n := root
	for rest := relPath; rest != ""; {
		name, tail, more := strings.Cut(rest, "/")
		var next *TreeNode
		for _, c := range n.Children {
			if c.Name == name && (c.IsDir || !more) {
				next = c
				break
			}
		}
		if next == nil {
			return nil
		}
		n, rest = next, tail
	}
	return n
}

// RenameNode renames a node in place. It reports whether the rename collided
// with a sibling that got merged in, in which case the node was left unlisted
// and the caller must re-scan it.
func (s *Scanner) RenameNode(node *TreeNode, newName, newRel, oldRel string, opts CompareOpts) bool {
	s.lockTree()
	defer s.unlockTree()
	node.Name, node.RelPath = newName, newRel
	for s := range node.Sides {
		node.Sides[s].Entry = renamedEntry(node.Sides[s].Entry, newName, newRel)
	}
	updateDescendantPaths(node.Children, oldRel, newRel)
	parent := findNode(s.tree, DirOf(newRel))
	if parent == nil {
		return false
	}
	merged := mergeRenamedCollision(parent, node, opts)
	sortNodes(parent.Children)
	return merged
}

// renamedEntry returns a copy of e under its new name. Entries are never
// edited in place: readers keep entry pointers across unlocked stretches.
func renamedEntry(e *FileEntry, name, rel string) *FileEntry {
	if e == nil {
		return nil
	}
	c := *e
	c.Name, c.RelPath = name, rel
	return &c
}

// mergeRenamedCollision folds a sibling into node when a rename has made node
// share a name and type with an existing sibling that holds the side node is
// missing, e.g. renaming a right-only entry to match a left-only one. Without
// this the two would keep rendering as separate single-sided rows. The sibling
// is removed and node becomes PresenceBoth. A merged directory is dropped to
// unlisted, since its subtree must now reflect both sides; it reports true so
// the caller re-scans it.
func mergeRenamedCollision(parent, node *TreeNode, opts CompareOpts) bool {
	for i, sib := range parent.Children {
		if sib == node || sib.Name != node.Name || sib.IsDir != node.IsDir {
			continue
		}
		merged := false
		for s := range node.Sides {
			if node.Sides[s].Entry != nil || sib.Sides[s].Entry == nil {
				continue
			}
			node.Sides[s] = sib.Sides[s]
			merged = true
		}
		if !merged {
			continue
		}
		parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
		compareNode(node, opts)
		if node.IsDir {
			node.Listed = false
			node.Children = nil
		} else {
			node.Compare.Checksum = checksumStatus(node)
		}
		return true
	}
	return false
}

// SwapSides exchanges the left and right backends and their checksum probe results.
func (s *Scanner) SwapSides() {
	s.lockTree()
	s.left, s.right = s.right, s.left
	s.unlockTree()
	s.cksumMu.Lock()
	s.cksumLeft, s.cksumRight = s.cksumRight, s.cksumLeft
	s.cksumMu.Unlock()
}

func updateDescendantPaths(children []*TreeNode, oldPrefix, newPrefix string) {
	for _, child := range children {
		child.RelPath = newPrefix + child.RelPath[len(oldPrefix):]
		for s := range child.Sides {
			child.Sides[s].Entry = renamedEntry(child.Sides[s].Entry, child.Name, child.RelPath)
		}
		updateDescendantPaths(child.Children, oldPrefix, newPrefix)
	}
}

func (s *Scanner) listCtx(parent context.Context, b Backend) (context.Context, context.CancelFunc) {
	if lm, ok := b.(LivenessManaged); ok && lm.ManagesLiveness() || s.listTimeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, s.listTimeout)
}

func (s *Scanner) listBoth(ctx context.Context, relDir string) ([]FileEntry, []FileEntry, error) {
	return s.listDir(ctx, relDir, true, true)
}

func DirOf(relPath string) string {
	if i := strings.LastIndexByte(relPath, '/'); i >= 0 {
		return relPath[:i]
	}
	return ""
}

// checksumNode synchronously runs both sides for one node and combines.
// Used by rescanFile for single-file checksum.
func (s *Scanner) checksumNode(ctx context.Context, node *TreeNode) {
	s.resetChecksumPhase([]checksumGroup{{files: []*TreeNode{node}}})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.checksumSideFile(ctx, node, SideLeft) }()
	go func() { defer wg.Done(); s.checksumSideFile(ctx, node, SideRight) }()
	wg.Wait()
}

// resetChecksumPhase clears transient per-file done flags and (re)marks the
// per-dir pending flags so the UI shows ≈ on each top-level dir that still
// has work scheduled for that side.
func (s *Scanner) resetChecksumPhase(groups []checksumGroup) {
	s.lockTree()
	defer s.unlockTree()
	for _, g := range groups {
		for _, f := range g.files {
			for s := range f.Sides {
				f.Sides[s] = SideState{Entry: f.Sides[s].Entry}
			}
			f.ChecksumCountedDone = false
			f.Compare.Checksum = AttrScanning
		}
		if g.dir == nil || len(g.files) == 0 {
			continue
		}
		for s := range g.dir.Sides {
			if e := g.dir.Sides[s].Entry; e != nil && e.IsDir {
				g.dir.Sides[s].ChecksumPending = true
			}
		}
	}
}

// runChecksumSides launches two goroutines, one per side, that march through
// groups independently. Each side does its own prefetch (if its backend
// supports it) and then per-file Checksum calls, up to s.concurrency in
// parallel. The faster side pipelines ahead, so the spinner on the left panel
// can be on a different dir than the right panel.
func (s *Scanner) runChecksumSides(ctx context.Context, groups []checksumGroup, onPairDone func()) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.processChecksumSide(ctx, groups, SideLeft, onPairDone) }()
	go func() { defer wg.Done(); s.processChecksumSide(ctx, groups, SideRight, onPairDone) }()
	wg.Wait()
}

func (s *Scanner) processChecksumSide(ctx context.Context, groups []checksumGroup, side Side, onPairDone func()) {
	prefetcher, _ := s.backend(side).(ChecksumPrefetcher)
	sem := make(chan struct{}, s.concurrency)

	setDir := func(dir *TreeNode, active, pending bool) {
		if dir == nil {
			return
		}
		s.lockTree()
		st := &dir.Sides[side]
		st.ChecksumActive, st.ChecksumPending = active, pending
		s.unlockTree()
	}

	for _, g := range groups {
		if ctx.Err() != nil {
			return
		}
		if len(g.files) == 0 {
			continue
		}
		setDir(g.dir, true, true)
		if prefetcher != nil && g.dir != nil {
			if e := g.dir.Sides[side].Entry; e != nil && e.IsDir {
				_ = prefetcher.PrefetchChecksums(ctx, g.dir.RelPath, true)
			}
		}

		var wg sync.WaitGroup
		for _, f := range g.files {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(n *TreeNode) {
				defer wg.Done()
				defer func() { <-sem }()
				if s.checksumSideFile(ctx, n, side) && onPairDone != nil {
					onPairDone()
				}
			}(f)
		}
		wg.Wait()
		setDir(g.dir, false, false)
	}
}

func markChecksumInFlight(node *TreeNode, side Side, delta int32) {
	for p := node.Parent; p != nil; p = p.Parent {
		p.ChecksumInFlight[side].Add(delta)
	}
}

// checksumSideFile runs one side's Checksum for node and stores the result.
// Returns true if this call completed the pair (both sides done); the caller
// uses that to increment the file-count progress exactly once per pair.
func (s *Scanner) checksumSideFile(ctx context.Context, node *TreeNode, side Side) bool {
	entry := node.Sides[side].Entry
	var sum string
	var err error
	if entry != nil {
		markChecksumInFlight(node, side, 1)
		sum, err = s.backend(side).Checksum(ctx, node.RelPath)
		markChecksumInFlight(node, side, -1)
	}

	s.lockTree()
	defer s.unlockTree()
	st := &node.Sides[side]
	st.ChecksumDone = true
	switch {
	case entry == nil:
	case err != nil:
		st.ChecksumErr = true
	default:
		st.Checksum, st.ChecksumSize, st.ChecksumModTime = sum, entry.Size, entry.ModTime
	}
	l, r := &node.Sides[SideLeft], &node.Sides[SideRight]
	if !l.ChecksumDone || !r.ChecksumDone {
		return false
	}
	switch {
	case l.ChecksumErr || r.ChecksumErr:
		node.Compare.Checksum = AttrUnknown
	case l.Entry == nil || r.Entry == nil:
		node.Compare.Checksum = AttrNA
	default:
		node.Compare.Checksum = checksumStatus(node)
	}
	if node.ChecksumCountedDone {
		return false
	}
	node.ChecksumCountedDone = true
	return true
}

type dirJob struct {
	relDir    string
	parent    *TreeNode
	depth     int
	listLeft  bool
	listRight bool
}

// walkDirs drains a directory queue with up to workers goroutines. process
// lists one directory and returns the subdirectories to descend into; it runs
// concurrently on distinct parents, so anything shared it touches must be
// atomic or locked. Returns once the queue is empty and every worker is idle,
// or as soon as ctx is cancelled.
//
// Directory listing is latency-bound on every remote backend, and doing it one
// directory at a time meant a scan of N dirs cost N round trips end to end.
func walkDirs(ctx context.Context, workers int, seed []dirJob, process func(dirJob) []dirJob) {
	workers = max(workers, 1)
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	queue := append([]dirJob(nil), seed...)
	active := 0
	stop := false

	// Workers park on cond, not on ctx, so cancellation has to wake them.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			mu.Lock()
			stop = true
			mu.Unlock()
			cond.Broadcast()
		case <-done:
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				for len(queue) == 0 && active > 0 && !stop {
					cond.Wait()
				}
				if stop || len(queue) == 0 {
					mu.Unlock()
					cond.Broadcast() // nothing left to hand out: release the others
					return
				}
				job := queue[len(queue)-1]
				queue = queue[:len(queue)-1]
				active++
				mu.Unlock()

				next := process(job)

				mu.Lock()
				queue = append(queue, next...)
				active--
				mu.Unlock()
				cond.Broadcast()
			}
		}()
	}
	wg.Wait()
}

// listDir lists relDir on the requested sides. err is non-nil when any
// requested side failed; callers must then leave the tree alone rather than
// merge a partial result, which would show the other side's entries as
// one-sided and the failing side's subtree as empty.
func (s *Scanner) listDir(ctx context.Context, relDir string, listLeft, listRight bool) ([]FileEntry, []FileEntry, error) {
	if !listLeft && !listRight {
		return nil, nil, nil
	}
	left, right := s.backends()
	one := func(b Backend, side string) ([]FileEntry, error) {
		c, cancel := s.listCtx(ctx, b)
		defer cancel()
		e, err := b.List(c, relDir)
		return e, wrapListErr(side, relDir, err)
	}
	if !listLeft {
		e, err := one(right, "right")
		return nil, e, err
	}
	if !listRight {
		e, err := one(left, "left")
		return e, nil, err
	}
	type result struct {
		entries []FileEntry
		err     error
		isLeft  bool
	}
	ch := make(chan result, 2)
	go func() {
		e, err := one(left, "left")
		ch <- result{e, err, true}
	}()
	go func() {
		e, err := one(right, "right")
		ch <- result{e, err, false}
	}()
	var le, re []FileEntry
	var firstErr error
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return le, re, ctx.Err()
		case r := <-ch:
			if r.err != nil && firstErr == nil {
				firstErr = r.err
			}
			if r.isLeft {
				le = r.entries
			} else {
				re = r.entries
			}
		}
	}
	return le, re, firstErr
}

func wrapListErr(side, relDir string, err error) error {
	if err == nil {
		return nil
	}
	if relDir == "" {
		relDir = "/"
	}
	return fmt.Errorf("list %s %s: %w", side, relDir, err)
}

type checksumGroup struct {
	dir   *TreeNode
	files []*TreeNode
}

// groupFiles walks the live tree, so it holds the read lock: a scan or a copy
// may still be listing directories underneath.
func (s *Scanner) groupFiles(root *TreeNode, onlyPending bool) []checksumGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return groupFilesByTopLevel(root, onlyPending)
}

// groupFilesByTopLevel splits PresenceBoth files under root into groups,
// one per top-level child dir (plus one with dir=nil for files directly under
// root). The scanner processes each group independently so the rsync MD4
// daemon call is scoped per top-level dir rather than fired once on the whole
// base, which on big trees can take days for a single call.
//
// onlyPending=true skips files whose Compare.Checksum is already known
// (AttrEqual/AttrDifferent); rescan paths use it so cached CRC survives
// rescan unchanged. ChecksumNode and initial Scan pass false to enqueue all
// PresenceBoth files.
func groupFilesByTopLevel(root *TreeNode, onlyPending bool) []checksumGroup {
	var groups []checksumGroup
	var rootFiles []*TreeNode
	for _, child := range root.Children {
		if !child.IsDir {
			if child.Compare.Presence == PresenceBoth && needsChecksum(child, onlyPending) {
				rootFiles = append(rootFiles, child)
			}
			continue
		}
		var files []*TreeNode
		collectFiles(child, &files, onlyPending)
		if len(files) > 0 {
			groups = append(groups, checksumGroup{dir: child, files: files})
		}
	}
	if len(rootFiles) > 0 {
		groups = append([]checksumGroup{{files: rootFiles}}, groups...)
	}
	return groups
}

func collectFiles(node *TreeNode, files *[]*TreeNode, onlyPending bool) {
	if !node.IsDir && node.Compare.Presence == PresenceBoth && needsChecksum(node, onlyPending) {
		*files = append(*files, node)
	}
	for _, child := range node.Children {
		collectFiles(child, files, onlyPending)
	}
}

func needsChecksum(n *TreeNode, onlyPending bool) bool {
	return !onlyPending || n.Compare.Checksum == AttrUnknown
}

// filterPartialCRCGroups keeps only files with a sum on exactly one side: a
// rescan without checksumming still completes those pairs.
func filterPartialCRCGroups(groups []checksumGroup) []checksumGroup {
	var out []checksumGroup
	for _, g := range groups {
		var keep []*TreeNode
		for _, f := range g.files {
			if f.oneSideSummed() {
				keep = append(keep, f)
			}
		}
		if len(keep) > 0 {
			out = append(out, checksumGroup{dir: g.dir, files: keep})
		}
	}
	return out
}

// checksumPreference orders the algorithms both sides may share.
var checksumPreference = []string{"xxh3", "sha256", "sha1", "md5", "md4", "rsync"}

// negotiateChecksum probes both backends once. The UI polls the results every
// tick from its own goroutine, so they are published under cksumMu rather than
// left to the Once; only callers of Do get its happens-before.
func (s *Scanner) negotiateChecksum() bool {
	s.cksumOnce.Do(func() {
		left, right := s.backends()
		leftAlgos, rightAlgos := probeBackend(left), probeBackend(right)
		leftSet, rightSet := toSet(leftAlgos), toSet(rightAlgos)
		algo, ok := "", false
		for _, a := range checksumPreference {
			if leftSet[a] && rightSet[a] {
				setBackendAlgo(left, a)
				setBackendAlgo(right, a)
				algo, ok = a, true
				break
			}
		}
		s.cksumMu.Lock()
		s.cksumProbed = true
		s.cksumLeft, s.cksumRight = leftAlgos, rightAlgos
		s.cksumAlgo, s.cksumOK = algo, ok
		s.cksumMu.Unlock()
	})
	s.cksumMu.RLock()
	defer s.cksumMu.RUnlock()
	return s.cksumOK
}

func (s *Scanner) ChecksumAlgo() string {
	s.cksumMu.RLock()
	defer s.cksumMu.RUnlock()
	return s.cksumAlgo
}

func (s *Scanner) ChecksumProbed() bool {
	s.cksumMu.RLock()
	defer s.cksumMu.RUnlock()
	return s.cksumProbed
}

func (s *Scanner) ChecksumInfo() (left, right []string) {
	s.cksumMu.RLock()
	defer s.cksumMu.RUnlock()
	return s.cksumLeft, s.cksumRight
}

func probeBackend(b Backend) []string {
	if p, ok := b.(ChecksumProber); ok {
		return p.ProbeChecksums()
	}
	return nil
}

func setBackendAlgo(b Backend, algo string) {
	if p, ok := b.(ChecksumProber); ok {
		p.SetChecksumAlgo(algo)
	}
}

func toSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}
