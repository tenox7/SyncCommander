package model

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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

type Scanner struct {
	left         Backend
	right        Backend
	concurrency  int
	stallTimeout time.Duration
	maxDepth     int
	progress     atomic.Value
	tree         *TreeNode
	mu           sync.Mutex
	cancel       context.CancelFunc
	cksumOnce    sync.Once
	cksumOK      bool
	cksumAlgo    string
	cksumProbed  bool
	cksumLeft    []string
	cksumRight   []string
}

func NewScanner(left, right Backend, concurrency int, deepScan bool) *Scanner {
	if concurrency < 1 {
		concurrency = 4
	}
	maxDepth := 0
	if !deepScan {
		maxDepth = 1
	}
	return &Scanner{
		left:         left,
		right:        right,
		concurrency:  concurrency,
		stallTimeout: 120 * time.Second,
		maxDepth:     maxDepth,
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

func (s *Scanner) Tree() *TreeNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree
}

func (s *Scanner) Cancel() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Scanner) Scan(ctx context.Context, withChecksum bool, subSecond, timeGrace, ignoreTZDST bool) {
	ctx, s.cancel = context.WithCancel(ctx)

	root := NewRootNode()
	s.mu.Lock()
	s.tree = root
	s.mu.Unlock()

	preloadFired := false

	var stats struct {
		totalFiles, totalDirs                        atomic.Int64
		dirsListed, dirsTotal                        atomic.Int64
		filesEqual, filesDiff, filesLeft, filesRight atomic.Int64
	}

	queue := []dirJob{{relDir: "", parent: root, depth: 1, listLeft: true, listRight: true}}
	stats.dirsTotal.Store(1)
	leftPending := 1
	rightPending := 1

	s.setProgress(ScanProgress{Phase: "scanning...", DirsTotal: 1, LeftActive: true, RightActive: true})

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
			LeftActive:     leftPending > 0,
			RightActive:    rightPending > 0,
		}
	}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return
		}

		job := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		s.setProgress(progress("scanning..."))

		leftEntries, rightEntries := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)

		if job.listLeft {
			leftPending--
		}
		if job.listRight {
			rightPending--
		}

		children := MergeChildren(job.parent, leftEntries, rightEntries, job.depth, subSecond, timeGrace, ignoreTZDST)

		s.mu.Lock()
		job.parent.Children = children
		job.parent.Listed = true
		s.mu.Unlock()

		stats.dirsListed.Add(1)
		descend := s.maxDepth == 0 || job.depth+1 <= s.maxDepth
		for _, child := range children {
			if child.IsDir {
				stats.totalDirs.Add(1)
				stats.dirsTotal.Add(1)
				if descend {
					if child.Compare.Presence != PresenceRightOnly {
						leftPending++
					}
					if child.Compare.Presence != PresenceLeftOnly {
						rightPending++
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

		if descend {
			for i := len(children) - 1; i >= 0; i-- {
				child := children[i]
				if !child.IsDir {
					continue
				}
				leftIsDir := child.Left != nil && child.Left.IsDir
				rightIsDir := child.Right != nil && child.Right.IsDir
				listLeft := leftIsDir && child.Compare.Presence != PresenceRightOnly
				listRight := rightIsDir && child.Compare.Presence != PresenceLeftOnly
				if !listLeft && !listRight {
					continue
				}
				queue = append(queue, dirJob{
					relDir:    child.RelPath,
					parent:    child,
					depth:     job.depth + 1,
					listLeft:  listLeft,
					listRight: listRight,
				})
			}
		}

		// After listing the root, kick off the recursive preload in the
		// background so the user sees the top-level entries first while
		// the deep listing fills the cache for subsequent dirs.
		if !preloadFired && s.maxDepth == 0 {
			s.preloadRecursive(ctx, "", true, true)
			preloadFired = true
		}

		s.setProgress(progress("scanning..."))
	}

	if !withChecksum || !s.negotiateChecksum() {
		p := progress("done")
		s.setProgress(p)
		return
	}

	groups := groupFilesByTopLevel(root, false)
	var checksumTotal int64
	for _, g := range groups {
		checksumTotal += int64(len(g.files))
	}
	s.resetChecksumPhase(groups)
	var checksumDone atomic.Int64

	p := progress("checksumming...")
	p.LeftActive = true
	p.RightActive = true
	p.ChecksumFiles = checksumTotal
	s.setProgress(p)

	onCount := func() {
		done := checksumDone.Add(1)
		p := progress("checksumming...")
		p.LeftActive = true
		p.RightActive = true
		p.ChecksumFiles = checksumTotal
		p.ChecksumDone = done
		s.setProgress(p)
	}
	s.runChecksumSides(ctx, groups, onCount)

	p = progress("done")
	p.ChecksumFiles = checksumTotal
	p.ChecksumDone = checksumDone.Load()
	s.setProgress(p)
}

func (s *Scanner) RescanNode(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace, ignoreTZDST bool, changed *ChangedPaths) {
	if node.IsDir {
		s.rescanDir(ctx, node, withChecksum, subSecond, timeGrace, ignoreTZDST, s.maxDepth, changed)
		return
	}
	s.rescanFile(ctx, node, withChecksum, subSecond, timeGrace, ignoreTZDST, changed)
}

// DeepRescanNode rescans a node recursively regardless of the scanner's
// maxDepth setting. Used by the explicit "deep scan" key.
func (s *Scanner) DeepRescanNode(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace, ignoreTZDST bool) {
	if node.IsDir {
		s.rescanDir(ctx, node, withChecksum, subSecond, timeGrace, ignoreTZDST, 0, nil)
		return
	}
	s.rescanFile(ctx, node, withChecksum, subSecond, timeGrace, ignoreTZDST, nil)
}

// RefreshTopLevel re-lists root's immediate children without descending.
// Reuses existing TreeNodes by (name, isDir) so already-listed subtrees keep
// their Children/Listed/Expanded state; new entries are added, gone entries
// dropped. Used by the "r" key so new top-level files appear regardless of
// where the cursor sits.
func (s *Scanner) RefreshTopLevel(ctx context.Context, subSecond, timeGrace, ignoreTZDST bool) {
	root := s.Tree()
	if root == nil {
		return
	}
	setp := func(phase string) {
		s.setProgress(ScanProgress{
			Phase:       phase,
			LeftActive:  phase != "done",
			RightActive: phase != "done",
		})
	}
	setp("scanning...")
	leftEntries, rightEntries := s.listBoth(ctx, "")

	s.mu.Lock()
	root.Children = mergeChildrenPreserving(root, leftEntries, rightEntries, root.Depth+1, subSecond, timeGrace, ignoreTZDST)
	root.Listed = true
	s.mu.Unlock()
	setp("done")
}

// ListNode lists a single directory's immediate children without descending.
// Used by the lazy-expand-on-Enter UI path when a dir was left unlisted by an
// initial shallow scan.
func (s *Scanner) ListNode(ctx context.Context, node *TreeNode, subSecond, timeGrace, ignoreTZDST bool) {
	if !node.IsDir {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	setp := func(phase string) {
		s.setProgress(ScanProgress{
			Phase:       phase,
			LeftActive:  phase != "done",
			RightActive: phase != "done",
		})
	}
	setp("scanning...")

	leftIsDir := node.Left != nil && node.Left.IsDir
	rightIsDir := node.Right != nil && node.Right.IsDir
	listLeft := leftIsDir && node.Compare.Presence != PresenceRightOnly
	listRight := rightIsDir && node.Compare.Presence != PresenceLeftOnly
	leftEntries, rightEntries := s.listDir(ctx, node.RelPath, listLeft, listRight)

	oldExpanded := make(map[string]bool)
	for _, child := range node.Children {
		collectExpanded(child, oldExpanded)
	}
	children := mergeChildrenPreserving(node, leftEntries, rightEntries, node.Depth+1, subSecond, timeGrace, ignoreTZDST)
	restoreExpanded(children, oldExpanded)

	s.mu.Lock()
	node.Children = children
	node.Listed = true
	s.mu.Unlock()
	setp("done")
}

func (s *Scanner) rescanFile(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace, ignoreTZDST bool, changed *ChangedPaths) {
	var totalFiles, ckFiles, ckDone int64
	setp := func(phase string) {
		s.setProgress(ScanProgress{
			Phase:         phase,
			TotalFiles:    totalFiles,
			ChecksumFiles: ckFiles,
			ChecksumDone:  ckDone,
			LeftActive:    phase != "done",
			RightActive:   phase != "done",
		})
	}
	setp("scanning...")

	leftEntries, rightEntries := s.listBoth(ctx, DirOf(node.RelPath))

	var le, re *FileEntry
	for i := range leftEntries {
		if leftEntries[i].Name == node.Name {
			le = &leftEntries[i]
			break
		}
	}
	for i := range rightEntries {
		if rightEntries[i].Name == node.Name {
			re = &rightEntries[i]
			break
		}
	}

	s.mu.Lock()
	node.Left = le
	node.Right = re
	compareNode(node, subSecond, timeGrace, ignoreTZDST)
	revalidateChecksum(node, changed)
	s.mu.Unlock()

	totalFiles = 1
	setp("scanning...")

	if withChecksum && node.Compare.Presence == PresenceBoth && node.Compare.Checksum == AttrUnknown {
		ckFiles = 1
		setp("checksumming...")
		s.checksumNode(ctx, node)
		ckDone = 1
	}
	setp("done")
}

func (s *Scanner) rescanDir(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace, ignoreTZDST bool, depthLimit int, changed *ChangedPaths) {
	preloadFired := false
	var dirsListed, totalFiles, ckTotal int64
	var ckDone atomic.Int64
	setp := func(phase string) {
		s.setProgress(ScanProgress{
			Phase:         phase,
			DirsListed:    dirsListed,
			TotalFiles:    totalFiles,
			ChecksumFiles: ckTotal,
			ChecksumDone:  ckDone.Load(),
			LeftActive:    phase != "done",
			RightActive:   phase != "done",
		})
	}
	setp("scanning...")

	// Update the node's own Left/Right entries so its presence reflects current
	// state on both backends. Skip for the root node which has no parent to list.
	if node.RelPath != "" {
		leftEntries, rightEntries := s.listBoth(ctx, DirOf(node.RelPath))
		var le, re *FileEntry
		for i := range leftEntries {
			if leftEntries[i].Name == node.Name {
				le = &leftEntries[i]
				break
			}
		}
		for i := range rightEntries {
			if rightEntries[i].Name == node.Name {
				re = &rightEntries[i]
				break
			}
		}
		s.mu.Lock()
		node.Left = le
		node.Right = re
		compareNode(node, subSecond, timeGrace, ignoreTZDST)
		s.mu.Unlock()
	}

	oldExpanded := make(map[string]bool)
	for _, child := range node.Children {
		collectExpanded(child, oldExpanded)
	}

	rootListLeft := true
	rootListRight := true
	if node.RelPath != "" {
		rootListLeft = node.Left != nil && node.Left.IsDir
		rootListRight = node.Right != nil && node.Right.IsDir
	}
	queue := []dirJob{{relDir: node.RelPath, parent: node, depth: node.Depth + 1, listLeft: rootListLeft, listRight: rootListRight}}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return
		}
		job := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		leftEntries, rightEntries := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)
		children := mergeChildrenPreservingWithChanged(job.parent, leftEntries, rightEntries, job.depth, subSecond, timeGrace, ignoreTZDST, changed)
		restoreExpanded(children, oldExpanded)

		s.mu.Lock()
		job.parent.Children = children
		job.parent.Listed = true
		s.mu.Unlock()

		dirsListed++
		for _, child := range children {
			if !child.IsDir {
				totalFiles++
			}
		}
		setp("scanning...")

		descend := depthLimit == 0 || job.depth+1 <= depthLimit
		if descend {
			for i := len(children) - 1; i >= 0; i-- {
				child := children[i]
				if !child.IsDir {
					continue
				}
				leftIsDir := child.Left != nil && child.Left.IsDir
				rightIsDir := child.Right != nil && child.Right.IsDir
				listLeft := leftIsDir && child.Compare.Presence != PresenceRightOnly
				listRight := rightIsDir && child.Compare.Presence != PresenceLeftOnly
				if !listLeft && !listRight {
					continue
				}
				queue = append(queue, dirJob{
					relDir:    child.RelPath,
					parent:    child,
					depth:     job.depth + 1,
					listLeft:  listLeft,
					listRight: listRight,
				})
			}
		}

		// After listing the rescan root, kick off the recursive preload in
		// the background so the user sees this dir's entries first while
		// the deep listing fills the cache for the rest of the subtree.
		if !preloadFired && depthLimit == 0 {
			s.preloadRecursive(ctx, node.RelPath, rootListLeft, rootListRight)
			preloadFired = true
		}
	}

	if !withChecksum || !s.negotiateChecksum() {
		setp("done")
		return
	}
	groups := groupFilesByTopLevel(node, true)
	if len(groups) == 0 {
		setp("done")
		return
	}
	for _, g := range groups {
		ckTotal += int64(len(g.files))
	}
	s.resetChecksumPhase(groups)
	setp("checksumming...")
	s.runChecksumSides(ctx, groups, func() {
		ckDone.Add(1)
		setp("checksumming...")
	})
	setp("done")
}

func (s *Scanner) ChecksumNode(ctx context.Context, node *TreeNode) {
	if !s.negotiateChecksum() {
		return
	}
	var groups []checksumGroup
	if node.IsDir {
		groups = groupFilesByTopLevel(node, false)
	} else if node.Compare.Presence == PresenceBoth {
		groups = []checksumGroup{{dir: nil, files: []*TreeNode{node}}}
	}

	var total int64
	for _, g := range groups {
		total += int64(len(g.files))
	}
	s.resetChecksumPhase(groups)

	var done atomic.Int64
	update := func() {
		p := s.Progress()
		p.Phase = "checksumming..."
		p.ChecksumFiles = total
		p.ChecksumDone = done.Load()
		p.LeftActive = true
		p.RightActive = true
		s.setProgress(p)
	}
	update()

	s.runChecksumSides(ctx, groups, func() {
		done.Add(1)
		update()
	})

	p := s.Progress()
	p.Phase = "done"
	p.LeftActive = false
	p.RightActive = false
	s.setProgress(p)
}

func (s *Scanner) preloadRecursive(ctx context.Context, scope string, leftIsDir, rightIsDir bool) {
	if leftIsDir {
		if p, ok := s.left.(RecursivePreloader); ok {
			p.PreloadRecursive(ctx, scope)
		}
	}
	if rightIsDir {
		if p, ok := s.right.(RecursivePreloader); ok {
			p.PreloadRecursive(ctx, scope)
		}
	}
}

func (s *Scanner) ListBothDir(ctx context.Context, relDir string) ([]FileEntry, []FileEntry) {
	return s.listBoth(ctx, relDir)
}

// FindNearestDestNode walks up from relPath and returns the deepest tree node
// that has an entry on the copy destination side. Used to find the correct
// rescan root when intermediate directories are created during a copy.
func (s *Scanner) FindNearestDestNode(relPath string, leftToRight bool) *TreeNode {
	tree := s.Tree()
	if tree == nil {
		return nil
	}
	for path := relPath; path != ""; path = DirOf(path) {
		n := findNode(tree, path)
		if n != nil {
			if (leftToRight && n.Right != nil) || (!leftToRight && n.Left != nil) {
				return n
			}
		}
	}
	return tree // root as fallback
}

func (s *Scanner) RefreshDir(parentDir string, left, right []FileEntry, subSecond, timeGrace, ignoreTZDST bool) {
	tree := s.Tree()
	if tree == nil {
		return
	}
	parent := findNode(tree, parentDir)
	if parent == nil {
		return
	}
	children := mergeChildrenPreserving(parent, left, right, parent.Depth+1, subSecond, timeGrace, ignoreTZDST)
	s.mu.Lock()
	parent.Children = children
	parent.Listed = true
	s.mu.Unlock()
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

func findNode(root *TreeNode, relPath string) *TreeNode {
	if relPath == "" {
		return root
	}
	for _, child := range root.Children {
		if child.RelPath == relPath {
			return child
		}
		if child.IsDir {
			if found := findNode(child, relPath); found != nil {
				return found
			}
		}
	}
	return nil
}

func (s *Scanner) RenameNode(node *TreeNode, newName, newRel, oldRel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node.Name = newName
	node.RelPath = newRel
	if node.Left != nil {
		node.Left.Name = newName
		node.Left.RelPath = newRel
	}
	if node.Right != nil {
		node.Right.Name = newName
		node.Right.RelPath = newRel
	}
	updateDescendantPaths(node.Children, oldRel, newRel)
	parent := findNode(s.tree, DirOf(newRel))
	if parent != nil {
		sort.Slice(parent.Children, func(i, j int) bool {
			a, b := parent.Children[i], parent.Children[j]
			if a.IsDir != b.IsDir {
				return a.IsDir
			}
			return a.Name < b.Name
		})
	}
}

// SwapSides exchanges the left and right backends and their checksum probe results.
func (s *Scanner) SwapSides() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.left, s.right = s.right, s.left
	s.cksumLeft, s.cksumRight = s.cksumRight, s.cksumLeft
}

func updateDescendantPaths(children []*TreeNode, oldPrefix, newPrefix string) {
	for _, child := range children {
		child.RelPath = newPrefix + child.RelPath[len(oldPrefix):]
		if child.Left != nil {
			child.Left.RelPath = newPrefix + child.Left.RelPath[len(oldPrefix):]
		}
		if child.Right != nil {
			child.Right.RelPath = newPrefix + child.Right.RelPath[len(oldPrefix):]
		}
		updateDescendantPaths(child.Children, oldPrefix, newPrefix)
	}
}

func (s *Scanner) listBoth(ctx context.Context, relDir string) ([]FileEntry, []FileEntry) {
	ctx, cancel := context.WithTimeout(ctx, s.stallTimeout)
	defer cancel()
	type result struct {
		entries []FileEntry
		side    int
	}
	ch := make(chan result, 2)
	go func() {
		e, _ := s.left.List(ctx, relDir)
		ch <- result{e, 0}
	}()
	go func() {
		e, _ := s.right.List(ctx, relDir)
		ch <- result{e, 1}
	}()
	var left, right []FileEntry
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return left, right
		case r := <-ch:
			if r.side == 0 {
				left = r.entries
			} else {
				right = r.entries
			}
		}
	}
	return left, right
}

func DirOf(relPath string) string {
	for i := len(relPath) - 1; i >= 0; i-- {
		if relPath[i] == '/' {
			return relPath[:i]
		}
	}
	return ""
}

// checksumNode synchronously runs both sides for one node and combines.
// Used by rescanFile for single-file checksum.
func (s *Scanner) checksumNode(ctx context.Context, node *TreeNode) {
	s.resetChecksumPhase([]checksumGroup{{dir: nil, files: []*TreeNode{node}}})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.checksumSideFile(ctx, node, true) }()
	go func() { defer wg.Done(); s.checksumSideFile(ctx, node, false) }()
	wg.Wait()
}

// resetChecksumPhase clears transient per-file done flags and (re)marks the
// per-dir pending flags so the UI shows ≈ on each top-level dir that still
// has work scheduled for that side.
func (s *Scanner) resetChecksumPhase(groups []checksumGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range groups {
		for _, f := range g.files {
			f.LeftChecksum = ""
			f.RightChecksum = ""
			f.LeftCksumSize = 0
			f.LeftCksumModTime = time.Time{}
			f.RightCksumSize = 0
			f.RightCksumModTime = time.Time{}
			f.LeftChecksumDone = false
			f.RightChecksumDone = false
			f.LeftChecksumErr = false
			f.RightChecksumErr = false
			f.ChecksumCountedDone = false
			f.Compare.Checksum = AttrScanning
		}
		if g.dir == nil || len(g.files) == 0 {
			continue
		}
		if g.dir.Left != nil && g.dir.Left.IsDir {
			g.dir.ChecksumPendingLeft = true
		}
		if g.dir.Right != nil && g.dir.Right.IsDir {
			g.dir.ChecksumPendingRight = true
		}
	}
}

// runChecksumSides launches two goroutines — one per side — that march
// through groups independently. Each side does its own prefetch (if its
// backend supports it) and then per-file Checksum calls, up to s.concurrency
// in parallel. The faster side pipelines ahead, so the spinner on the left
// panel can be on a different dir than the right panel.
func (s *Scanner) runChecksumSides(ctx context.Context, groups []checksumGroup, onPairDone func()) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.processChecksumSide(ctx, groups, true, onPairDone) }()
	go func() { defer wg.Done(); s.processChecksumSide(ctx, groups, false, onPairDone) }()
	wg.Wait()
}

func (s *Scanner) processChecksumSide(ctx context.Context, groups []checksumGroup, isLeft bool, onPairDone func()) {
	backend := s.right
	if isLeft {
		backend = s.left
	}
	prefetcher, _ := backend.(ChecksumPrefetcher)
	sem := make(chan struct{}, s.concurrency)

	for _, g := range groups {
		if ctx.Err() != nil {
			return
		}
		if len(g.files) == 0 {
			continue
		}
		if g.dir != nil {
			s.mu.Lock()
			if isLeft {
				g.dir.ChecksumActiveLeft = true
			} else {
				g.dir.ChecksumActiveRight = true
			}
			s.mu.Unlock()
		}

		if prefetcher != nil && g.dir != nil {
			sideEntry := g.dir.Right
			if isLeft {
				sideEntry = g.dir.Left
			}
			if sideEntry != nil && sideEntry.IsDir {
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
				justCompletedPair := s.checksumSideFile(ctx, n, isLeft)
				if justCompletedPair && onPairDone != nil {
					onPairDone()
				}
			}(f)
		}
		wg.Wait()

		if g.dir != nil {
			s.mu.Lock()
			if isLeft {
				g.dir.ChecksumActiveLeft = false
				g.dir.ChecksumPendingLeft = false
			} else {
				g.dir.ChecksumActiveRight = false
				g.dir.ChecksumPendingRight = false
			}
			s.mu.Unlock()
		}
	}
}

func markChecksumInFlight(node *TreeNode, isLeft bool, delta int32) {
	for p := node.Parent; p != nil; p = p.Parent {
		if isLeft {
			atomic.AddInt32(&p.ChecksumInFlightLeft, delta)
		} else {
			atomic.AddInt32(&p.ChecksumInFlightRight, delta)
		}
	}
}

// checksumSideFile runs one side's Checksum for node and stores the result.
// Returns true if this call completed the pair (both sides done) — caller
// uses that to increment the file-count progress exactly once per pair.
func (s *Scanner) checksumSideFile(ctx context.Context, node *TreeNode, isLeft bool) bool {
	entry := node.Right
	backend := s.right
	if isLeft {
		entry = node.Left
		backend = s.left
	}
	var sum string
	var err error
	if entry != nil {
		markChecksumInFlight(node, isLeft, 1)
		sum, err = backend.Checksum(ctx, node.RelPath)
		markChecksumInFlight(node, isLeft, -1)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if isLeft {
		if entry == nil {
			// nothing to do
		} else if err != nil {
			node.LeftChecksumErr = true
		} else {
			node.LeftChecksum = sum
			node.LeftCksumSize = entry.Size
			node.LeftCksumModTime = entry.ModTime
		}
		node.LeftChecksumDone = true
	} else {
		if entry == nil {
			// nothing to do
		} else if err != nil {
			node.RightChecksumErr = true
		} else {
			node.RightChecksum = sum
			node.RightCksumSize = entry.Size
			node.RightCksumModTime = entry.ModTime
		}
		node.RightChecksumDone = true
	}
	if !node.LeftChecksumDone || !node.RightChecksumDone {
		return false
	}
	switch {
	case node.LeftChecksumErr || node.RightChecksumErr:
		node.Compare.Checksum = AttrUnknown
	case node.Left == nil || node.Right == nil:
		node.Compare.Checksum = AttrNA
	case node.LeftChecksum == node.RightChecksum:
		node.Compare.Checksum = AttrEqual
	default:
		node.Compare.Checksum = AttrDifferent
	}
	if node.ChecksumCountedDone {
		return false
	}
	node.ChecksumCountedDone = true
	return true
}

func (s *Scanner) setProgress(p ScanProgress) {
	s.progress.Store(p)
}

type dirJob struct {
	relDir    string
	parent    *TreeNode
	depth     int
	listLeft  bool
	listRight bool
}

func (s *Scanner) listDir(ctx context.Context, relDir string, listLeft, listRight bool) ([]FileEntry, []FileEntry) {
	if !listLeft && !listRight {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.stallTimeout)
	defer cancel()
	if !listLeft {
		e, _ := s.right.List(ctx, relDir)
		return nil, e
	}
	if !listRight {
		e, _ := s.left.List(ctx, relDir)
		return e, nil
	}
	type result struct {
		entries []FileEntry
		side    int
	}
	ch := make(chan result, 2)
	go func() {
		e, _ := s.left.List(ctx, relDir)
		ch <- result{e, 0}
	}()
	go func() {
		e, _ := s.right.List(ctx, relDir)
		ch <- result{e, 1}
	}()
	var left, right []FileEntry
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return left, right
		case r := <-ch:
			if r.side == 0 {
				left = r.entries
			} else {
				right = r.entries
			}
		}
	}
	return left, right
}

type checksumGroup struct {
	dir   *TreeNode
	files []*TreeNode
}

// groupFilesByTopLevel splits PresenceBoth files under root into groups,
// one per top-level child dir (plus one with dir=nil for files directly under
// root). The scanner processes each group independently so the rsync MD4
// daemon call is scoped per top-level dir rather than fired once on the whole
// base — which on big trees can take days for a single call.
//
// onlyPending=true skips files whose Compare.Checksum is already known
// (AttrEqual/AttrDifferent) — used by rescan paths so cached CRC survives
// rescan unchanged. ChecksumNode and initial Scan pass false to enqueue all
// PresenceBoth files.
func groupFilesByTopLevel(root *TreeNode, onlyPending bool) []checksumGroup {
	var groups []checksumGroup
	var rootFiles []*TreeNode
	for _, child := range root.Children {
		if child.IsAttr {
			continue
		}
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
		groups = append([]checksumGroup{{dir: nil, files: rootFiles}}, groups...)
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
	if !onlyPending {
		return true
	}
	return n.Compare.Checksum == AttrUnknown
}

func (s *Scanner) negotiateChecksum() bool {
	s.cksumOnce.Do(func() {
		s.cksumProbed = true
		s.cksumLeft = probeBackend(s.left)
		s.cksumRight = probeBackend(s.right)
		leftSet := toSet(s.cksumLeft)
		rightSet := toSet(s.cksumRight)
		for _, algo := range []string{"sha256", "sha1", "md5", "md4", "rsync"} {
			if leftSet[algo] && rightSet[algo] {
				setBackendAlgo(s.left, algo)
				setBackendAlgo(s.right, algo)
				s.cksumAlgo = algo
				s.cksumOK = true
				return
			}
		}
	})
	return s.cksumOK
}

func (s *Scanner) ChecksumAlgo() string {
	return s.cksumAlgo
}

func (s *Scanner) ChecksumProbed() bool {
	return s.cksumProbed
}

func (s *Scanner) ChecksumInfo() (left, right []string) {
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
	m := make(map[string]bool)
	for _, v := range s {
		m[v] = true
	}
	return m
}
