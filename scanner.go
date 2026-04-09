package main

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

type checksumProber interface {
	probeChecksums() []string
	setChecksumAlgo(algo string)
}

type Scanner struct {
	left         Backend
	right        Backend
	concurrency  int
	stallTimeout time.Duration
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

func NewScanner(left, right Backend, concurrency int) *Scanner {
	if concurrency < 1 {
		concurrency = 4
	}
	return &Scanner{
		left:         left,
		right:        right,
		concurrency:  concurrency,
		stallTimeout: 120 * time.Second,
	}
}

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

func (s *Scanner) Scan(ctx context.Context, withChecksum bool, subSecond, timeGrace bool) {
	ctx, s.cancel = context.WithCancel(ctx)

	root := NewRootNode()
	s.mu.Lock()
	s.tree = root
	s.mu.Unlock()

	var stats struct {
		totalFiles, totalDirs                          atomic.Int64
		dirsListed, dirsTotal                          atomic.Int64
		filesEqual, filesDiff, filesLeft, filesRight   atomic.Int64
	}

	queue := []dirJob{{relDir: "", parent: root, depth: 1, listLeft: true, listRight: true}}
	stats.dirsTotal.Store(1)
	leftPending := 1
	rightPending := 1

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

		job := queue[0]
		queue = queue[1:]

		s.setProgress(progress("scanning..."))

		leftEntries, rightEntries := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)

		if job.listLeft {
			leftPending--
		}
		if job.listRight {
			rightPending--
		}

		children := MergeChildren(job.parent, leftEntries, rightEntries, job.depth, subSecond, timeGrace)

		s.mu.Lock()
		job.parent.Children = children
		job.parent.Listed = true
		s.mu.Unlock()

		stats.dirsListed.Add(1)
		for _, child := range children {
			if child.IsDir {
				stats.totalDirs.Add(1)
				stats.dirsTotal.Add(1)
				ll := child.Compare.Presence != PresenceRightOnly
				lr := child.Compare.Presence != PresenceLeftOnly
				if ll {
					leftPending++
				}
				if lr {
					rightPending++
				}
				queue = append(queue, dirJob{
					relDir:    child.RelPath,
					parent:    child,
					depth:     job.depth + 1,
					listLeft:  ll,
					listRight: lr,
				})
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

		s.setProgress(progress("scanning..."))
	}

	if !withChecksum || !s.negotiateChecksum() {
		p := progress("done")
		s.setProgress(p)
		return
	}

	var files []*TreeNode
	collectFiles(root, &files)
	checksumTotal := int64(len(files))
	var checksumDone atomic.Int64

	p := progress("checksumming...")
	p.LeftActive = true
	p.RightActive = true
	p.ChecksumFiles = checksumTotal
	s.setProgress(p)

	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup

	for _, node := range files {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(n *TreeNode) {
			defer wg.Done()
			defer func() { <-sem }()
			s.checksumNode(ctx, n)
			done := checksumDone.Add(1)
			p := progress("checksumming...")
			p.LeftActive = true
			p.RightActive = true
			p.ChecksumFiles = checksumTotal
			p.ChecksumDone = done
			s.setProgress(p)
		}(node)
	}
	wg.Wait()

	p = progress("done")
	p.ChecksumFiles = checksumTotal
	p.ChecksumDone = checksumDone.Load()
	s.setProgress(p)
}

func (s *Scanner) RescanNode(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace bool) {
	if node.IsDir {
		s.rescanDir(ctx, node, withChecksum, subSecond, timeGrace)
		return
	}
	s.rescanFile(ctx, node, withChecksum, subSecond, timeGrace)
}

func (s *Scanner) rescanFile(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace bool) {
	leftEntries, rightEntries := s.listBoth(ctx, dirOf(node.RelPath))

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
	compareNode(node, subSecond, timeGrace)
	s.mu.Unlock()

	if withChecksum && node.Compare.Presence == PresenceBoth {
		s.checksumNode(ctx, node)
	}
}

func (s *Scanner) rescanDir(ctx context.Context, node *TreeNode, withChecksum bool, subSecond, timeGrace bool) {
	// Update the node's own Left/Right entries so its presence reflects current
	// state on both backends. Skip for the root node which has no parent to list.
	if node.RelPath != "" {
		leftEntries, rightEntries := s.listBoth(ctx, dirOf(node.RelPath))
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
		compareNode(node, subSecond, timeGrace)
		s.mu.Unlock()
	}

	oldExpanded := make(map[string]bool)
	for _, child := range node.Children {
		collectExpanded(child, oldExpanded)
	}

	queue := []dirJob{{relDir: node.RelPath, parent: node, depth: node.Depth + 1, listLeft: true, listRight: true}}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return
		}
		job := queue[0]
		queue = queue[1:]

		leftEntries, rightEntries := s.listDir(ctx, job.relDir, job.listLeft, job.listRight)
		children := MergeChildren(job.parent, leftEntries, rightEntries, job.depth, subSecond, timeGrace)
		restoreExpanded(children, oldExpanded)

		s.mu.Lock()
		job.parent.Children = children
		job.parent.Listed = true
		s.mu.Unlock()

		for _, child := range children {
			if child.IsDir {
				queue = append(queue, dirJob{
					relDir:    child.RelPath,
					parent:    child,
					depth:     job.depth + 1,
					listLeft:  child.Compare.Presence != PresenceRightOnly,
					listRight: child.Compare.Presence != PresenceLeftOnly,
				})
			}
		}
	}

	if !withChecksum {
		return
	}
	var files []*TreeNode
	collectFiles(node, &files)
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for _, f := range files {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(n *TreeNode) {
			defer wg.Done()
			defer func() { <-sem }()
			s.checksumNode(ctx, n)
		}(f)
	}
	wg.Wait()
}

func (s *Scanner) ChecksumNode(ctx context.Context, node *TreeNode) {
	if !s.negotiateChecksum() {
		return
	}
	if !node.IsDir {
		if node.Compare.Presence == PresenceBoth {
			s.checksumNode(ctx, node)
		}
		return
	}
	var files []*TreeNode
	collectFiles(node, &files)
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for _, f := range files {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(n *TreeNode) {
			defer wg.Done()
			defer func() { <-sem }()
			s.checksumNode(ctx, n)
		}(f)
	}
	wg.Wait()
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
	for path := relPath; path != ""; path = dirOf(path) {
		n := findNode(tree, path)
		if n != nil {
			if (leftToRight && n.Right != nil) || (!leftToRight && n.Left != nil) {
				return n
			}
		}
	}
	return tree // root as fallback
}

func (s *Scanner) RefreshDir(parentDir string, left, right []FileEntry, subSecond, timeGrace bool) {
	tree := s.Tree()
	if tree == nil {
		return
	}
	parent := findNode(tree, parentDir)
	if parent == nil {
		return
	}
	oldExpanded := make(map[string]bool)
	for _, child := range parent.Children {
		collectExpanded(child, oldExpanded)
	}
	children := MergeChildren(parent, left, right, parent.Depth+1, subSecond, timeGrace)
	restoreExpanded(children, oldExpanded)
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
	parent := findNode(s.tree, dirOf(newRel))
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

func dirOf(relPath string) string {
	for i := len(relPath) - 1; i >= 0; i-- {
		if relPath[i] == '/' {
			return relPath[:i]
		}
	}
	return ""
}

func (s *Scanner) checksumNode(ctx context.Context, node *TreeNode) {
	s.mu.Lock()
	node.Compare.Checksum = AttrScanning
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.stallTimeout)
	defer cancel()

	type result struct {
		sum  string
		err  error
		side int
	}
	ch := make(chan result, 2)
	expected := 0

	if node.Left != nil {
		expected++
		go func() {
			sum, err := s.left.Checksum(ctx, node.RelPath)
			ch <- result{sum, err, 0}
		}()
	}
	if node.Right != nil {
		expected++
		go func() {
			sum, err := s.right.Checksum(ctx, node.RelPath)
			ch <- result{sum, err, 1}
		}()
	}

	var leftSum, rightSum string
	var leftErr, rightErr error
	for i := 0; i < expected; i++ {
		select {
		case <-ctx.Done():
			leftErr = ctx.Err()
			i = expected
		case r := <-ch:
			if r.side == 0 {
				leftSum, leftErr = r.sum, r.err
			} else {
				rightSum, rightErr = r.sum, r.err
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if leftErr != nil || rightErr != nil {
		node.Compare.Checksum = AttrUnknown
		return
	}
	if node.Left == nil || node.Right == nil {
		node.Compare.Checksum = AttrNA
		return
	}
	node.LeftChecksum = leftSum
	node.RightChecksum = rightSum
	if leftSum == rightSum {
		node.Compare.Checksum = AttrEqual
	} else {
		node.Compare.Checksum = AttrDifferent
	}
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

func collectFiles(node *TreeNode, files *[]*TreeNode) {
	if !node.IsDir && node.Compare.Presence == PresenceBoth {
		*files = append(*files, node)
	}
	for _, child := range node.Children {
		collectFiles(child, files)
	}
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
	if p, ok := b.(checksumProber); ok {
		return p.probeChecksums()
	}
	return nil
}

func setBackendAlgo(b Backend, algo string) {
	if p, ok := b.(checksumProber); ok {
		p.setChecksumAlgo(algo)
	}
}

func toSet(s []string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range s {
		m[v] = true
	}
	return m
}
