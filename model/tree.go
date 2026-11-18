package model

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CompareOpts controls which file attributes are compared.
type CompareOpts struct {
	Size        bool
	ModTime     bool
	ATime       bool
	CTime       bool
	BirthTime   bool
	Mode        bool
	Checksum    bool
	SubSecond   bool
	TimeGrace   bool
	IgnoreTZDST bool
}

// AttrStatus and Presence are a byte each: every node keeps eight of them
// (F13).
type AttrStatus uint8

const (
	AttrUnknown AttrStatus = iota
	AttrScanning
	AttrEqual
	AttrDifferent
	AttrNA
)

type Presence uint8

const (
	PresenceBoth Presence = iota
	PresenceLeftOnly
	PresenceRightOnly
)

type CompareResult struct {
	Presence  Presence
	Size      AttrStatus
	ModTime   AttrStatus
	ATime     AttrStatus
	CTime     AttrStatus
	BirthTime AttrStatus
	Mode      AttrStatus
	Checksum  AttrStatus
}

// Side selects one of the two compared trees and indexes every per-side
// array below.
type Side int

const (
	SideLeft Side = iota
	SideRight
)

// Other is the opposite side.
func (s Side) Other() Side { return 1 - s }

// Only is the presence of a node found on s alone.
func (s Side) Only() Presence {
	if s == SideLeft {
		return PresenceLeftOnly
	}
	return PresenceRightOnly
}

// CopySides is the source and destination of a copy in the given direction.
func CopySides(leftToRight bool) (src, dst Side) {
	if leftToRight {
		return SideLeft, SideRight
	}
	return SideRight, SideLeft
}

// SideState is one side's view of a node. The scanner owns every field and
// writes them under the tree lock.
type SideState struct {
	Entry *FileEntry
	// Checksum and the (size, mtime) of Entry when it was computed; the
	// preserving merge keeps the sum only while that fingerprint still holds.
	Checksum        string
	ChecksumSize    int64
	ChecksumModTime UnixNano
	ChecksumDone    bool // file: this side's pass finished
	ChecksumErr     bool
	ChecksumPending bool // dir: checksum work is scheduled on this side
	ChecksumActive  bool // dir: a worker is inside it now
}

// dropChecksum forgets the cached sum and its fingerprint.
func (st *SideState) dropChecksum() {
	st.Checksum, st.ChecksumSize, st.ChecksumModTime = "", 0, 0
}

// SideTotals is a directory's subtree rollup on one side.
type SideTotals struct {
	Size  int64
	Files int
	Dirs  int
}

func (t *SideTotals) add(o SideTotals) {
	t.Size, t.Files, t.Dirs = t.Size+o.Size, t.Files+o.Files, t.Dirs+o.Dirs
}

// A TreeNode is one file or folder of the tree, which holds one for each:
// at ten million files every byte of it is ten megabytes. The fields go from
// the largest to the smallest, so the small ones pack together, and what
// only a folder needs a file does not carry (F13).
type TreeNode struct {
	RelPath  string
	Name     string
	Sides    [2]SideState // indexed by Side
	Parent   *TreeNode
	Children []*TreeNode
	Depth    int
	// The totals, the SubtreeXxx fields, ChildStatus, Guides and IsLast are
	// written only by PropagateStatus and FlattenTree on the UI goroutine.
	// The scanner never touches them, which is why the UI may refresh them
	// while holding only Scanner.ReadTree's shared lock.
	//
	// Guides is a bitmask of the vertical tree-guide columns to the left of
	// this row: bit i is set when the ancestor at depth i has more siblings
	// below it. Valid bits are [0, Depth); depths past 64 render unguided.
	Guides uint64
	// totals is what a folder holds on each side (Totals): nil for a file,
	// and for a folder that holds nothing.
	totals *[2]SideTotals
	// ChecksumInFlight counts files under this dir being summed on each side.
	// Workers bump it without the tree lock, so it stays outside Sides to keep
	// that array assignable.
	ChecksumInFlight       [2]atomic.Int32
	SubtreeBothFiles       int32
	SubtreeChecksumPending int32
	Compare                CompareResult
	IsDir                  bool
	// IsSpecial marks a row of links, pipes or devices (FileEntry.IsSpecial):
	// its own kind, so a file of the same name is a twin, as a folder is.
	IsSpecial bool
	// Expanded is what the user unfolded. Like the rollup fields it belongs
	// to the UI goroutine, which reads and writes it under the shared lock;
	// the scanner never touches it. So a fold leaves the tree's revision
	// alone and costs no rollup walk (U21).
	Expanded               bool
	Listed                 bool
	ListErr                bool
	SubtreePending         bool
	ChecksumCountedDone    bool
	SubtreeChecksumAnyDiff bool
	ChildStatus            AttrStatus
	IsLast                 bool
}

// Totals is what a folder holds on side, as the last rollup counted it:
// nothing for a file.
func (n *TreeNode) Totals(side Side) SideTotals {
	if n.totals == nil {
		return SideTotals{}
	}
	return n.totals[side]
}

// setTotals keeps a folder's rollup, in a block only a folder that holds
// something has.
func (n *TreeNode) setTotals(t [2]SideTotals) {
	if n.totals == nil {
		if t == ([2]SideTotals{}) {
			return
		}
		n.totals = new([2]SideTotals)
	}
	*n.totals = t
}

// SwapTotals swaps the sides of the rollup, as the sides swap.
func (n *TreeNode) SwapTotals() {
	if n.totals != nil {
		n.totals[0], n.totals[1] = n.totals[1], n.totals[0]
	}
}

// Entries returns both sides' entries.
func (n *TreeNode) Entries() (left, right *FileEntry) {
	return n.Sides[SideLeft].Entry, n.Sides[SideRight].Entry
}

// IsRoot reports the top row. It has no entries: it stands for both base
// directories, which always exist.
func (n *TreeNode) IsRoot() bool { return n.IsDir && n.RelPath == "" }

// dirOn reports whether n is a directory on side.
func (n *TreeNode) dirOn(side Side) bool {
	if e := n.Sides[side].Entry; e != nil {
		return e.IsDir
	}
	return n.IsRoot()
}

// oneSideSummed reports a file with a cached checksum on exactly one side.
func (n *TreeNode) oneSideSummed() bool {
	return (n.Sides[SideLeft].Checksum != "") != (n.Sides[SideRight].Checksum != "")
}

// Row is one visible line: a tree node, or one attribute line under an
// expanded file. Attribute rows are built per frame and never live in the
// tree, so a node pays nothing for them.
type Row struct {
	Node *TreeNode // the node, or the file an attribute row belongs to
	Attr *AttrRow  // nil for a node row
}

// AttrRow is one compared attribute of an expanded file.
type AttrRow struct {
	Label    string
	Val      [2]string // per side; "-" when absent
	Raw      [2]string // per side; "" when there is nothing to add
	Status   AttrStatus
	Inactive bool
	Winner   int // -1 when the left value is newer or larger, 1 for the right, 0 neither
	Shift    int // times equal only by the time-zone rule: how many whole hours apart
	Guides   uint64
	IsLast   bool
	Depth    int
}

// SubtreeChecksumScanned reports whether every PresenceBoth file in n's
// subtree has had its checksum computed (success or mismatch). False for
// subtrees with no Both files or with unlisted dirs underneath.
func (n *TreeNode) SubtreeChecksumScanned() bool {
	return !n.SubtreePending && n.SubtreeBothFiles > 0 && n.SubtreeChecksumPending == 0
}

// combineStatus folds attribute results: any difference wins, otherwise any
// unknown, otherwise equal. An empty set is unknown.
func combineStatus(attrs []AttrStatus) AttrStatus {
	if len(attrs) == 0 {
		return AttrUnknown
	}
	result := AttrEqual
	for _, a := range attrs {
		switch a {
		case AttrDifferent:
			return AttrDifferent
		case AttrUnknown, AttrScanning:
			result = AttrUnknown
		}
	}
	return result
}

func NewRootNode() *TreeNode {
	return &TreeNode{Name: "/", IsDir: true, Expanded: true, Listed: true}
}

// ChangedPaths names files whose content this process modified since CRC was
// last cached. The preserving merge drops cached CRC for any path listed here
// even when the new (size, mtime) match the cached fingerprint — handles the
// case where rsync -t preserves mtime across a copy, leaving the fingerprint
// looking valid while the body has changed.
//
// Dirs names whole subtrees that were rewritten wholesale (batch transfer),
// where enumerating every file would cost as much memory as the tree itself.
type ChangedPaths struct {
	Paths [2]map[string]bool // indexed by Side
	Dirs  [2][]string

	once sync.Once
	dirs map[string]bool // every directory holding a changed path, built on first use
	all  bool            // a whole-tree rewrite: everything is touched
}

func underAny(dirs []string, relPath string) bool {
	for _, d := range dirs {
		if d == "" || relPath == d || strings.HasPrefix(relPath, d+"/") {
			return true
		}
	}
	return false
}

func (c *ChangedPaths) has(side Side, relPath string) bool {
	return c != nil && (c.Paths[side][relPath] || underAny(c.Dirs[side], relPath))
}

// touchesSubtree reports whether any changed path lies at or under dir. The
// directories holding changes are indexed once, so a rescan asks O(1) per
// directory instead of scanning every changed path each time.
func (c *ChangedPaths) touchesSubtree(dir string) bool {
	if c == nil || dir == "" {
		return false
	}
	c.once.Do(c.index)
	return c.all || c.dirs[dir] || underAny(c.Dirs[SideLeft], dir) || underAny(c.Dirs[SideRight], dir)
}

func (c *ChangedPaths) index() {
	c.dirs = make(map[string]bool)
	mark := func(p string) {
		for d := p; d != ""; d = DirOf(d) {
			c.dirs[d] = true
		}
	}
	for _, paths := range c.Paths {
		for p := range paths {
			mark(p)
		}
	}
	for _, d := range slices.Concat(c.Dirs[SideLeft], c.Dirs[SideRight]) {
		if d == "" {
			c.all = true
		}
		mark(d)
	}
}

// MergeChildren merges fresh entries into parent's children, reusing existing
// TreeNodes by (name, isDir) so already-listed subtrees keep their
// Children/Listed/Expanded state; new entries are added, missing ones
// dropped. changed names paths whose cached checksums must not survive.
func MergeChildren(parent *TreeNode, leftEntries, rightEntries []FileEntry, depth int, opts CompareOpts, changed *ChangedPaths) []*TreeNode {
	return mergeChildren(parent, parent.Children, leftEntries, rightEntries, depth, opts, changed)
}

// mergeChildren merges the listings into existing, the children it reuses
// and rewrites. With none it builds new nodes only, and reads and writes no
// live node: what the scan does without the lock (F19).
func mergeChildren(parent *TreeNode, existingChildren []*TreeNode, leftEntries, rightEntries []FileEntry, depth int, opts CompareOpts, changed *ChangedPaths) []*TreeNode {
	keyFor := func(name string, isDir, special bool) string {
		switch {
		case isDir:
			return name + "/"
		case special:
			return name + "\x00" // no name holds a NUL
		}
		return name
	}
	existing := make(map[string]*TreeNode, len(existingChildren))
	for _, c := range existingChildren {
		existing[keyFor(c.Name, c.IsDir, c.IsSpecial)] = c
	}
	byKey := make(map[string]*TreeNode)
	pick := func(name string, isDir, special bool, relPath string) *TreeNode {
		k := keyFor(name, isDir, special)
		if n, ok := byKey[k]; ok {
			return n
		}
		if old, ok := existing[k]; ok {
			for s := range old.Sides {
				old.Sides[s].Entry = nil
			}
			byKey[k] = old
			return old
		}
		n := &TreeNode{
			RelPath:   relPath,
			Name:      name,
			IsDir:     isDir,
			IsSpecial: special,
			Depth:     depth,
			Parent:    parent,
		}
		byKey[k] = n
		return n
	}
	for i := range leftEntries {
		e := &leftEntries[i]
		n := pick(e.Name, e.IsDir, e.IsSpecial(), e.RelPath)
		n.Sides[SideLeft].Entry = e
	}
	for i := range rightEntries {
		e := &rightEntries[i]
		n := pick(e.Name, e.IsDir, e.IsSpecial(), e.RelPath)
		n.Sides[SideRight].Entry = e
	}
	nodes := make([]*TreeNode, 0, len(byKey))
	for _, n := range byKey {
		compareNode(n, opts)
		revalidateChecksum(n, changed)
		if n.IsDir && len(n.Children) > 0 && n.Compare.Presence != PresenceBoth {
			keep := SideRight
			if n.Compare.Presence == PresenceLeftOnly {
				keep = SideLeft
			}
			n.Children = pruneSubtreeToSide(n.Children, keep, opts)
		}
		nodes = append(nodes, n)
	}
	sortNodes(nodes)
	return nodes
}

func pruneSubtreeToSide(children []*TreeNode, keep Side, opts CompareOpts) []*TreeNode {
	kept := children[:0]
	for _, c := range children {
		c.Sides[keep.Other()] = SideState{}
		if c.Sides[keep].Entry == nil {
			continue
		}
		compareNode(c, opts)
		if !c.IsDir {
			c.Compare.Checksum = AttrNA
		}
		if c.IsDir && len(c.Children) > 0 {
			c.Children = pruneSubtreeToSide(c.Children, keep, opts)
		}
		kept = append(kept, c)
	}
	return kept
}

// revalidateChecksum drops a side's cached checksum if the new entry's
// (size, mtime) disagree with the stored fingerprint, or if the path was
// explicitly flagged as content-changed on that side. After this runs,
// Compare.Checksum reflects whatever survived: AttrUnknown when either side
// is missing a cached sum and both sides are present.
func revalidateChecksum(n *TreeNode, changed *ChangedPaths) {
	if n.IsDir || n.IsSpecial {
		return
	}
	for s := range n.Sides {
		st, e := &n.Sides[s], n.Sides[s].Entry
		if e == nil || changed.has(Side(s), e.RelPath) || st.ChecksumSize != e.Size || st.ChecksumModTime != NanoOf(e.ModTime) {
			st.dropChecksum()
		}
	}
	if n.Compare.Presence != PresenceBoth {
		n.Compare.Checksum = AttrNA
		return
	}
	n.Compare.Checksum = checksumStatus(n)
}

// checksumStatus derives Compare.Checksum from the cached sums of a file
// present on both sides.
func checksumStatus(n *TreeNode) AttrStatus {
	l, r := n.Sides[SideLeft].Checksum, n.Sides[SideRight].Checksum
	switch {
	case l == "" || r == "":
		return AttrUnknown
	case l == r:
		return AttrEqual
	}
	return AttrDifferent
}

// sortNodes orders directories first, then by name, a file before its
// special twin.
func sortNodes(nodes []*TreeNode) {
	slices.SortFunc(nodes, func(a, b *TreeNode) int {
		if a.IsDir != b.IsDir {
			if a.IsDir {
				return -1
			}
			return 1
		}
		if c := strings.Compare(a.Name, b.Name); c != 0 || a.IsSpecial == b.IsSpecial {
			return c
		}
		if a.IsSpecial {
			return 1
		}
		return -1
	})
}

// sameKind reports rows of one kind: folder, file, or special. Rows of one
// name and different kinds are twins.
func sameKind(a, b *TreeNode) bool { return a.IsDir == b.IsDir && a.IsSpecial == b.IsSpecial }

func compareNode(n *TreeNode, opts CompareOpts) {
	l, r := n.Entries()
	n.Compare.Presence = PresenceBoth
	if l == nil {
		n.Compare.Presence = PresenceRightOnly
		return
	}
	if r == nil {
		n.Compare.Presence = PresenceLeftOnly
		return
	}
	if n.IsDir || n.IsSpecial {
		n.Compare.Size = AttrNA
		n.Compare.ModTime = AttrNA
		n.Compare.ATime = AttrNA
		n.Compare.CTime = AttrNA
		n.Compare.BirthTime = AttrNA
		n.Compare.Mode = AttrNA
		n.Compare.Checksum = AttrNA
		return
	}
	n.Compare.Size = cmpAttr(l.Size == r.Size)
	n.Compare.ModTime = cmpModTime(l, r, opts)
	n.Compare.ATime = cmpKnownTime(l.ATime, r.ATime, opts)
	n.Compare.CTime = cmpKnownTime(l.CTime, r.CTime, opts)
	n.Compare.BirthTime = cmpKnownTime(l.BirthTime, r.BirthTime, opts)
	n.Compare.Mode = cmpAttr(l.Mode == r.Mode)
}

// maxZoneShift is the largest gap between two clocks that are both right
// in their own time zone: the zone offsets run from UTC-12 to UTC+14, and a
// file system that keeps local time (FAT, SMB) is read with one offset and
// written with another, or across a change to or from daylight saving time.
const maxZoneShift = 14 * time.Hour

func cmpTime(a, b time.Time, opts CompareOpts) AttrStatus {
	return cmpTimeWithin(a, b, 0, opts)
}

// cmpKnownTime compares an atime, ctime or birth time. A side that does not
// have that time leaves it out of the compare (AttrNA), as cmpModTime does
// a side's mtime that is not the file's. An rsync file list has no atime
// or ctime, which an rsync+ssh live listing has, and no FTP or WebDAV
// listing has a birth time: with that time on, every file of such a side
// was different from its copy anywhere else (R22).
func cmpKnownTime(a, b UnixNano, opts CompareOpts) AttrStatus {
	if a.IsZero() || b.IsZero() {
		return AttrNA
	}
	return cmpTime(a.Time(), b.Time(), opts)
}

// cmpModTime compares the mtimes of l and r as exactly as both keep them
// (TimeRes): closer than the coarser one's step is equal. A side whose
// mtime is not the file's leaves the mtime out of the compare (AttrNA), as
// a time a side does not know (L17).
func cmpModTime(l, r *FileEntry, opts CompareOpts) AttrStatus {
	res := max(l.TimeRes, r.TimeRes)
	if res == TimeNone {
		return AttrNA
	}
	return cmpTimeWithin(l.ModTime, r.ModTime, res.Step(), opts)
}

// cmpTimeWithin is cmpTime for times kept to step: closer than step is
// equal. The window is on the times as listed, not cut to the second: a
// server that rounds a time up (FAT) is as close as one that cuts it.
func cmpTimeWithin(a, b time.Time, step time.Duration, opts CompareOpts) AttrStatus {
	if step > 0 {
		opts.SubSecond = true
	}
	diff := timeDiff(a, b, opts)
	// With IgnoreTZDST, times a whole number of hours apart are one moment
	// in two zones: up to maxZoneShift. Further apart, they are two moments
	// (F4): a disk image rewritten daily at the same size is not equal to
	// yesterday's.
	if opts.IgnoreTZDST && diff <= maxZoneShift+30*time.Minute {
		diff %= time.Hour
		if diff > 30*time.Minute {
			diff = time.Hour - diff
		}
	}
	switch {
	case diff < step:
		return AttrEqual
	case opts.TimeGrace:
		return cmpAttr(diff <= time.Second)
	}
	return cmpAttr(diff == 0)
}

// timeDiff is how far apart a and b are, to the second unless SubSecond.
func timeDiff(a, b time.Time, opts CompareOpts) time.Duration {
	if !opts.SubSecond {
		a = a.Truncate(time.Second)
		b = b.Truncate(time.Second)
	}
	if diff := a.Sub(b); diff >= 0 {
		return diff
	}
	return b.Sub(a)
}

// ZoneShiftHours returns how many whole hours apart a and b are when they
// compare equal only by the time-zone rule (IgnoreTZDST), else 0. The UI
// marks such times, so an equal that rests on the rule can be told from an
// equal that does not.
func ZoneShiftHours(a, b time.Time, opts CompareOpts) int {
	if !opts.IgnoreTZDST || cmpTime(a, b, opts) != AttrEqual {
		return 0
	}
	return int((timeDiff(a, b, opts) + 30*time.Minute) / time.Hour)
}

func cmpAttr(equal bool) AttrStatus {
	if equal {
		return AttrEqual
	}
	return AttrDifferent
}

// TreeStats are whole-tree totals, accumulated by PropagateStatus so the UI
// gets them without a second full walk.
type TreeStats struct {
	LeftFiles, RightFiles int
	LeftDirs, RightDirs   int
	LeftSize, RightSize   int64
	// Files on both sides by NodeStatus: equal, different, or not compared
	// yet (no checksum yet, or no compare option on).
	FilesEqual, FilesDiff int
	FilesUnknown          int
	FilesLeftOnly         int
	FilesRightOnly        int
	TotalDirs             int64
	TotalFiles            int64
	TotalSize             int64
}

// PropagateStatus recomputes every subtree rollup (sizes, counts, child status,
// checksum pending) and returns whole-tree totals. O(all nodes) — call it when
// the tree has changed, not once per frame.
//
// It writes the rollup fields of every node, but those belong to the single UI
// goroutine and the scanner never touches them, so Scanner.ReadTree's shared
// lock is enough: it excludes the scanner without serializing the UI against
// the other readers (a copy enumerating the same tree).
func PropagateStatus(root *TreeNode, opts *CompareOpts) TreeStats {
	var s TreeStats
	propagateStatus(root, opts, &s)
	return s
}

// FlattenTree returns the visible rows, walking only expanded nodes. It does
// not refresh rollups — see PropagateStatus. hint sizes the result up front;
// pass the previous result's length. Writes Guides/IsLast under the same
// UI-goroutine ownership rule as PropagateStatus.
func FlattenTree(root *TreeNode, opts *CompareOpts, hint int) []Row {
	flat := make([]Row, 0, hint+1)
	root.IsLast = true
	root.Guides = 0
	flat = append(flat, Row{Node: root})
	if !root.Expanded {
		return flat
	}
	for i, child := range root.Children {
		child.IsLast = i == len(root.Children)-1
		flattenNode(child, 0, opts, &flat)
	}
	return flat
}

// childGuides returns the guide mask for the children of a node at depth,
// extending it with this node's own continuation bit.
func childGuides(guides uint64, depth int, isLast bool) uint64 {
	if depth >= 64 || isLast {
		return guides
	}
	return guides | 1<<uint(depth)
}

func propagateStatus(node *TreeNode, opts *CompareOpts, st *TreeStats) AttrStatus {
	if !node.IsDir {
		node.SubtreePending = false
		node.SubtreeBothFiles = 0
		node.SubtreeChecksumPending = 0
		node.SubtreeChecksumAnyDiff = false
		if node.Compare.Presence == PresenceBoth {
			node.SubtreeBothFiles = 1
			switch node.Compare.Checksum {
			case AttrEqual:
			case AttrDifferent:
				node.SubtreeChecksumAnyDiff = true
			default:
				node.SubtreeChecksumPending = 1
			}
		}
		return NodeStatus(node, opts)
	}
	if !node.Listed {
		node.ChildStatus = AttrUnknown
		node.setTotals([2]SideTotals{})
		node.SubtreePending = true
		node.SubtreeBothFiles = 0
		node.SubtreeChecksumPending = 0
		node.SubtreeChecksumAnyDiff = false
		return AttrUnknown
	}
	result := AttrEqual
	var tot [2]SideTotals
	pending := false
	var bothFiles, cksumPending int32
	anyDiff := false
	for _, child := range node.Children {
		s := propagateStatus(child, opts, st)
		if s == AttrDifferent {
			result = AttrDifferent
		} else if s == AttrUnknown && result != AttrDifferent {
			result = AttrUnknown
		}
		if child.IsDir && child.SubtreePending {
			pending = true
		}
		bothFiles += child.SubtreeBothFiles
		cksumPending += child.SubtreeChecksumPending
		if child.SubtreeChecksumAnyDiff {
			anyDiff = true
		}
		l, r := child.Entries()
		if l != nil {
			if l.IsDir {
				st.LeftDirs++
			} else {
				st.LeftFiles++
				st.LeftSize += l.Size
			}
		}
		if r != nil {
			if r.IsDir {
				st.RightDirs++
			} else {
				st.RightFiles++
				st.RightSize += r.Size
			}
		}
		if child.IsDir {
			st.TotalDirs++
			for s := range tot {
				tot[s].add(child.Totals(Side(s)))
				if child.Sides[s].Entry != nil {
					tot[s].Dirs++
				}
			}
			continue
		}
		st.TotalFiles++
		if l != nil {
			st.TotalSize += l.Size
		} else if r != nil {
			st.TotalSize += r.Size
		}
		for s := range tot {
			if e := child.Sides[s].Entry; e != nil {
				tot[s].Size += e.Size
				tot[s].Files++
			}
		}
		switch child.Compare.Presence {
		case PresenceBoth:
			switch s { // NodeStatus, as the panel shows it
			case AttrEqual:
				st.FilesEqual++
			case AttrDifferent:
				st.FilesDiff++
			default:
				st.FilesUnknown++
			}
		case PresenceLeftOnly:
			st.FilesLeftOnly++
		case PresenceRightOnly:
			st.FilesRightOnly++
		}
	}
	node.setTotals(tot)
	node.SubtreePending = pending
	node.SubtreeBothFiles = bothFiles
	node.SubtreeChecksumPending = cksumPending
	node.SubtreeChecksumAnyDiff = anyDiff
	if node.Compare.Presence != PresenceBoth {
		node.ChildStatus = AttrDifferent
		return AttrDifferent
	}
	node.ChildStatus = result
	return result
}

// NodeStatus is the one rule for whether a file is equal on both sides: the
// panel's colors, the equal and different counts, next difference and the
// copy all ask it (F22). It folds the attributes opts enables; a checksum
// known to differ makes the file different with checksum compare off too,
// since the bytes are not the same.
func NodeStatus(node *TreeNode, opts *CompareOpts) AttrStatus {
	if node.Compare.Presence != PresenceBoth || node.Compare.Checksum == AttrDifferent {
		return AttrDifferent
	}
	if opts == nil {
		return AttrUnknown
	}
	c := &node.Compare
	var attrs [7]AttrStatus
	n := 0
	for _, a := range [...]struct {
		on bool
		s  AttrStatus
	}{{opts.Size, c.Size}, {opts.ModTime, c.ModTime}, {opts.ATime, c.ATime}, {opts.CTime, c.CTime}, {opts.BirthTime, c.BirthTime}, {opts.Mode, c.Mode}, {opts.Checksum, c.Checksum}} {
		if a.on {
			attrs[n] = a.s
			n++
		}
	}
	return combineStatus(attrs[:n])
}

func flattenNode(node *TreeNode, guides uint64, opts *CompareOpts, flat *[]Row) {
	node.Guides = guides
	*flat = append(*flat, Row{Node: node})
	if !node.Expanded {
		return
	}
	if node.IsDir {
		kg := childGuides(guides, node.Depth, node.IsLast)
		for i, child := range node.Children {
			child.IsLast = i == len(node.Children)-1
			flattenNode(child, kg, opts, flat)
		}
		return
	}
	flattenFileAttrs(node, guides, opts, flat)
}

func flattenFileAttrs(node *TreeNode, guides uint64, opts *CompareOpts, flat *[]Row) {
	kg := childGuides(guides, node.Depth, node.IsLast)

	type attr struct {
		label    string
		leftVal  string
		rightVal string
		leftRaw  string
		rightRaw string
		status   AttrStatus
		inactive bool
		winner   int
		shift    int
	}

	tf := "2006-01-02 15:04:05 MST"
	if opts != nil && opts.SubSecond {
		tf = "2006-01-02 15:04:05.000000000 MST"
	}
	l, r := node.Entries()
	val := func(get func(*FileEntry) string) (string, string) {
		lv, rv := "-", "-"
		if l != nil {
			lv = get(l)
		}
		if r != nil {
			rv = get(r)
		}
		return lv, rv
	}

	sizeWin := 0
	timeWin := func(lt, rt time.Time) int {
		if lt.After(rt) {
			return -1
		}
		if rt.After(lt) {
			return 1
		}
		return 0
	}
	if l != nil && r != nil {
		switch {
		case l.Size > r.Size:
			sizeWin = -1
		case r.Size > l.Size:
			sizeWin = 1
		}
	}

	var attrs []attr

	rawStr := func(get func(*FileEntry) string) (string, string) {
		lv, rv := "", ""
		if l != nil {
			lv = get(l)
		}
		if r != nil {
			rv = get(r)
		}
		return lv, rv
	}

	addTime := func(label string, get func(*FileEntry) time.Time, status AttrStatus, optEnabled bool) {
		lv, rv := val(func(e *FileEntry) string {
			if t := get(e); isTimeValid(t) {
				return t.Format(tf)
			}
			return "n/a"
		})
		lraw, rraw := rawStr(func(e *FileEntry) string {
			if t := get(e); isTimeValid(t) {
				return fmt.Sprintf("%d", t.Unix())
			}
			return ""
		})
		bothValid := l != nil && r != nil && isTimeValid(get(l)) && isTimeValid(get(r))
		inactive := !optEnabled || !bothValid
		w, shift := 0, 0
		if !inactive {
			w = timeWin(get(l), get(r))
			if status == AttrEqual {
				shift = ZoneShiftHours(get(l), get(r), *opts)
			}
		}
		attrs = append(attrs, attr{label, lv, rv, lraw, rraw, status, inactive, w, shift})
	}

	ls, rs := val(func(e *FileEntry) string { return FormatSize(e.Size) })
	if node.Compare.Size == AttrDifferent && ls == rs && l != nil && r != nil {
		ls = fmt.Sprintf("%d", l.Size)
		rs = fmt.Sprintf("%d", r.Size)
	}
	lsraw, rsraw := rawStr(func(e *FileEntry) string { return fmt.Sprintf("%d", e.Size) })
	attrs = append(attrs, attr{"size", ls, rs, lsraw, rsraw, node.Compare.Size, false, sizeWin, 0})
	addTime("mtime", func(e *FileEntry) time.Time { return e.ModTime }, node.Compare.ModTime, opts != nil && opts.ModTime)
	addTime("atime", func(e *FileEntry) time.Time { return e.ATime.Time() }, node.Compare.ATime, opts != nil && opts.ATime)
	addTime("ctime", func(e *FileEntry) time.Time { return e.CTime.Time() }, node.Compare.CTime, opts != nil && opts.CTime)
	addTime("btime", func(e *FileEntry) time.Time { return e.BirthTime.Time() }, node.Compare.BirthTime, opts != nil && opts.BirthTime)
	lv, rv := val(func(e *FileEntry) string { return e.Mode.String() })
	lpraw, rpraw := rawStr(func(e *FileEntry) string { return fmt.Sprintf("0%o", e.Mode.Perm()) })
	attrs = append(attrs, attr{"perm", lv, rv, lpraw, rpraw, node.Compare.Mode, false, 0, 0})
	lc, rc := node.Sides[SideLeft].Checksum, node.Sides[SideRight].Checksum
	if lc == "" {
		lc = "-"
	}
	if rc == "" {
		rc = "-"
	}
	attrs = append(attrs, attr{"cksum", lc, rc, "", "", node.Compare.Checksum, false, 0, 0})

	for i, a := range attrs {
		*flat = append(*flat, Row{Node: node, Attr: &AttrRow{
			Label: a.label, Val: [2]string{a.leftVal, a.rightVal}, Raw: [2]string{a.leftRaw, a.rightRaw},
			Status: a.status, Inactive: a.inactive, Winner: a.winner, Shift: a.shift,
			Guides: kg, IsLast: i == len(attrs)-1, Depth: node.Depth + 1,
		}})
	}
}

// CollectCopyFiles returns what a copy of node writes: the files and empty
// folders missing on the destination, the files that differ, and those of
// the other type there. A file on both sides that is not compared yet
// (NodeStatus unknown: with checksum compare on, a checksum not computed;
// or no compare option on) goes in only with unknown set: most such files
// are equal, and copying them silently rewrites them all (F10).
// WalkCopyFiles names them, so the user can be asked.
func CollectCopyFiles(node *TreeNode, opts *CompareOpts, leftToRight, unknown bool) []*TreeNode {
	var result []*TreeNode
	WalkCopyFiles(node, opts, leftToRight, func(n *TreeNode, notCompared bool) {
		if unknown || !notCompared {
			result = append(result, n)
		}
	})
	return result
}

// UnlistedDir returns the first directory in node's subtree that was never
// listed (Listed) or whose last listing failed (ListErr), or nil.
// CollectCopyFiles and CollectMirrorDeletes see such a dir as empty, so copy
// would skip the subtree and mirror would under-count deletes; callers must
// list it first.
func UnlistedDir(node *TreeNode) *TreeNode {
	if node == nil || !node.IsDir {
		return nil
	}
	if !node.Listed || node.ListErr {
		return node
	}
	for _, child := range node.Children {
		if n := UnlistedDir(child); n != nil {
			return n
		}
	}
	return nil
}

// WalkCopyFiles calls visit for each node a copy of node may write;
// notCompared marks a file on both sides that is written only when the
// user says to copy what is not compared yet.
func WalkCopyFiles(node *TreeNode, opts *CompareOpts, leftToRight bool, visit func(n *TreeNode, notCompared bool)) {
	from, to := CopySides(leftToRight)
	src, dst := node.Sides[from].Entry, node.Sides[to].Entry
	if !node.dirOn(from) {
		switch node.Compare.Presence {
		case PresenceBoth:
			collision := dst != nil && src != nil && src.IsDir != dst.IsDir
			switch status := NodeStatus(node, opts); {
			case collision || status == AttrDifferent:
				visit(node, false)
			case status == AttrUnknown:
				visit(node, true)
			}
		case PresenceLeftOnly:
			if leftToRight {
				visit(node, false)
			}
		case PresenceRightOnly:
			if !leftToRight {
				visit(node, false)
			}
		}
		return
	}
	if node.Listed && len(node.Children) == 0 {
		switch node.Compare.Presence {
		case PresenceLeftOnly:
			if leftToRight {
				visit(node, false)
			}
		case PresenceRightOnly:
			if !leftToRight {
				visit(node, false)
			}
		}
		return
	}
	for _, child := range node.Children {
		WalkCopyFiles(child, opts, leftToRight, visit)
	}
}

// OverwritesNewer says why a copy of n would put an older or smaller file
// over the destination's: the file there is "newer", "larger", or "newer
// and larger"; or "" (F16). A time the compare calls equal (to the second,
// within the grace, a time-zone shift, within what a side keeps of it) is
// not newer, nor is a time a side does not know, nor an upload time.
func OverwritesNewer(n *TreeNode, leftToRight bool, opts CompareOpts) string {
	from, to := CopySides(leftToRight)
	src, dst := n.Sides[from].Entry, n.Sides[to].Entry
	if src == nil || dst == nil || src.IsDir || dst.IsDir || src.IsSpecial() || dst.IsSpecial() {
		return ""
	}
	newer := !src.ModTime.IsZero() && dst.ModTime.After(src.ModTime) && cmpModTime(src, dst, opts) == AttrDifferent
	larger := dst.Size > src.Size
	switch {
	case newer && larger:
		return "newer and larger"
	case newer:
		return "newer"
	case larger:
		return "larger"
	}
	return ""
}

// CollectTypeCollisions returns the destination nodes a copy of node has to
// remove before it can write: same-name twins of another kind (file, folder,
// link or other special) of node itself, and of every node the copy writes
// inside it. Twins of node's siblings belong to other copies and are never
// returned. A special source row is never written, so nothing is in its way.
func CollectTypeCollisions(node *TreeNode, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	src, dst := CopySides(leftToRight)
	if node.Parent != nil && node.Sides[src].Entry != nil && !node.IsSpecial {
		for _, sib := range node.Parent.Children {
			if sib != node && sib.Name == node.Name && !sameKind(sib, node) && sib.Sides[dst].Entry != nil {
				result = append(result, sib)
			}
		}
	}
	if node.dirOn(src) {
		collectTypeCollisionsRec(node, leftToRight, &result)
	}
	return result
}

// findTwinPairs appends each child on the destination side that has a
// same-name twin of another kind on the source side, which the copy writes
// there. A special source row is never written, so its twin stays.
func findTwinPairs(children []*TreeNode, leftToRight bool, result *[]*TreeNode) {
	src, dst := CopySides(leftToRight)
	// Twins need a folder or a special row; most folders have no name twice.
	var named map[string][]*TreeNode
	for _, c := range children {
		if c.IsDir || c.IsSpecial {
			if named == nil {
				named = make(map[string][]*TreeNode)
			}
			named[c.Name] = append(named[c.Name], c)
		}
	}
	if named == nil {
		return
	}
	for _, c := range children {
		if g, ok := named[c.Name]; ok && !c.IsDir && !c.IsSpecial {
			named[c.Name] = append(g, c)
		}
	}
	for _, b := range children {
		if b.Sides[dst].Entry == nil {
			continue
		}
		for _, a := range named[b.Name] {
			if !sameKind(a, b) && !a.IsSpecial && a.Sides[src].Entry != nil {
				*result = append(*result, b)
				break
			}
		}
	}
}

func collectTypeCollisionsRec(node *TreeNode, leftToRight bool, result *[]*TreeNode) {
	src, _ := CopySides(leftToRight)
	findTwinPairs(node.Children, leftToRight, result)
	for _, child := range node.Children {
		if child.dirOn(src) {
			collectTypeCollisionsRec(child, leftToRight, result)
		}
	}
}

func CollectMirrorDeletes(node *TreeNode, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	_, dst := CopySides(leftToRight)
	destOnly := dst.Only()
	for _, child := range node.Children {
		if child.Compare.Presence == destOnly {
			if hasTwinWithSrc(node, child, leftToRight) {
				continue
			}
			result = append(result, child)
			continue
		}
		if child.IsDir {
			result = append(result, CollectMirrorDeletes(child, leftToRight)...)
		}
	}
	return result
}

// hasTwinWithSrc reports whether child has a sibling with the same Name but
// another kind, where that sibling has a src-side entry. Used to exclude
// type-collision twins from mirror-delete logic — they're cleaned up by
// CollectTypeCollisions and would otherwise be double-processed (and risk
// removing the just-copied file at the same path).
func hasTwinWithSrc(parent, child *TreeNode, leftToRight bool) bool {
	src, _ := CopySides(leftToRight)
	for _, sibling := range parent.Children {
		if sibling == child || sibling.Name != child.Name || sameKind(sibling, child) {
			continue
		}
		if sibling.Sides[src].Entry != nil {
			return true
		}
	}
	return false
}

func CountDescendants(node *TreeNode) (files, dirs int, complete bool) {
	complete = true
	if !node.Listed {
		return 0, 0, false
	}
	for _, child := range node.Children {
		if child.IsDir {
			dirs++
			cf, cd, cc := CountDescendants(child)
			files += cf
			dirs += cd
			if !cc {
				complete = false
			}
		} else {
			files++
		}
	}
	return
}

// FindNextDiff returns the next difference after `after` in DFS order: a
// node on one side only, or a file on both sides whose compared attributes
// differ. Folders on both sides aren't leaves; the search drills into them.
// If `after` is itself a diff leaf, its subtree is skipped (the dir's
// existence is the diff). Pass after=nil to start from the top.
//
// The walk starts at `after`, not at the top: what lies under it, then what
// follows it and each folder above it. With counted set, the folder rollups
// are current (PropagateStatus ran with opts since the tree last changed),
// and a folder whose rollup holds nothing but equal files is passed over
// whole. So a jump costs the way to the next difference, not the tree before
// or after the cursor (U21). Returns nil if no further difference is found,
// or if `after` is no longer in the tree.
func FindNextDiff(root, after *TreeNode, opts *CompareOpts, counted bool) *TreeNode {
	if root == nil {
		return nil
	}
	w := diffWalk{opts, counted}
	if after == nil {
		return w.first(root)
	}
	for n := after; n != root; n = n.Parent {
		if n.Parent == nil || !slices.Contains(n.Parent.Children, n) {
			return nil // a rescan dropped it
		}
	}
	if after.IsDir && !isDiffLeaf(after, opts) {
		if d := w.under(after); d != nil {
			return d
		}
	}
	for n := after; n != root; n = n.Parent {
		if w.allEqual(n.Parent) {
			continue
		}
		siblings := n.Parent.Children
		if d := w.firstIn(siblings[slices.Index(siblings, n)+1:]); d != nil {
			return d
		}
	}
	return nil
}

// diffWalk finds differences in DFS order. With counted set it passes over
// the folders whose rollup holds only equal files.
type diffWalk struct {
	opts    *CompareOpts
	counted bool
}

// allEqual reports a folder the current rollup found to hold only folders
// and files on both sides, every file equal. A folder that was not listed,
// or holds a file not compared yet, rolls up as unknown, never as equal.
func (w diffWalk) allEqual(dir *TreeNode) bool {
	return w.counted && dir.ChildStatus == AttrEqual
}

// first is the first difference at or under n.
func (w diffWalk) first(n *TreeNode) *TreeNode {
	if isDiffLeaf(n, w.opts) {
		return n
	}
	if n.IsDir {
		return w.under(n)
	}
	return nil
}

// under is the first difference below dir.
func (w diffWalk) under(dir *TreeNode) *TreeNode {
	if w.allEqual(dir) {
		return nil
	}
	return w.firstIn(dir.Children)
}

func (w diffWalk) firstIn(nodes []*TreeNode) *TreeNode {
	for _, n := range nodes {
		if d := w.first(n); d != nil {
			return d
		}
	}
	return nil
}

// FindByName walks the tree in DFS order starting after `after` and returns
// the next non-attribute node whose Name matches re. Pass after=nil to start
// from the beginning of the tree. The root node is excluded from matches.
func FindByName(root, after *TreeNode, re *regexp.Regexp) *TreeNode {
	if root == nil || re == nil {
		return nil
	}
	started := after == nil
	var visit func(n *TreeNode) *TreeNode
	visit = func(n *TreeNode) *TreeNode {
		if started && n != root && re.MatchString(n.Name) {
			return n
		}
		if n == after {
			started = true
		}
		if n.IsDir {
			for _, child := range n.Children {
				if r := visit(child); r != nil {
					return r
				}
			}
		}
		return nil
	}
	return visit(root)
}

func isDiffLeaf(n *TreeNode, opts *CompareOpts) bool {
	if n.Compare.Presence != PresenceBoth {
		return true
	}
	if !n.IsDir {
		return NodeStatus(n, opts) == AttrDifferent
	}
	return false
}

func SetExpandedAll(node *TreeNode, expanded bool) {
	if node.IsDir {
		node.Expanded = expanded
	}
	for _, child := range node.Children {
		SetExpandedAll(child, expanded)
	}
}

func isTimeValid(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1970
}

func TimeAgo(t time.Time) string {
	d := time.Since(t)
	if d < 0 {
		d = 0
	}
	days := int(d.Hours() / 24)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case days < 1:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case days < 7:
		return fmt.Sprintf("%dd ago", days)
	case days < 30:
		return fmt.Sprintf("%dwk ago", days/7)
	case days < 365:
		return fmt.Sprintf("%dmo ago", days/30)
	default:
		return fmt.Sprintf("%dyr ago", days/365)
	}
}

func FormatSize(b int64) string {
	switch {
	case b >= 1<<50:
		return fmt.Sprintf("%.1fP", float64(b)/float64(1<<50))
	case b >= 1<<40:
		return fmt.Sprintf("%.1fT", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fK", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func FormatRate(bytesPerSec float64) string {
	switch {
	case bytesPerSec >= 1<<50:
		return fmt.Sprintf("%.1f PB/s", bytesPerSec/float64(1<<50))
	case bytesPerSec >= 1<<40:
		return fmt.Sprintf("%.1f TB/s", bytesPerSec/float64(1<<40))
	case bytesPerSec >= 1<<30:
		return fmt.Sprintf("%.1f GB/s", bytesPerSec/float64(1<<30))
	case bytesPerSec >= 1<<20:
		return fmt.Sprintf("%.1f MB/s", bytesPerSec/float64(1<<20))
	case bytesPerSec >= 1<<10:
		return fmt.Sprintf("%.1f KB/s", bytesPerSec/float64(1<<10))
	default:
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}
}
