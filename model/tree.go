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

type AttrStatus int

const (
	AttrUnknown AttrStatus = iota
	AttrScanning
	AttrEqual
	AttrDifferent
	AttrNA
)

type Presence int

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
	ChecksumModTime time.Time
	ChecksumDone    bool // file: this side's pass finished
	ChecksumErr     bool
	ChecksumPending bool // dir: checksum work is scheduled on this side
	ChecksumActive  bool // dir: a worker is inside it now
}

// dropChecksum forgets the cached sum and its fingerprint.
func (st *SideState) dropChecksum() {
	st.Checksum, st.ChecksumSize, st.ChecksumModTime = "", 0, time.Time{}
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

type TreeNode struct {
	RelPath        string
	Name           string
	IsDir          bool
	Sides          [2]SideState // indexed by Side
	Parent         *TreeNode
	Compare        CompareResult
	Children       []*TreeNode
	Expanded       bool
	Listed         bool
	ListErr        bool
	SubtreePending bool
	// ChecksumInFlight counts files under this dir being summed on each side.
	// Workers bump it without the tree lock, so it stays outside Sides to keep
	// that array assignable.
	ChecksumInFlight       [2]atomic.Int32
	ChecksumCountedDone    bool
	SubtreeBothFiles       int
	SubtreeChecksumPending int
	SubtreeChecksumAnyDiff bool
	Depth                  int
	ChildStatus            AttrStatus
	// Totals and the SubtreeXxx, ChildStatus, Guides and IsLast fields are
	// written only by PropagateStatus and FlattenTree on the UI goroutine.
	// The scanner never touches them, which is why the UI may refresh them
	// while holding only Scanner.ReadTree's shared lock.
	Totals [2]SideTotals
	// Guides is a bitmask of the vertical tree-guide columns to the left of
	// this row: bit i is set when the ancestor at depth i has more siblings
	// below it. Valid bits are [0, Depth); depths past 64 render unguided.
	Guides uint64
	IsLast bool
}

// Entries returns both sides' entries.
func (n *TreeNode) Entries() (left, right *FileEntry) {
	return n.Sides[SideLeft].Entry, n.Sides[SideRight].Entry
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

func (n *TreeNode) OverallStatus() AttrStatus {
	if n.Compare.Presence != PresenceBoth {
		return AttrDifferent
	}
	c := &n.Compare
	return combineStatus([]AttrStatus{c.Size, c.ModTime, c.ATime, c.CTime, c.BirthTime, c.Mode, c.Checksum})
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
	keyFor := func(name string, isDir bool) string {
		if isDir {
			return name + "/"
		}
		return name
	}
	existing := make(map[string]*TreeNode, len(parent.Children))
	for _, c := range parent.Children {
		existing[keyFor(c.Name, c.IsDir)] = c
	}
	byKey := make(map[string]*TreeNode)
	pick := func(name string, isDir bool, relPath string) *TreeNode {
		k := keyFor(name, isDir)
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
			RelPath: relPath,
			Name:    name,
			IsDir:   isDir,
			Depth:   depth,
			Parent:  parent,
		}
		byKey[k] = n
		return n
	}
	for i := range leftEntries {
		e := &leftEntries[i]
		n := pick(e.Name, e.IsDir, e.RelPath)
		n.Sides[SideLeft].Entry = e
	}
	for i := range rightEntries {
		e := &rightEntries[i]
		n := pick(e.Name, e.IsDir, e.RelPath)
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
	if n.IsDir {
		return
	}
	for s := range n.Sides {
		st, e := &n.Sides[s], n.Sides[s].Entry
		if e == nil || changed.has(Side(s), e.RelPath) || st.ChecksumSize != e.Size || !st.ChecksumModTime.Equal(e.ModTime) {
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

// sortNodes orders directories first, then by name.
func sortNodes(nodes []*TreeNode) {
	slices.SortFunc(nodes, func(a, b *TreeNode) int {
		if a.IsDir != b.IsDir {
			if a.IsDir {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
}

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
	if n.IsDir {
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
	n.Compare.ModTime = cmpTime(l.ModTime, r.ModTime, opts)
	n.Compare.ATime = cmpTime(l.ATime, r.ATime, opts)
	n.Compare.CTime = cmpTime(l.CTime, r.CTime, opts)
	n.Compare.BirthTime = cmpTime(l.BirthTime, r.BirthTime, opts)
	n.Compare.Mode = cmpAttr(l.Mode == r.Mode)
}

func cmpTime(a, b time.Time, opts CompareOpts) AttrStatus {
	if !opts.SubSecond {
		a = a.Truncate(time.Second)
		b = b.Truncate(time.Second)
	}
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	if opts.IgnoreTZDST {
		diff %= time.Hour
		if diff > 30*time.Minute {
			diff = time.Hour - diff
		}
	}
	if opts.TimeGrace {
		return cmpAttr(diff <= time.Second)
	}
	return cmpAttr(diff == 0)
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
	FilesEqual, FilesDiff int
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
		node.Totals = [2]SideTotals{}
		node.SubtreePending = true
		node.SubtreeBothFiles = 0
		node.SubtreeChecksumPending = 0
		node.SubtreeChecksumAnyDiff = false
		return AttrUnknown
	}
	result := AttrEqual
	var tot [2]SideTotals
	pending := false
	bothFiles := 0
	cksumPending := 0
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
				tot[s].add(child.Totals[s])
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
			if child.Compare.Size == AttrEqual && child.Compare.ModTime == AttrEqual {
				st.FilesEqual++
			} else {
				st.FilesDiff++
			}
		case PresenceLeftOnly:
			st.FilesLeftOnly++
		case PresenceRightOnly:
			st.FilesRightOnly++
		}
	}
	node.Totals = tot
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

// NodeStatus folds the attribute results opts enables into one status for a
// file present on both sides.
func NodeStatus(node *TreeNode, opts *CompareOpts) AttrStatus {
	if node.Compare.Presence != PresenceBoth {
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
		w := 0
		if !inactive {
			w = timeWin(get(l), get(r))
		}
		attrs = append(attrs, attr{label, lv, rv, lraw, rraw, status, inactive, w})
	}

	ls, rs := val(func(e *FileEntry) string { return FormatSize(e.Size) })
	if node.Compare.Size == AttrDifferent && ls == rs && l != nil && r != nil {
		ls = fmt.Sprintf("%d", l.Size)
		rs = fmt.Sprintf("%d", r.Size)
	}
	lsraw, rsraw := rawStr(func(e *FileEntry) string { return fmt.Sprintf("%d", e.Size) })
	attrs = append(attrs, attr{"size", ls, rs, lsraw, rsraw, node.Compare.Size, false, sizeWin})
	addTime("mtime", func(e *FileEntry) time.Time { return e.ModTime }, node.Compare.ModTime, opts != nil && opts.ModTime)
	addTime("atime", func(e *FileEntry) time.Time { return e.ATime }, node.Compare.ATime, opts != nil && opts.ATime)
	addTime("ctime", func(e *FileEntry) time.Time { return e.CTime }, node.Compare.CTime, opts != nil && opts.CTime)
	addTime("btime", func(e *FileEntry) time.Time { return e.BirthTime }, node.Compare.BirthTime, opts != nil && opts.BirthTime)
	lv, rv := val(func(e *FileEntry) string { return e.Mode.String() })
	lpraw, rpraw := rawStr(func(e *FileEntry) string { return fmt.Sprintf("0%o", e.Mode.Perm()) })
	attrs = append(attrs, attr{"perm", lv, rv, lpraw, rpraw, node.Compare.Mode, false, 0})
	lc, rc := node.Sides[SideLeft].Checksum, node.Sides[SideRight].Checksum
	if lc == "" {
		lc = "-"
	}
	if rc == "" {
		rc = "-"
	}
	attrs = append(attrs, attr{"cksum", lc, rc, "", "", node.Compare.Checksum, false, 0})

	for i, a := range attrs {
		*flat = append(*flat, Row{Node: node, Attr: &AttrRow{
			Label: a.label, Val: [2]string{a.leftVal, a.rightVal}, Raw: [2]string{a.leftRaw, a.rightRaw},
			Status: a.status, Inactive: a.inactive, Winner: a.winner,
			Guides: kg, IsLast: i == len(attrs)-1, Depth: node.Depth + 1,
		}})
	}
}

func CollectCopyFiles(node *TreeNode, opts *CompareOpts, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	collectCopyFilesRec(node, opts, leftToRight, &result)
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

func collectCopyFilesRec(node *TreeNode, opts *CompareOpts, leftToRight bool, result *[]*TreeNode) {
	from, to := CopySides(leftToRight)
	src, dst := node.Sides[from].Entry, node.Sides[to].Entry
	srcIsDir := src != nil && src.IsDir
	if !srcIsDir {
		switch node.Compare.Presence {
		case PresenceBoth:
			collision := dst != nil && src != nil && src.IsDir != dst.IsDir
			if collision || NodeStatus(node, opts) != AttrEqual || node.Compare.Checksum == AttrDifferent {
				*result = append(*result, node)
			}
		case PresenceLeftOnly:
			if leftToRight {
				*result = append(*result, node)
			}
		case PresenceRightOnly:
			if !leftToRight {
				*result = append(*result, node)
			}
		}
		return
	}
	if node.Listed && len(node.Children) == 0 {
		switch node.Compare.Presence {
		case PresenceLeftOnly:
			if leftToRight {
				*result = append(*result, node)
			}
		case PresenceRightOnly:
			if !leftToRight {
				*result = append(*result, node)
			}
		}
		return
	}
	for _, child := range node.Children {
		collectCopyFilesRec(child, opts, leftToRight, result)
	}
}

// CollectTypeCollisions returns dst-side nodes that have a same-name twin on the
// src side with the opposite type (file vs directory). These twins must be
// removed on dst before a copy can succeed. Walks the input node's subtree and
// also checks the input node's own siblings (so single-file copies work).
func CollectTypeCollisions(node *TreeNode, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	seen := make(map[*TreeNode]bool)
	if node.Parent != nil {
		findTwinPairs(node.Parent.Children, leftToRight, seen, &result)
	}
	collectTypeCollisionsRec(node, leftToRight, seen, &result)
	return result
}

func findTwinPairs(children []*TreeNode, leftToRight bool, seen map[*TreeNode]bool, result *[]*TreeNode) {
	src, dst := CopySides(leftToRight)
	byName := make(map[string][]*TreeNode)
	for _, c := range children {
		byName[c.Name] = append(byName[c.Name], c)
	}
	for _, group := range byName {
		if len(group) < 2 {
			continue
		}
		for _, a := range group {
			if a.Sides[src].Entry == nil {
				continue
			}
			for _, b := range group {
				if a == b || a.IsDir == b.IsDir {
					continue
				}
				if b.Sides[dst].Entry == nil || seen[b] {
					continue
				}
				seen[b] = true
				*result = append(*result, b)
			}
		}
	}
}

func collectTypeCollisionsRec(node *TreeNode, leftToRight bool, seen map[*TreeNode]bool, result *[]*TreeNode) {
	findTwinPairs(node.Children, leftToRight, seen, result)
	for _, child := range node.Children {
		if child.IsDir {
			collectTypeCollisionsRec(child, leftToRight, seen, result)
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

func CountMirrorDeletes(node *TreeNode, leftToRight bool) (files, dirs int) {
	_, dst := CopySides(leftToRight)
	destOnly := dst.Only()
	for _, child := range node.Children {
		if child.Compare.Presence == destOnly {
			if hasTwinWithSrc(node, child, leftToRight) {
				continue
			}
			if child.IsDir {
				dirs++
				cf, cd, _ := CountDescendants(child)
				files += cf
				dirs += cd
			} else {
				files++
			}
			continue
		}
		if child.IsDir {
			cf, cd := CountMirrorDeletes(child, leftToRight)
			files += cf
			dirs += cd
		}
	}
	return
}

// hasTwinWithSrc reports whether child has a sibling with the same Name but
// opposite IsDir, where that sibling has a src-side entry. Used to exclude
// type-collision twins from mirror-delete logic — they're cleaned up by
// CollectTypeCollisions and would otherwise be double-processed (and risk
// removing the just-copied file at the same path).
func hasTwinWithSrc(parent, child *TreeNode, leftToRight bool) bool {
	src, _ := CopySides(leftToRight)
	for _, sibling := range parent.Children {
		if sibling == child || sibling.Name != child.Name || sibling.IsDir == child.IsDir {
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

// FindNextDiff walks the tree in DFS order starting after `after` and returns
// the next node that represents a "diff leaf" — a file whose compared
// attributes differ, or any item present on only one side. Directories present
// on both sides aren't leaves; the search drills into them. If `after` is
// itself a diff leaf, its subtree is skipped (the dir's existence is the
// diff). Returns nil if no further diff is found.
func FindNextDiff(root, after *TreeNode, opts *CompareOpts) *TreeNode {
	if root == nil {
		return nil
	}
	started := after == nil
	var visit func(n *TreeNode) *TreeNode
	visit = func(n *TreeNode) *TreeNode {
		if started && isDiffLeaf(n, opts) {
			return n
		}
		if n == after {
			started = true
			if isDiffLeaf(n, opts) {
				return nil
			}
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
