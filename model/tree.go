package model

import (
	"fmt"
	"sort"
	"time"
)

// CompareOpts controls which file attributes are compared.
type CompareOpts struct {
	Size        bool
	ModTime     bool
	ATime       bool
	CTime       bool
	BTime       bool
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

type TreeNode struct {
	RelPath         string
	Name            string
	IsDir           bool
	Left            *FileEntry
	Right           *FileEntry
	Parent          *TreeNode
	Compare         CompareResult
	Children        []*TreeNode
	Expanded        bool
	Listed          bool
	SubtreePending  bool
	Depth           int
	ChildStatus     AttrStatus
	LeftChecksum    string
	RightChecksum   string
	LeftTotalSize   int64
	RightTotalSize  int64
	LeftTotalFiles  int
	RightTotalFiles int
	LeftTotalDirs   int
	RightTotalDirs  int
	Guides          []bool
	IsLast          bool
	IsAttr          bool
	AttrLabel       string
	AttrLeftVal     string
	AttrRightVal    string
	AttrLeftRaw     string
	AttrRightRaw    string
	AttrStatus      AttrStatus
	AttrInactive    bool
	AttrWinner      int
	AttrPresence    Presence
}

func (n *TreeNode) OverallStatus() AttrStatus {
	if n.Compare.Presence != PresenceBoth {
		return AttrDifferent
	}
	attrs := []AttrStatus{n.Compare.Size, n.Compare.ModTime, n.Compare.ATime, n.Compare.CTime, n.Compare.BirthTime, n.Compare.Mode, n.Compare.Checksum}
	hasUnknown := false
	for _, a := range attrs {
		if a == AttrDifferent {
			return AttrDifferent
		}
		if a == AttrUnknown || a == AttrScanning {
			hasUnknown = true
		}
	}
	if hasUnknown {
		return AttrUnknown
	}
	return AttrEqual
}

func NewRootNode() *TreeNode {
	return &TreeNode{Name: "/", IsDir: true, Expanded: true, Listed: true}
}

func MergeChildren(parent *TreeNode, leftEntries, rightEntries []FileEntry, depth int, subSecond, timeGrace, ignoreTZDST bool) []*TreeNode {
	keyFor := func(name string, isDir bool) string {
		if isDir {
			return name + "/"
		}
		return name
	}
	byKey := make(map[string]*TreeNode)

	for i := range leftEntries {
		e := &leftEntries[i]
		key := keyFor(e.Name, e.IsDir)
		node := byKey[key]
		if node == nil {
			node = &TreeNode{
				RelPath: e.RelPath,
				Name:    e.Name,
				IsDir:   e.IsDir,
				Depth:   depth,
				Parent:  parent,
			}
			byKey[key] = node
		}
		node.Left = e
	}

	for i := range rightEntries {
		e := &rightEntries[i]
		key := keyFor(e.Name, e.IsDir)
		node := byKey[key]
		if node == nil {
			node = &TreeNode{
				RelPath: e.RelPath,
				Name:    e.Name,
				IsDir:   e.IsDir,
				Depth:   depth,
				Parent:  parent,
			}
			byKey[key] = node
		}
		node.Right = e
	}

	nodes := make([]*TreeNode, 0, len(byKey))
	for _, n := range byKey {
		compareNode(n, subSecond, timeGrace, ignoreTZDST)
		nodes = append(nodes, n)
	}

	sort.Slice(nodes, func(i, j int) bool {
		a, b := nodes[i], nodes[j]
		if a.IsDir != b.IsDir {
			return a.IsDir
		}
		return a.Name < b.Name
	})

	return nodes
}

func compareNode(n *TreeNode, subSecond, timeGrace, ignoreTZDST bool) {
	n.Compare.Presence = PresenceBoth
	if n.Left == nil {
		n.Compare.Presence = PresenceRightOnly
		return
	}
	if n.Right == nil {
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
	n.Compare.Size = cmpAttr(n.Left.Size == n.Right.Size)
	n.Compare.ModTime = cmpTime(n.Left.ModTime, n.Right.ModTime, subSecond, timeGrace, ignoreTZDST)
	n.Compare.ATime = cmpTime(n.Left.ATime, n.Right.ATime, subSecond, timeGrace, ignoreTZDST)
	n.Compare.CTime = cmpTime(n.Left.CTime, n.Right.CTime, subSecond, timeGrace, ignoreTZDST)
	n.Compare.BirthTime = cmpTime(n.Left.BirthTime, n.Right.BirthTime, subSecond, timeGrace, ignoreTZDST)
	n.Compare.Mode = cmpAttr(n.Left.Mode == n.Right.Mode)
}

func cmpTime(a, b time.Time, subSecond, timeGrace, ignoreTZDST bool) AttrStatus {
	if !subSecond {
		a = a.Truncate(time.Second)
		b = b.Truncate(time.Second)
	}
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	if ignoreTZDST {
		diff %= time.Hour
		if diff > 30*time.Minute {
			diff = time.Hour - diff
		}
	}
	if timeGrace {
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

func FlattenTree(root *TreeNode, opts *CompareOpts) []*TreeNode {
	propagateStatus(root, opts)
	var flat []*TreeNode
	root.IsLast = true
	root.Guides = nil
	flat = append(flat, root)
	if !root.Expanded {
		return flat
	}
	childGuides := []bool{false}
	for i, child := range root.Children {
		child.IsLast = i == len(root.Children)-1
		flattenNode(child, childGuides, opts, &flat)
	}
	return flat
}

func propagateStatus(node *TreeNode, opts *CompareOpts) AttrStatus {
	if !node.IsDir {
		node.SubtreePending = false
		return nodeStatus(node, opts)
	}
	if !node.Listed {
		node.ChildStatus = AttrUnknown
		node.LeftTotalSize = 0
		node.RightTotalSize = 0
		node.LeftTotalFiles = 0
		node.RightTotalFiles = 0
		node.LeftTotalDirs = 0
		node.RightTotalDirs = 0
		node.SubtreePending = true
		return AttrUnknown
	}
	result := AttrEqual
	var lt, rt int64
	var lf, rf, ld, rd int
	pending := false
	for _, child := range node.Children {
		s := propagateStatus(child, opts)
		if s == AttrDifferent {
			result = AttrDifferent
		} else if s == AttrUnknown && result != AttrDifferent {
			result = AttrUnknown
		}
		if child.IsDir && child.SubtreePending {
			pending = true
		}
		if child.IsDir {
			lt += child.LeftTotalSize
			rt += child.RightTotalSize
			lf += child.LeftTotalFiles
			rf += child.RightTotalFiles
			ld += child.LeftTotalDirs
			rd += child.RightTotalDirs
			if child.Left != nil {
				ld++
			}
			if child.Right != nil {
				rd++
			}
		} else {
			if child.Left != nil {
				lt += child.Left.Size
				lf++
			}
			if child.Right != nil {
				rt += child.Right.Size
				rf++
			}
		}
	}
	node.LeftTotalSize = lt
	node.RightTotalSize = rt
	node.LeftTotalFiles = lf
	node.RightTotalFiles = rf
	node.LeftTotalDirs = ld
	node.RightTotalDirs = rd
	node.SubtreePending = pending
	if node.Compare.Presence != PresenceBoth {
		node.ChildStatus = AttrDifferent
		return AttrDifferent
	}
	node.ChildStatus = result
	return result
}

func nodeStatus(node *TreeNode, opts *CompareOpts) AttrStatus {
	if node.Compare.Presence != PresenceBoth {
		return AttrDifferent
	}
	if opts == nil {
		return AttrUnknown
	}
	attrs := []AttrStatus{}
	if opts.Size {
		attrs = append(attrs, node.Compare.Size)
	}
	if opts.ModTime {
		attrs = append(attrs, node.Compare.ModTime)
	}
	if opts.ATime {
		attrs = append(attrs, node.Compare.ATime)
	}
	if opts.CTime {
		attrs = append(attrs, node.Compare.CTime)
	}
	if opts.BTime {
		attrs = append(attrs, node.Compare.BirthTime)
	}
	if opts.Mode {
		attrs = append(attrs, node.Compare.Mode)
	}
	if opts.Checksum {
		attrs = append(attrs, node.Compare.Checksum)
	}
	hasUnknown := false
	for _, a := range attrs {
		if a == AttrDifferent {
			return AttrDifferent
		}
		if a == AttrUnknown || a == AttrScanning {
			hasUnknown = true
		}
	}
	if hasUnknown || len(attrs) == 0 {
		return AttrUnknown
	}
	return AttrEqual
}

func flattenNode(node *TreeNode, parentGuides []bool, opts *CompareOpts, flat *[]*TreeNode) {
	node.Guides = parentGuides
	*flat = append(*flat, node)
	if !node.Expanded {
		return
	}
	if node.IsDir {
		childGuides := make([]bool, len(parentGuides)+1)
		copy(childGuides, parentGuides)
		childGuides[len(parentGuides)] = !node.IsLast
		for i, child := range node.Children {
			child.IsLast = i == len(node.Children)-1
			flattenNode(child, childGuides, opts, flat)
		}
		return
	}
	flattenFileAttrs(node, parentGuides, opts, flat)
}

func flattenFileAttrs(node *TreeNode, parentGuides []bool, opts *CompareOpts, flat *[]*TreeNode) {
	childGuides := make([]bool, len(parentGuides)+1)
	copy(childGuides, parentGuides)
	childGuides[len(parentGuides)] = !node.IsLast

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
	l, r := node.Left, node.Right
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
	addTime("btime", func(e *FileEntry) time.Time { return e.BirthTime }, node.Compare.BirthTime, opts != nil && opts.BTime)
	lv, rv := val(func(e *FileEntry) string { return e.Mode.String() })
	lpraw, rpraw := rawStr(func(e *FileEntry) string { return fmt.Sprintf("0%o", e.Mode.Perm()) })
	attrs = append(attrs, attr{"perm", lv, rv, lpraw, rpraw, node.Compare.Mode, false, 0})
	lc := node.LeftChecksum
	rc := node.RightChecksum
	if lc == "" {
		lc = "-"
	}
	if rc == "" {
		rc = "-"
	}
	attrs = append(attrs, attr{"cksum", lc, rc, "", "", node.Compare.Checksum, false, 0})

	for i, a := range attrs {
		row := &TreeNode{
			IsAttr:       true,
			AttrLabel:    a.label,
			AttrLeftVal:  a.leftVal,
			AttrRightVal: a.rightVal,
			AttrLeftRaw:  a.leftRaw,
			AttrRightRaw: a.rightRaw,
			AttrStatus:   a.status,
			AttrInactive: a.inactive,
			AttrWinner:   a.winner,
			AttrPresence: node.Compare.Presence,
			Guides:       childGuides,
			IsLast:       i == len(attrs)-1,
			Depth:        node.Depth + 1,
		}
		*flat = append(*flat, row)
	}
}

func CollectCopyFiles(node *TreeNode, opts *CompareOpts, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	collectCopyFilesRec(node, opts, leftToRight, &result)
	return result
}

func collectCopyFilesRec(node *TreeNode, opts *CompareOpts, leftToRight bool, result *[]*TreeNode) {
	if node.IsAttr {
		return
	}
	src, dst := node.Left, node.Right
	if !leftToRight {
		src, dst = node.Right, node.Left
	}
	srcIsDir := src != nil && src.IsDir
	if !srcIsDir {
		switch node.Compare.Presence {
		case PresenceBoth:
			collision := dst != nil && src != nil && src.IsDir != dst.IsDir
			if collision || nodeStatus(node, opts) != AttrEqual {
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
	byName := make(map[string][]*TreeNode)
	for _, c := range children {
		if c.IsAttr {
			continue
		}
		byName[c.Name] = append(byName[c.Name], c)
	}
	for _, group := range byName {
		if len(group) < 2 {
			continue
		}
		for _, a := range group {
			srcEntry := a.Left
			if !leftToRight {
				srcEntry = a.Right
			}
			if srcEntry == nil {
				continue
			}
			for _, b := range group {
				if a == b || a.IsDir == b.IsDir {
					continue
				}
				dstEntry := b.Right
				if !leftToRight {
					dstEntry = b.Left
				}
				if dstEntry == nil || seen[b] {
					continue
				}
				seen[b] = true
				*result = append(*result, b)
			}
		}
	}
}

func collectTypeCollisionsRec(node *TreeNode, leftToRight bool, seen map[*TreeNode]bool, result *[]*TreeNode) {
	if node.IsAttr {
		return
	}
	findTwinPairs(node.Children, leftToRight, seen, result)
	for _, child := range node.Children {
		if child.IsDir {
			collectTypeCollisionsRec(child, leftToRight, seen, result)
		}
	}
}

func CollectMirrorDeletes(node *TreeNode, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	destOnly := PresenceRightOnly
	if !leftToRight {
		destOnly = PresenceLeftOnly
	}
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
	destOnly := PresenceRightOnly
	if !leftToRight {
		destOnly = PresenceLeftOnly
	}
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
	for _, sibling := range parent.Children {
		if sibling == child || sibling.IsAttr {
			continue
		}
		if sibling.Name != child.Name || sibling.IsDir == child.IsDir {
			continue
		}
		srcEntry := sibling.Left
		if !leftToRight {
			srcEntry = sibling.Right
		}
		if srcEntry != nil {
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
