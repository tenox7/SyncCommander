package main

import (
	"fmt"
	"sort"
	"time"
)

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
	RelPath  string
	Name     string
	IsDir    bool
	Left     *FileEntry
	Right    *FileEntry
	Compare  CompareResult
	Children []*TreeNode
	Expanded bool
	Listed   bool
	Depth    int
	ChildStatus    AttrStatus
	LeftChecksum   string
	RightChecksum  string
	LeftTotalSize  int64
	RightTotalSize int64
	Guides       []bool
	IsLast       bool
	IsAttr       bool
	AttrLabel    string
	AttrLeftVal  string
	AttrRightVal string
	AttrStatus   AttrStatus
	AttrWinner   int
	AttrPresence Presence
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

func MergeChildren(parent *TreeNode, leftEntries, rightEntries []FileEntry, depth int, subSecond bool) []*TreeNode {
	byName := make(map[string]*TreeNode)

	for i := range leftEntries {
		e := &leftEntries[i]
		node := byName[e.Name]
		if node == nil {
			node = &TreeNode{
				RelPath: e.RelPath,
				Name:    e.Name,
				IsDir:   e.IsDir,
				Depth:   depth,
			}
			byName[e.Name] = node
		}
		node.Left = e
	}

	for i := range rightEntries {
		e := &rightEntries[i]
		node := byName[e.Name]
		if node == nil {
			node = &TreeNode{
				RelPath: e.RelPath,
				Name:    e.Name,
				IsDir:   e.IsDir,
				Depth:   depth,
			}
			byName[e.Name] = node
		}
		node.Right = e
		if e.IsDir {
			node.IsDir = true
		}
	}

	nodes := make([]*TreeNode, 0, len(byName))
	for _, n := range byName {
		compareNode(n, subSecond)
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

func compareNode(n *TreeNode, subSecond bool) {
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
	n.Compare.ModTime = cmpTime(n.Left.ModTime, n.Right.ModTime, subSecond)
	n.Compare.ATime = cmpTime(n.Left.ATime, n.Right.ATime, subSecond)
	n.Compare.CTime = cmpTime(n.Left.CTime, n.Right.CTime, subSecond)
	n.Compare.BirthTime = cmpTime(n.Left.BirthTime, n.Right.BirthTime, subSecond)
	n.Compare.Mode = cmpAttr(n.Left.Mode == n.Right.Mode)
}

func cmpTime(a, b time.Time, subSecond bool) AttrStatus {
	if !subSecond {
		a = a.Truncate(time.Second)
		b = b.Truncate(time.Second)
	}
	return cmpAttr(a.Equal(b))
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
		return nodeStatus(node, opts)
	}
	if !node.Listed {
		node.ChildStatus = AttrUnknown
		node.LeftTotalSize = 0
		node.RightTotalSize = 0
		return AttrUnknown
	}
	result := AttrEqual
	var lt, rt int64
	for _, child := range node.Children {
		s := propagateStatus(child, opts)
		if s == AttrDifferent {
			result = AttrDifferent
		} else if s == AttrUnknown && result != AttrDifferent {
			result = AttrUnknown
		}
		if child.IsDir {
			lt += child.LeftTotalSize
			rt += child.RightTotalSize
		} else {
			if child.Left != nil {
				lt += child.Left.Size
			}
			if child.Right != nil {
				rt += child.Right.Size
			}
		}
	}
	node.LeftTotalSize = lt
	node.RightTotalSize = rt
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
		status   AttrStatus
		winner   int
	}

	tf := "2006-01-02 15:04:05"
	if opts != nil && opts.SubSecond {
		tf = "2006-01-02 15:04:05.000000000"
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

	addTime := func(label string, get func(*FileEntry) time.Time, status AttrStatus) {
		lv, rv := val(func(e *FileEntry) string { return get(e).Format(tf) })
		w := 0
		if l != nil && r != nil {
			w = timeWin(get(l), get(r))
		}
		attrs = append(attrs, attr{label, lv, rv, status, w})
	}

	ls, rs := val(func(e *FileEntry) string { return formatSize(e.Size) })
	if node.Compare.Size == AttrDifferent && ls == rs && l != nil && r != nil {
		ls = fmt.Sprintf("%d", l.Size)
		rs = fmt.Sprintf("%d", r.Size)
	}
	attrs = append(attrs, attr{"size", ls, rs, node.Compare.Size, sizeWin})
	addTime("mtime", func(e *FileEntry) time.Time { return e.ModTime }, node.Compare.ModTime)
	addTime("atime", func(e *FileEntry) time.Time { return e.ATime }, node.Compare.ATime)
	addTime("ctime", func(e *FileEntry) time.Time { return e.CTime }, node.Compare.CTime)
	addTime("btime", func(e *FileEntry) time.Time { return e.BirthTime }, node.Compare.BirthTime)
	lv, rv := val(func(e *FileEntry) string { return e.Mode.String() })
	attrs = append(attrs, attr{"perm", lv, rv, node.Compare.Mode, 0})
	lc := node.LeftChecksum
	rc := node.RightChecksum
	if lc == "" {
		lc = "-"
	}
	if rc == "" {
		rc = "-"
	}
	attrs = append(attrs, attr{"cksum", lc, rc, node.Compare.Checksum, 0})

	for i, a := range attrs {
		row := &TreeNode{
			IsAttr:       true,
			AttrLabel:    a.label,
			AttrLeftVal:  a.leftVal,
			AttrRightVal: a.rightVal,
			AttrStatus:   a.status,
			AttrWinner:   a.winner,
			AttrPresence: node.Compare.Presence,
			Guides:       childGuides,
			IsLast:       i == len(attrs)-1,
			Depth:        node.Depth + 1,
		}
		*flat = append(*flat, row)
	}
}

func collectCopyFiles(node *TreeNode, opts *CompareOpts, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	collectCopyFilesRec(node, opts, leftToRight, &result)
	return result
}

func collectCopyFilesRec(node *TreeNode, opts *CompareOpts, leftToRight bool, result *[]*TreeNode) {
	if node.IsAttr {
		return
	}
	if !node.IsDir {
		switch node.Compare.Presence {
		case PresenceBoth:
			if nodeStatus(node, opts) != AttrEqual {
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
	for _, child := range node.Children {
		collectCopyFilesRec(child, opts, leftToRight, result)
	}
}

func collectMirrorDeletes(node *TreeNode, leftToRight bool) []*TreeNode {
	var result []*TreeNode
	destOnly := PresenceRightOnly
	if !leftToRight {
		destOnly = PresenceLeftOnly
	}
	for _, child := range node.Children {
		if child.Compare.Presence == destOnly {
			result = append(result, child)
			continue
		}
		if child.IsDir {
			result = append(result, collectMirrorDeletes(child, leftToRight)...)
		}
	}
	return result
}

func countMirrorDeletes(node *TreeNode, leftToRight bool) (files, dirs int) {
	destOnly := PresenceRightOnly
	if !leftToRight {
		destOnly = PresenceLeftOnly
	}
	for _, child := range node.Children {
		if child.Compare.Presence == destOnly {
			if child.IsDir {
				dirs++
				cf, cd, _ := countDescendants(child)
				files += cf
				dirs += cd
			} else {
				files++
			}
			continue
		}
		if child.IsDir {
			cf, cd := countMirrorDeletes(child, leftToRight)
			files += cf
			dirs += cd
		}
	}
	return
}

func countDescendants(node *TreeNode) (files, dirs int, complete bool) {
	complete = true
	if !node.Listed {
		return 0, 0, false
	}
	for _, child := range node.Children {
		if child.IsDir {
			dirs++
			cf, cd, cc := countDescendants(child)
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

func formatSize(b int64) string {
	switch {
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
