package ui

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

type Panel struct {
	title   string
	rows    []model.Row
	cursor  int
	offset  int
	width   int
	height  int
	active  bool
	isLeft  bool
	wrap    bool
	cmpOpts *model.CompareOpts
	spinner string
}

func NewPanel(title string) *Panel {
	return &Panel{title: title}
}

// SetRows replaces the visible rows, keeping the cursor on the same row when
// it still exists.
func (p *Panel) SetRows(rows []model.Row) {
	anchor, hasAnchor := p.CursorRow()
	p.rows = rows
	if hasAnchor {
		for i, r := range rows {
			if sameRow(r, anchor) {
				p.cursor = i
				p.clampOffset()
				return
			}
		}
	}
	if p.cursor >= len(p.rows) {
		p.cursor = max(0, len(p.rows)-1)
	}
	p.clampOffset()
}

// sameRow matches rows across rebuilds: attribute rows are recreated every
// frame, so they compare by file and label.
func sameRow(a, b model.Row) bool {
	if a.Node != b.Node || (a.Attr == nil) != (b.Attr == nil) {
		return false
	}
	return a.Attr == nil || a.Attr.Label == b.Attr.Label
}

func (p *Panel) CursorRow() (model.Row, bool) {
	if p.cursor < 0 || p.cursor >= len(p.rows) {
		return model.Row{}, false
	}
	return p.rows[p.cursor], true
}

// CursorNode is the node under the cursor; nil on an attribute row.
func (p *Panel) CursorNode() *model.TreeNode {
	r, ok := p.CursorRow()
	if !ok || r.Attr != nil {
		return nil
	}
	return r.Node
}

// CursorFile is the node under the cursor, or the file an attribute row
// belongs to.
func (p *Panel) CursorFile() *model.TreeNode {
	r, _ := p.CursorRow()
	return r.Node
}

func (p *Panel) MoveUp() {
	p.cursor = max(p.cursor-1, 0)
	p.clampOffset()
}

func (p *Panel) MoveDown() {
	p.cursor = max(min(p.cursor+1, len(p.rows)-1), 0)
	p.clampOffset()
}

func (p *Panel) PageUp() {
	p.cursor = max(p.cursor-p.height, 0)
	p.clampOffset()
}

func (p *Panel) PageDown() {
	p.cursor = max(min(p.cursor+p.height, len(p.rows)-1), 0)
	p.clampOffset()
}

func (p *Panel) Toggle() {
	if node := p.CursorNode(); node != nil {
		node.Expanded = !node.Expanded
	}
}

func (p *Panel) clampOffset() {
	if p.height <= 0 {
		return
	}
	if p.cursor < p.offset {
		p.offset = p.cursor
	}
	if p.cursor >= p.offset+p.height {
		p.offset = p.cursor - p.height + 1
	}
}

var (
	styleEqual     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleDifferent = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleUnknown   = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleChrome    = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	styleDir       = lipgloss.NewStyle()
	styleCursor    = lipgloss.NewStyle().Reverse(true)
	styleScanning  = lipgloss.NewStyle().Foreground(lipgloss.Color("5"))
)

func (p *Panel) View() string {
	visible := max(p.height, 0)
	end := min(p.offset+visible, len(p.rows))
	var sb strings.Builder
	for i := p.offset; i < end; i++ {
		line := p.renderRow(p.rows[i])
		if !p.wrap {
			line = ansi.Truncate(line, p.width, "")
		}
		if i == p.cursor {
			line = styleCursor.Render(padRight(ansi.Strip(line), p.width))
		} else {
			line = padRight(line, p.width)
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	for i := end - p.offset; i < visible; i++ {
		sb.WriteString(strings.Repeat(" ", p.width))
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

// isHidden reports whether node has nothing on this panel's side.
func (p *Panel) isHidden(node *model.TreeNode) bool {
	if p.isLeft {
		return node.Compare.Presence == model.PresenceRightOnly
	}
	return node.Compare.Presence == model.PresenceLeftOnly
}

func (p *Panel) renderRow(r model.Row) string {
	if r.Attr != nil {
		prefix := ""
		if !p.isLeft {
			prefix = " "
		}
		if p.isHidden(r.Node) {
			return prefix + renderGuidesOnly(r.Attr.Guides, r.Attr.Depth, r.Attr.IsLast)
		}
		return prefix + p.renderAttrRow(r.Attr)
	}
	node := r.Node
	if p.isHidden(node) {
		return p.eqPrefix(node) + renderGuidesOnly(node.Guides, node.Depth, node.IsLast)
	}

	entry := node.Left
	if !p.isLeft {
		entry = node.Right
	}
	sideIsDir := entry != nil && entry.IsDir

	name := node.Name
	if sideIsDir {
		name = p.dirStyle(node).Render(name + "/")
	} else {
		name = p.nodeStyle(node).Render(name)
	}
	arrow := "▶"
	if node.Expanded {
		arrow = "▼"
	}

	var left string
	if node.Depth == 0 {
		spin := ""
		if p.spinner != "" {
			spin = " " + p.spinner
		}
		left = p.eqPrefix(node) + styleChrome.Render(arrow) + spin + " " + p.dirStyle(node).Render(p.title)
	} else {
		var pendingCksum, activeCksum bool
		if sideIsDir {
			if p.isLeft {
				pendingCksum = node.ChecksumPendingLeft
				activeCksum = node.ChecksumActiveLeft || atomic.LoadInt32(&node.ChecksumInFlightLeft) > 0
			} else {
				pendingCksum = node.ChecksumPendingRight
				activeCksum = node.ChecksumActiveRight || atomic.LoadInt32(&node.ChecksumInFlightRight) > 0
			}
		}
		switch {
		case sideIsDir && node.ListErr:
			arrow = "▶!"
		case sideIsDir && !node.Listed:
			arrow = "▶…"
		case activeCksum && p.spinner != "":
			arrow += p.spinner
		case pendingCksum:
			arrow += "≈"
		case sideIsDir && node.SubtreePending && p.spinner != "":
			arrow += p.spinner
		}
		left = p.eqPrefix(node) + renderGuides(node.Guides, node.Depth, node.IsLast) + styleChrome.Render(arrow) + " " + name
	}

	info := p.inlineInfo(node)
	if info == "" {
		return left
	}
	infoLen := lipgloss.Width(info)
	if maxLeft := p.width - infoLen - 1; lipgloss.Width(left) > maxLeft && maxLeft > 0 {
		left = ansi.Truncate(left, maxLeft, "")
	}
	gap := max(p.width-lipgloss.Width(left)-infoLen, 1)
	return left + strings.Repeat(" ", gap) + info
}

// eqPrefix is the right panel's leading glyph summarising how the row
// compares: ≡/≢ once checksums are known, =/≠ before that.
func (p *Panel) eqPrefix(node *model.TreeNode) string {
	if p.isLeft {
		return ""
	}
	if node.Depth == 0 || node.Right == nil {
		return " "
	}
	if !node.Right.IsDir {
		if node.Compare.Presence != model.PresenceBoth {
			return " "
		}
		switch node.Compare.Checksum {
		case model.AttrDifferent:
			return styleDifferent.Render("≢")
		case model.AttrEqual:
			if fileOtherAttrsDiffer(node, p.cmpOpts) {
				return styleDifferent.Render("≢")
			}
			return styleEqual.Render("≡")
		}
		return " "
	}
	if node.Compare.Presence != model.PresenceBoth {
		return styleDifferent.Render("≠")
	}
	scanned := node.SubtreeChecksumScanned()
	switch node.ChildStatus {
	case model.AttrEqual:
		if !scanned {
			return styleEqual.Render("=")
		}
		if node.SubtreeChecksumAnyDiff {
			return styleDifferent.Render("≢")
		}
		return styleEqual.Render("≡")
	case model.AttrDifferent:
		if scanned {
			return styleDifferent.Render("≢")
		}
		return styleDifferent.Render("≠")
	}
	return " "
}

// fileOtherAttrsDiffer reports a difference in any enabled attribute other
// than the checksum.
func fileOtherAttrsDiffer(n *model.TreeNode, opts *model.CompareOpts) bool {
	if opts == nil {
		return false
	}
	c := n.Compare
	for _, a := range []struct {
		on bool
		s  model.AttrStatus
	}{{opts.Size, c.Size}, {opts.ModTime, c.ModTime}, {opts.ATime, c.ATime}, {opts.CTime, c.CTime}, {opts.BirthTime, c.BirthTime}, {opts.Mode, c.Mode}} {
		if a.on && a.s == model.AttrDifferent {
			return true
		}
	}
	return false
}

func (p *Panel) renderAttrRow(a *model.AttrRow) string {
	chrome := renderGuides(a.Guides, a.Depth, a.IsLast)
	activeStyle := styleUnknown
	if !a.Inactive {
		switch a.Status {
		case model.AttrEqual:
			activeStyle = styleEqual
		case model.AttrDifferent:
			activeStyle = styleDifferent
		}
	}
	label := activeStyle.Render(fmt.Sprintf("%-5s", a.Label))

	var st string
	switch a.Status {
	case model.AttrNA:
		st = styleUnknown.Render("-")
	case model.AttrScanning:
		st = styleScanning.Render("=")
	case model.AttrDifferent:
		st = activeStyle.Render("≠")
	default:
		st = activeStyle.Render("=")
	}
	if a.LeftVal == "" && a.RightVal == "" {
		return fmt.Sprintf("%s %s %s", chrome, label, st)
	}

	val, raw, win := a.LeftVal, a.LeftRaw, a.Winner
	if !p.isLeft {
		val, raw, win = a.RightVal, a.RightRaw, -win
	}
	if a.Status == model.AttrDifferent && win != 0 {
		if win < 0 {
			val = styleEqual.Render(val)
		} else {
			val = styleDifferent.Render(val)
		}
	}
	if raw != "" && val != "-" {
		val += styleChrome.Render(" [" + raw + "]")
	}
	return fmt.Sprintf("%s %s %s  %s", chrome, label, st, val)
}

func (p *Panel) inlineInfo(node *model.TreeNode) string {
	entry := node.Left
	if !p.isLeft {
		entry = node.Right
	}
	if entry == nil {
		return ""
	}
	if !entry.IsDir {
		return styleChrome.Render(fmt.Sprintf("%8s %7s", model.TimeAgo(entry.ModTime), model.FormatSize(entry.Size)))
	}
	dirs, files, size := node.LeftTotalDirs, node.LeftTotalFiles, node.LeftTotalSize
	if !p.isLeft {
		dirs, files, size = node.RightTotalDirs, node.RightTotalFiles, node.RightTotalSize
	}
	if dirs == 0 && files == 0 {
		return ""
	}
	return styleChrome.Render(fmt.Sprintf("%4dd %5df %7s", dirs, files, model.FormatSize(size)))
}

func (p *Panel) dirStyle(node *model.TreeNode) lipgloss.Style {
	if node.Compare.Presence != model.PresenceBoth || node.SubtreeChecksumAnyDiff {
		return styleDir.Foreground(styleDifferent.GetForeground())
	}
	switch node.ChildStatus {
	case model.AttrEqual:
		return styleDir.Foreground(styleEqual.GetForeground())
	case model.AttrDifferent:
		return styleDir.Foreground(styleDifferent.GetForeground())
	}
	return styleDir
}

func (p *Panel) nodeStyle(node *model.TreeNode) lipgloss.Style {
	if node.Compare.Presence != model.PresenceBoth || node.Compare.Checksum == model.AttrDifferent {
		return styleDifferent
	}
	switch model.NodeStatus(node, p.cmpOpts) {
	case model.AttrDifferent:
		return styleDifferent
	case model.AttrEqual:
		return styleEqual
	}
	return styleUnknown
}

// guideColumns writes the ancestor guide columns, bits [1, depth) of the
// guide mask, leaving the caller to add this row's own corner.
func guideColumns(sb *strings.Builder, guides uint64, depth int) {
	for i := 1; i < min(depth, 64); i++ {
		if guides&(1<<uint(i)) != 0 {
			sb.WriteString("│")
			continue
		}
		sb.WriteString(" ")
	}
}

func renderGuides(guides uint64, depth int, isLast bool) string {
	var sb strings.Builder
	guideColumns(&sb, guides, depth)
	if isLast {
		sb.WriteString("└")
	} else {
		sb.WriteString("├")
	}
	return styleChrome.Render(sb.String())
}

func renderGuidesOnly(guides uint64, depth int, isLast bool) string {
	var sb strings.Builder
	guideColumns(&sb, guides, depth)
	if isLast {
		sb.WriteString(" ")
	} else {
		sb.WriteString("│")
	}
	return styleChrome.Render(sb.String())
}

func padRight(s string, width int) string {
	return s + strings.Repeat(" ", max(width-lipgloss.Width(s), 0))
}

// jumpTo puts the cursor on node's row.
func (p *Panel) jumpTo(node *model.TreeNode) {
	for i, r := range p.rows {
		if r.Attr == nil && r.Node == node {
			p.cursor = i
			p.clampOffset()
			return
		}
	}
}
