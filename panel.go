package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

type Panel struct {
	title    string
	nodes    []*TreeNode
	cursor   int
	offset   int
	width    int
	height   int
	active   bool
	isLeft   bool
	wrap     bool
	cmpOpts  *CompareOpts
	spinner  string
}

func NewPanel(title string) *Panel {
	return &Panel{title: title}
}

func (p *Panel) SetNodes(nodes []*TreeNode) {
	p.nodes = nodes
	if p.cursor >= len(p.nodes) {
		p.cursor = max(0, len(p.nodes)-1)
	}
	p.clampOffset()
}

func (p *Panel) CursorNode() *TreeNode {
	if p.cursor < 0 || p.cursor >= len(p.nodes) {
		return nil
	}
	return p.nodes[p.cursor]
}

func (p *Panel) MoveUp() {
	if p.cursor > 0 {
		p.cursor--
	}
	p.clampOffset()
}

func (p *Panel) MoveDown() {
	if p.cursor < len(p.nodes)-1 {
		p.cursor++
	}
	p.clampOffset()
}

func (p *Panel) PageUp() {
	visible := p.visibleRows()
	p.cursor -= visible
	if p.cursor < 0 {
		p.cursor = 0
	}
	p.clampOffset()
}

func (p *Panel) PageDown() {
	visible := p.visibleRows()
	p.cursor += visible
	if p.cursor >= len(p.nodes) {
		p.cursor = len(p.nodes) - 1
	}
	if p.cursor < 0 {
		p.cursor = 0
	}
	p.clampOffset()
}

func (p *Panel) Toggle() {
	node := p.CursorNode()
	if node == nil || node.IsAttr {
		return
	}
	node.Expanded = !node.Expanded
}

func (p *Panel) visibleRows() int {
	return p.height
}

func (p *Panel) clampOffset() {
	visible := p.visibleRows()
	if visible <= 0 {
		return
	}
	if p.cursor < p.offset {
		p.offset = p.cursor
	}
	if p.cursor >= p.offset+visible {
		p.offset = p.cursor - visible + 1
	}
}

var (
	styleEqual     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleDifferent = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleUnknown   = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleChrome    = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	styleDir       = lipgloss.NewStyle()
	styleCursor = lipgloss.NewStyle().Reverse(true)
)

func (p *Panel) View() string {
	visible := p.visibleRows()
	if visible < 0 {
		visible = 0
	}

	end := p.offset + visible
	if end > len(p.nodes) {
		end = len(p.nodes)
	}

	var sb strings.Builder
	for i := p.offset; i < end; i++ {
		line := p.renderNode(p.nodes[i])
		if !p.wrap {
			line = ansi.Truncate(line, p.width, "")
		}
		if i == p.cursor {
			visLen := lipgloss.Width(line)
			pad := ""
			if visLen < p.width {
				pad = strings.Repeat(" ", p.width-visLen)
			}
			sb.WriteString(styleCursor.Render(ansi.Strip(line) + pad))
		} else {
			sb.WriteString(padRight(line, p.width))
		}
		sb.WriteString("\n")
	}

	for i := end - p.offset; i < visible; i++ {
		sb.WriteString(strings.Repeat(" ", p.width))
		sb.WriteString("\n")
	}

	return strings.TrimRight(sb.String(), "\n")
}

func (p *Panel) isHidden(node *TreeNode) bool {
	if node.IsAttr {
		if p.isLeft && node.AttrPresence == PresenceRightOnly {
			return true
		}
		if !p.isLeft && node.AttrPresence == PresenceLeftOnly {
			return true
		}
		return false
	}
	if p.isLeft && node.Compare.Presence == PresenceRightOnly {
		return true
	}
	if !p.isLeft && node.Compare.Presence == PresenceLeftOnly {
		return true
	}
	return false
}

func (p *Panel) renderNode(node *TreeNode) string {
	if p.isHidden(node) {
		return ""
	}

	if node.IsAttr {
		return p.renderAttrRow(node)
	}

	name := node.Name
	if node.IsDir {
		name = p.dirStyle(node).Render(name + "/")
	} else {
		name = p.nodeStyle(node).Render(name)
	}
	info := p.inlineInfo(node)

	var left string
	if node.Depth == 0 {
		arrow := "▶"
		if node.Expanded {
			arrow = "▼"
		}
		spin := ""
		if p.spinner != "" {
			spin = " " + p.spinner
		}
		left = styleChrome.Render(arrow) + spin + " " + p.dirStyle(node).Render(p.title)
	} else {
		chrome := renderGuides(node)
		arrow := "▶"
		if node.Expanded {
			arrow = "▼"
		}
		left = chrome + styleChrome.Render(arrow) + " " + name
	}

	if info == "" {
		return left
	}
	infoLen := lipgloss.Width(info)
	maxLeft := p.width - infoLen - 1
	leftLen := lipgloss.Width(left)
	if leftLen > maxLeft && maxLeft > 0 {
		left = ansi.Truncate(left, maxLeft, "")
		leftLen = lipgloss.Width(left)
	}
	gap := p.width - leftLen - infoLen
	if gap < 1 {
		gap = 1
	}
	return left + strings.Repeat(" ", gap) + info
}

func (p *Panel) renderAttrRow(node *TreeNode) string {
	chrome := renderGuides(node)

	label := styleUnknown.Render(fmt.Sprintf("%-5s", node.AttrLabel))
	st := attrChar("=", node.AttrStatus)
	if node.AttrStatus == AttrDifferent {
		st = attrChar("≠", node.AttrStatus)
	}

	left := node.AttrLeftVal
	right := node.AttrRightVal
	if left == "" && right == "" {
		return fmt.Sprintf("%s %s %s", chrome, label, st)
	}

	val := left
	win := node.AttrWinner
	if !p.isLeft {
		val = right
		win = -win
	}
	if node.AttrStatus == AttrDifferent && win != 0 {
		if win < 0 {
			val = styleEqual.Render(val)
		} else {
			val = styleDifferent.Render(val)
		}
	}
	return fmt.Sprintf("%s %s %s  %s", chrome, label, st, val)
}

func (p *Panel) inlineInfo(node *TreeNode) string {
	if node.IsDir {
		var size int64
		if p.isLeft {
			size = node.LeftTotalSize
		} else {
			size = node.RightTotalSize
		}
		if size == 0 {
			return ""
		}
		return styleChrome.Render(fmt.Sprintf("%7s", formatSize(size)))
	}
	var entry *FileEntry
	if p.isLeft {
		entry = node.Left
	} else {
		entry = node.Right
	}
	if entry == nil {
		return ""
	}
	return styleChrome.Render(entry.ModTime.Format("060102 15:04") + " " + fmt.Sprintf("%7s", formatSize(entry.Size)))
}

func attrChar(label string, status AttrStatus) string {
	switch status {
	case AttrEqual:
		return styleEqual.Render(label)
	case AttrDifferent:
		return styleDifferent.Render(label)
	case AttrScanning:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("5")).Render(label)
	case AttrNA:
		return styleUnknown.Render("-")
	default:
		return styleUnknown.Render(".")
	}
}

func (p *Panel) dirStyle(node *TreeNode) lipgloss.Style {
	switch node.Compare.Presence {
	case PresenceLeftOnly, PresenceRightOnly:
		return styleDir.Foreground(styleDifferent.GetForeground())
	}
	switch node.ChildStatus {
	case AttrEqual:
		return styleDir.Foreground(styleEqual.GetForeground())
	case AttrDifferent:
		return styleDir.Foreground(styleDifferent.GetForeground())
	default:
		return styleDir
	}
}

func (p *Panel) nodeStyle(node *TreeNode) lipgloss.Style {
	switch node.Compare.Presence {
	case PresenceLeftOnly, PresenceRightOnly:
		return styleDifferent
	}
	opts := p.cmpOpts
	attrs := []AttrStatus{}
	if opts != nil && opts.Size {
		attrs = append(attrs, node.Compare.Size)
	}
	if opts != nil && opts.ModTime {
		attrs = append(attrs, node.Compare.ModTime)
	}
	if opts != nil && opts.ATime {
		attrs = append(attrs, node.Compare.ATime)
	}
	if opts != nil && opts.CTime {
		attrs = append(attrs, node.Compare.CTime)
	}
	if opts != nil && opts.BTime {
		attrs = append(attrs, node.Compare.BirthTime)
	}
	if opts != nil && opts.Mode {
		attrs = append(attrs, node.Compare.Mode)
	}
	if opts != nil && opts.Checksum {
		attrs = append(attrs, node.Compare.Checksum)
	}
	hasUnknown := false
	for _, a := range attrs {
		if a == AttrDifferent {
			return styleDifferent
		}
		if a == AttrUnknown || a == AttrScanning {
			hasUnknown = true
		}
	}
	if hasUnknown || len(attrs) == 0 {
		return styleUnknown
	}
	return styleEqual
}

func renderGuides(node *TreeNode) string {
	var sb strings.Builder
	for i, cont := range node.Guides {
		if i == 0 {
			continue
		}
		if cont {
			sb.WriteString("│")
		} else {
			sb.WriteString(" ")
		}
	}
	if node.IsLast {
		sb.WriteString("└")
	} else {
		sb.WriteString("├")
	}
	return styleChrome.Render(sb.String())
}

func padRight(s string, width int) string {
	l := lipgloss.Width(s)
	if l >= width {
		return s
	}
	return s + strings.Repeat(" ", width-l)
}
