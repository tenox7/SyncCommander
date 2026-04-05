package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type TreeStats struct {
	LeftFiles, RightFiles   int
	LeftDirs, RightDirs     int
	LeftSize, RightSize     int64
	FilesEqual, FilesDiff   int
	FilesLeftOnly           int
	FilesRightOnly          int
}

func computeTreeStats(root *TreeNode) TreeStats {
	var s TreeStats
	walkStats(root, &s)
	return s
}

func walkStats(node *TreeNode, s *TreeStats) {
	for _, child := range node.Children {
		if child.IsDir {
			if child.Left != nil {
				s.LeftDirs++
			}
			if child.Right != nil {
				s.RightDirs++
			}
			walkStats(child, s)
			continue
		}
		if child.Left != nil {
			s.LeftFiles++
			s.LeftSize += child.Left.Size
		}
		if child.Right != nil {
			s.RightFiles++
			s.RightSize += child.Right.Size
		}
		switch child.Compare.Presence {
		case PresenceBoth:
			if child.Compare.Size == AttrEqual && child.Compare.ModTime == AttrEqual {
				s.FilesEqual++
			} else {
				s.FilesDiff++
			}
		case PresenceLeftOnly:
			s.FilesLeftOnly++
		case PresenceRightOnly:
			s.FilesRightOnly++
		}
	}
}

type InfoDialog struct {
	visible   bool
	stats     TreeStats
	left      string
	right     string
	cksumAlgo string
	cksumLeft string
	cksumRight string
}

func NewInfoDialog() *InfoDialog {
	return &InfoDialog{}
}

func (d *InfoDialog) Open(stats TreeStats, left, right string, cksumAlgo string, cksumLeft, cksumRight []string) {
	d.visible = true
	d.stats = stats
	d.left = left
	d.right = right
	d.cksumAlgo = cksumAlgo
	if len(cksumLeft) > 0 {
		d.cksumLeft = strings.Join(cksumLeft, ",")
	} else {
		d.cksumLeft = "none"
	}
	if len(cksumRight) > 0 {
		d.cksumRight = strings.Join(cksumRight, ",")
	} else {
		d.cksumRight = "none"
	}
}

func (d *InfoDialog) Close()       { d.visible = false }
func (d *InfoDialog) IsOpen() bool { return d.visible }

func (d *InfoDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	s := d.stats
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Width(14)
	valStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("7")).Width(14)
	headStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("4"))

	var sb strings.Builder
	sb.WriteString(headStyle.Render("Statistics"))
	sb.WriteString("\n\n")

	sb.WriteString(labelStyle.Render(""))
	sb.WriteString(valStyle.Render("Left"))
	sb.WriteString(valStyle.Render("Right"))
	sb.WriteString("\n")

	row := func(label string, l, r string) {
		sb.WriteString(labelStyle.Render(label))
		sb.WriteString(valStyle.Render(l))
		sb.WriteString(valStyle.Render(r))
		sb.WriteString("\n")
	}

	row("Files", fmt.Sprintf("%d", s.LeftFiles), fmt.Sprintf("%d", s.RightFiles))
	row("Directories", fmt.Sprintf("%d", s.LeftDirs), fmt.Sprintf("%d", s.RightDirs))
	row("Total size", formatSizeLong(s.LeftSize), formatSizeLong(s.RightSize))

	sb.WriteString("\n")
	sb.WriteString(labelStyle.Render("Equal"))
	sb.WriteString(fmt.Sprintf("%d", s.FilesEqual))
	sb.WriteString("\n")
	sb.WriteString(labelStyle.Render("Different"))
	sb.WriteString(fmt.Sprintf("%d", s.FilesDiff))
	sb.WriteString("\n")
	sb.WriteString(labelStyle.Render("Left only"))
	sb.WriteString(fmt.Sprintf("%d", s.FilesLeftOnly))
	sb.WriteString("\n")
	sb.WriteString(labelStyle.Render("Right only"))
	sb.WriteString(fmt.Sprintf("%d", s.FilesRightOnly))

	sb.WriteString("\n")
	algo := d.cksumAlgo
	if algo == "" {
		algo = "n/a"
	}
	sb.WriteString(labelStyle.Render("Checksum"))
	sb.WriteString(fmt.Sprintf("%s  L:[%s]  R:[%s]", algo, d.cksumLeft, d.cksumRight))

	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render(d.left))
	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render(d.right))

	sb.WriteString("\n\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Esc=close"))

	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("4")).
		Padding(1, 2)

	dialog := style.Render(sb.String())
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, dialog)
}

func formatSizeLong(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
