package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var styleBar = lipgloss.NewStyle().
	Background(lipgloss.Color("4")).
	Foreground(lipgloss.Color("15")).
	Padding(0, 1)

func progressBar(done, total int64, barWidth int) string {
	if total <= 0 {
		return strings.Repeat("░", barWidth)
	}
	filled := int(done * int64(barWidth) / total)
	if filled > barWidth {
		filled = barWidth
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
}

func sizeDiffStr(tree *TreeNode) string {
	if tree == nil {
		return ""
	}
	s := computeTreeStats(tree)
	diff := s.LeftSize - s.RightSize
	if diff == 0 {
		return "Δ 0"
	}
	sign := "L+"
	if diff < 0 {
		diff = -diff
		sign = "R+"
	}
	return fmt.Sprintf("Δ %s%s", sign, formatSizeLong(diff))
}

func RenderTopBar(progress ScanProgress, tree *TreeNode, spinner string, operation string, cksumStatus string, width int) string {
	cksum := "cksum=" + cksumStatus
	if operation != "" {
		return styleBar.Width(width).Render(operation + "  " + cksum)
	}
	switch progress.Phase {
	case "":
		return styleBar.Width(width).Render(cksum)
	case "done":
		s := fmt.Sprintf("=%d ≠%d  %s",
			progress.FilesEqual, progress.FilesDifferent,
			sizeDiffStr(tree))
		if progress.ChecksumFiles > 0 {
			s += fmt.Sprintf("  cksum:%d/%d", progress.ChecksumDone, progress.ChecksumFiles)
		} else {
			s += "  " + cksum
		}
		return styleBar.Width(width).Render(s)
	default:
		s := fmt.Sprintf("%s dirs:%d/%d files:%d  =%d ≠%d",
			progress.Phase,
			progress.DirsListed, progress.DirsTotal,
			progress.TotalFiles,
			progress.FilesEqual, progress.FilesDifferent)
		if progress.ChecksumFiles > 0 {
			s += fmt.Sprintf("  cksum:%d/%d", progress.ChecksumDone, progress.ChecksumFiles)
		} else {
			s += "  " + cksum
		}
		return styleBar.Width(width).Render(s)
	}
}

func RenderBottomBar(width int) string {
	return styleBar.Width(width).Render("←→=expand R=rescan C=cksum T=touch N=ren D=del <Copy> I=info ~=log O=opts ?=help Q=quit")
}
