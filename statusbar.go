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

func RenderPanelTopBar(stats *TreeStats, isLeft bool, prefix string, width int) string {
	var sb strings.Builder
	if prefix != "" {
		sb.WriteString(prefix)
		sb.WriteString("  ")
	}
	if stats != nil {
		var dirDelta, fileDelta int
		var sizeDelta int64
		if isLeft {
			dirDelta = stats.LeftDirs - stats.RightDirs
			fileDelta = stats.LeftFiles - stats.RightFiles
			sizeDelta = stats.LeftSize - stats.RightSize
		} else {
			dirDelta = stats.RightDirs - stats.LeftDirs
			fileDelta = stats.RightFiles - stats.LeftFiles
			sizeDelta = stats.RightSize - stats.LeftSize
		}
		sb.WriteString(fmt.Sprintf("%sd %sf %s",
			formatDelta(dirDelta),
			formatDelta(fileDelta),
			formatSizeDelta(sizeDelta)))
	}
	return styleBar.Width(width).Render(sb.String())
}

func formatDelta(d int) string {
	if d == 0 {
		return "0"
	}
	return fmt.Sprintf("%+d", d)
}

func formatSizeDelta(d int64) string {
	if d == 0 {
		return "0"
	}
	if d > 0 {
		return "+" + formatSize(d)
	}
	return "-" + formatSize(-d)
}

func RenderBottomBar(width int) string {
	hint := "E=ren D=del <Lcp >Rcp I=info ~=log B=base U=url =pref ?=help"
	inner := width - 2 // styleBar has Padding(0,1) on each side
	if inner < 0 {
		inner = 0
	}
	if runes := []rune(hint); len(runes) > inner {
		hint = string(runes[:inner])
	}
	return styleBar.Width(width).Render(hint)
}
