package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"sc/model"
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
		return "+" + model.FormatSize(d)
	}
	return "-" + model.FormatSize(-d)
}

type StatusInfo struct {
	State         string
	DirsListed    int64
	DirsTotal     int64
	FilesScanned  int64
	ChecksumDone  int64
	ChecksumTotal int64
	FilesDone     int64
	FilesTotal    int64
	BytesCopied   int64
	Elapsed       time.Duration
	Errors        int
	Retries       int
	ChecksumAlgo  string
}

func crcLabel(algo string) string {
	switch algo {
	case "":
		return "NO"
	case "sha1", "sha256":
		return "SHA"
	default:
		return strings.ToUpper(algo)
	}
}

func RenderStatusBar(info StatusInfo, width int) string {
	state := info.State
	if state == "" {
		state = "IDLE"
	}
	left := state
	switch info.State {
	case "DIR SCAN":
		left = fmt.Sprintf("DIR SCAN: %d/%d dirs, %d files", info.DirsListed, info.DirsTotal, info.FilesScanned)
	case "CHECKSUM":
		left = fmt.Sprintf("CHECKSUM: %d/%d files", info.ChecksumDone, info.ChecksumTotal)
	case "COPY":
		rate := ""
		if info.Elapsed > 0 && info.BytesCopied > 0 {
			mbps := float64(info.BytesCopied) / info.Elapsed.Seconds() / (1 << 20)
			rate = fmt.Sprintf(" %.1f MB/s", mbps)
		}
		left = fmt.Sprintf("COPY: %d/%d files, %s%s", info.FilesDone, info.FilesTotal, model.FormatSize(info.BytesCopied), rate)
	case "READ":
		left = fmt.Sprintf("READ: %s", model.FormatSize(info.BytesCopied))
	case "DELETE":
		left = "DELETE"
	case "IDLE":
		if info.DirsListed > 0 || info.FilesScanned > 0 {
			left = fmt.Sprintf("IDLE: %d dirs, %d files", info.DirsListed, info.FilesScanned)
		}
	}

	var counters strings.Builder
	counters.WriteString("  ")
	counters.WriteString(fmt.Sprintf("CRC:%s", crcLabel(info.ChecksumAlgo)))
	if info.Errors > 0 {
		counters.WriteString(fmt.Sprintf("  err:%d", info.Errors))
	}
	if info.Retries > 0 {
		counters.WriteString(fmt.Sprintf("  ret:%d", info.Retries))
	}

	right := "?=help"

	leftW := lipgloss.Width(left) + lipgloss.Width(counters.String())
	rightW := lipgloss.Width(right)
	inner := width - 2
	if inner < 0 {
		inner = 0
	}
	gap := inner - leftW - rightW
	if gap < 1 {
		gap = 1
	}
	content := left + counters.String() + strings.Repeat(" ", gap) + right
	return styleBar.Width(width).Render(content)
}
