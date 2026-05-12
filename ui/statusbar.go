package ui

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

var styleCopyPopup = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("4")).
	Background(lipgloss.Color("0")).
	Foreground(lipgloss.Color("15")).
	Padding(0, 1)

var styleDeletePopup = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("1")).
	Background(lipgloss.Color("0")).
	Foreground(lipgloss.Color("15")).
	Padding(0, 1)

func RenderDeletePopup(file, side string, done, total int64, enumerating bool, elapsed time.Duration, width int) string {
	inner := width - 4
	if inner < 20 {
		inner = 20
	}

	pct := 0
	if total > 0 {
		if done > total {
			done = total
		}
		pct = int(done * 100 / total)
	}

	name := file
	if name == "" {
		name = "—"
	}
	if lipgloss.Width(name) > inner {
		name = ansi.Truncate(name, inner, "…")
	}

	barIndent := "  ✗ "
	pctStrLen := 5
	barWidth := inner - lipgloss.Width(barIndent) - pctStrLen
	if barWidth < 5 {
		barWidth = 5
	}

	header := fmt.Sprintf("DELETE %s", side)
	var line1 string
	if enumerating {
		line1 = fmt.Sprintf("%s  enumerating… %d found", header, total)
	} else {
		line1 = fmt.Sprintf("%s  %d/%d items", header, done, total)
	}
	line2 := name
	var line3 string
	if enumerating {
		line3 = fmt.Sprintf("%s%s   --%%", barIndent, strings.Repeat("░", barWidth))
	} else {
		line3 = fmt.Sprintf("%s%s %3d%%", barIndent, progressBar(done, total, barWidth), pct)
	}
	line4 := fmt.Sprintf("Elapsed: %s", formatElapsed(elapsed))
	line5 := "X=cancel"

	pad := func(s string) string {
		if lipgloss.Width(s) > inner {
			s = ansi.Truncate(s, inner, "…")
		}
		w := lipgloss.Width(s)
		if w >= inner {
			return s
		}
		return s + strings.Repeat(" ", inner-w)
	}
	body := pad(line1) + "\n" + pad(line2) + "\n" +
		pad(line3) + "\n" + pad(line4) + "\n" +
		pad(line5)
	return styleDeletePopup.Width(width).Render(body)
}

func formatElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	secs := int64(d.Seconds())
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

func RenderCopyPopup(file string, leftToRight bool,
	doneFiles, totalFiles int64,
	fileBytes, fileSize, fileBaseBytes int64,
	bytesCopied, totalBytes, baseBytes int64,
	fileElapsed, totalElapsed time.Duration,
	progressSpinner string,
	width int) string {
	arrow := ">"
	if !leftToRight {
		arrow = "<"
	}
	inner := width - 4
	if inner < 20 {
		inner = 20
	}

	if fileBytes < 0 {
		fileBytes = 0
	}
	if fileSize > 0 && fileBytes > fileSize {
		fileBytes = fileSize
	}

	filePct := 0
	if fileSize > 0 {
		filePct = int(fileBytes * 100 / fileSize)
	}
	totalPct := 0
	if totalBytes > 0 {
		totalPct = int(bytesCopied * 100 / totalBytes)
	}

	fileRealBytes := fileBytes - fileBaseBytes
	if fileRealBytes < 0 {
		fileRealBytes = 0
	}
	totalRealBytes := bytesCopied - baseBytes
	if totalRealBytes < 0 {
		totalRealBytes = 0
	}

	fileRate := 0.0
	if fileElapsed >= 100*time.Millisecond && fileRealBytes > 0 {
		fileRate = float64(fileRealBytes) / fileElapsed.Seconds()
	}
	totalRate := 0.0
	if totalElapsed >= 100*time.Millisecond && totalRealBytes > 0 {
		totalRate = float64(totalRealBytes) / totalElapsed.Seconds()
	}

	progressMark := progressSpinner
	if progressMark == "" {
		progressMark = "·"
	}
	nameW := inner - lipgloss.Width(progressMark) - 1
	if nameW < 5 {
		nameW = 5
	}
	name := file
	if name == "" {
		name = "—"
	}
	if lipgloss.Width(name) > nameW {
		name = path.Base(name)
		if lipgloss.Width(name) > nameW {
			name = ansi.Truncate(name, nameW, "…")
		}
	}

	barIndent := "  " + arrow + " "
	pctStrLen := 5
	barWidth := inner - lipgloss.Width(barIndent) - pctStrLen
	if barWidth < 5 {
		barWidth = 5
	}

	line1 := fmt.Sprintf("COPY  %d/%d files", doneFiles, totalFiles)
	line2 := progressMark + " " + name
	line3 := fmt.Sprintf("File:  %s/%s  %s  ETA %s",
		model.FormatSize(fileBytes), model.FormatSize(fileSize),
		formatRateOrDash(fileRate), formatETA(fileSize-fileBytes, fileRate))
	line4 := fmt.Sprintf("%s%s %3d%%", barIndent, progressBar(fileBytes, fileSize, barWidth), filePct)
	line5 := fmt.Sprintf("Total: %s/%s  %s  ETA %s",
		model.FormatSize(bytesCopied), model.FormatSize(totalBytes),
		formatRateOrDash(totalRate), formatETA(totalBytes-bytesCopied, totalRate))
	line6 := fmt.Sprintf("%s%s %3d%%", barIndent, progressBar(bytesCopied, totalBytes, barWidth), totalPct)
	line7 := "X=cancel"

	pad := func(s string) string {
		if lipgloss.Width(s) > inner {
			s = ansi.Truncate(s, inner, "…")
		}
		w := lipgloss.Width(s)
		if w >= inner {
			return s
		}
		return s + strings.Repeat(" ", inner-w)
	}
	body := pad(line1) + "\n" + pad(line2) + "\n" +
		pad(line3) + "\n" + pad(line4) + "\n" +
		pad(line5) + "\n" + pad(line6) + "\n" +
		pad(line7)
	return styleCopyPopup.Width(width).Render(body)
}

func formatRateOrDash(rate float64) string {
	if rate <= 0 {
		return "— B/s"
	}
	return model.FormatRate(rate)
}

func formatETA(remaining int64, rate float64) string {
	if remaining <= 0 || rate <= 0 {
		return "--:--"
	}
	secs := int64(float64(remaining)/rate + 0.5)
	if secs < 0 {
		return "--:--"
	}
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

func overlayString(base, popup string, x, y int) string {
	baseLines := strings.Split(base, "\n")
	popupLines := strings.Split(popup, "\n")
	for i, pl := range popupLines {
		row := y + i
		if row < 0 || row >= len(baseLines) {
			continue
		}
		bl := baseLines[row]
		baseW := lipgloss.Width(bl)
		popupW := lipgloss.Width(pl)
		left := ansi.Cut(bl, 0, x)
		if lw := lipgloss.Width(left); lw < x {
			left += strings.Repeat(" ", x-lw)
		}
		right := ""
		if rs := x + popupW; rs < baseW {
			right = ansi.Cut(bl, rs, baseW)
		}
		baseLines[row] = left + pl + right
	}
	return strings.Join(baseLines, "\n")
}

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
	TotalSize     int64
	ChecksumDone  int64
	ChecksumTotal int64
	FilesDone     int64
	FilesTotal    int64
	BytesCopied   int64
	BaseBytes     int64
	Elapsed       time.Duration
	Errors        int
	Retries       int
	Recovered     int
	Failed        int
	ChecksumAlgo    string
	ChecksumEnabled bool
	Spinner         string
}

func crcLabel(algo string, enabled bool) string {
	switch algo {
	case "":
		if enabled {
			return "WAIT"
		}
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
	details := state
	switch info.State {
	case "DIR SCAN":
		details = fmt.Sprintf("DIR SCAN  Progress: %d/%d dirs, %d files, %s", info.DirsListed, info.DirsTotal, info.FilesScanned, formatSizeLong(info.TotalSize))
	case "CHECKSUM":
		details = fmt.Sprintf("CHECKSUM: %d/%d files", info.ChecksumDone, info.ChecksumTotal)
	case "COPY":
		rate := ""
		realBytes := info.BytesCopied - info.BaseBytes
		if realBytes < 0 {
			realBytes = 0
		}
		if info.Elapsed > 0 && realBytes > 0 {
			rate = " " + model.FormatRate(float64(realBytes)/info.Elapsed.Seconds())
		}
		details = fmt.Sprintf("COPY: %d/%d files, %s%s", info.FilesDone, info.FilesTotal, model.FormatSize(info.BytesCopied), rate)
	case "READ":
		details = fmt.Sprintf("READ: %s", model.FormatSize(info.BytesCopied))
	case "DELETE":
		details = fmt.Sprintf("DELETE: %d/%d items", info.FilesDone, info.FilesTotal)
	case "IDLE":
		if info.DirsListed > 0 || info.FilesScanned > 0 {
			details = fmt.Sprintf("IDLE  Totals: %dd, %df, %s", info.DirsListed, info.FilesScanned, strings.ReplaceAll(formatSizeLong(info.TotalSize), " ", ""))
		}
	}

	left := "STATUS: " + details
	if info.Spinner != "" {
		left = info.Spinner + " " + left
	}

	counters := fmt.Sprintf("  CRC:%s err:%d ret:%d rec:%d fail:%d",
		crcLabel(info.ChecksumAlgo, info.ChecksumEnabled), info.Errors, info.Retries, info.Recovered, info.Failed)

	right := "?=help"

	leftW := lipgloss.Width(left) + lipgloss.Width(counters)
	rightW := lipgloss.Width(right)
	inner := width - 2
	if inner < 0 {
		inner = 0
	}
	gap := inner - leftW - rightW
	if gap < 1 {
		content := ansi.Truncate(left+counters, inner, "")
		return styleBar.Width(width).Render(content)
	}
	content := left + counters + strings.Repeat(" ", gap) + right
	return styleBar.Width(width).Render(content)
}
