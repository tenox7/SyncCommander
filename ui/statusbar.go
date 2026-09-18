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
	BorderForeground(lipgloss.Color("14")).
	Background(lipgloss.Color("4")).
	Foreground(lipgloss.Color("15")).
	Padding(0, 1)

var styleDeletePopup = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("1")).
	Background(lipgloss.Color("0")).
	Foreground(lipgloss.Color("15")).
	Padding(0, 1)

func RenderDeletePopup(file, side string, done, failed, total int64, elapsed time.Duration, width int) string {
	inner := max(width-4, 20)
	done = min(max(done, 0), max(total, 0))
	pct := 0
	if total > 0 {
		pct = int(done * 100 / total)
	}
	barIndent := "  ✗ "
	barWidth := max(inner-lipgloss.Width(barIndent)-5, 5)
	header := fmt.Sprintf("DELETE %s  %d/%d items", side, done, total)
	if failed > 0 {
		header += fmt.Sprintf("  %d FAILED", failed)
	}
	rows := []string{
		header,
		truncateName(file, inner),
		fmt.Sprintf("%s%s %3d%%", barIndent, progressBar(done, total, barWidth), pct),
		"Elapsed: " + formatHMS(int64(elapsed.Seconds())),
		"X=cancel",
	}
	return styleDeletePopup.Width(width).Render(padRows(rows, inner))
}

// padRows fits every row to exactly w cells and joins them.
func padRows(rows []string, w int) string {
	for i, r := range rows {
		if lipgloss.Width(r) > w {
			r = ansi.Truncate(r, w, "…")
		}
		rows[i] = r + strings.Repeat(" ", max(w-lipgloss.Width(r), 0))
	}
	return strings.Join(rows, "\n")
}

// formatHMS renders seconds as m:ss, or h:mm:ss past an hour.
func formatHMS(secs int64) string {
	secs = max(secs, 0)
	h, m, s := secs/3600, (secs%3600)/60, secs%60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

// CopyPopupData is the input to RenderCopyPopup. Slots are the in-flight
// per-file rows; BatchFile/BatchFileBytes are populated instead when the
// transfer is running through a single batch session.
type CopyPopupData struct {
	LeftToRight        bool
	Listing            bool
	DoneFiles          int64
	FailedFiles        int64
	TotalFiles         int64
	InFlight           int64
	Parallel           int64
	Batched            bool
	BatchFile          string
	BatchFileBytes     int64
	BatchFileSize      int64
	BatchFileBaseBytes int64
	BatchFileElapsed   time.Duration
	Slots              []CopySlotView
	BytesCopied        int64
	TotalBytes         int64
	BaseBytes          int64
	TotalElapsed       time.Duration
	Spinner            string
}

type CopySlotView struct {
	File      string
	Size      int64
	Bytes     int64
	BaseBytes int64
	Elapsed   time.Duration
}

func RenderCopyPopup(d CopyPopupData, width int) string {
	arrow := ">"
	if !d.LeftToRight {
		arrow = "<"
	}
	inner := max(width-4, 20)

	totalPct := 0
	if d.TotalBytes > 0 {
		totalPct = int(d.BytesCopied * 100 / d.TotalBytes)
	}
	totalRealBytes := d.BytesCopied - d.BaseBytes
	if totalRealBytes < 0 {
		totalRealBytes = 0
	}
	totalRate := 0.0
	if d.TotalElapsed >= 100*time.Millisecond && totalRealBytes > 0 {
		totalRate = float64(totalRealBytes) / d.TotalElapsed.Seconds()
	}

	progressMark := d.Spinner
	if progressMark == "" {
		progressMark = "·"
	}

	barIndent := "  " + arrow + " "
	pctStrLen := 5
	rateW := 11
	totalBarWidth := inner - lipgloss.Width(barIndent) - pctStrLen
	if totalBarWidth < 5 {
		totalBarWidth = 5
	}

	header := fmt.Sprintf("COPY  %d/%d files", d.DoneFiles, d.TotalFiles)
	if d.Listing {
		header = "COPY  listing subtree…"
	}
	if d.FailedFiles > 0 {
		header += fmt.Sprintf("  %d FAILED", d.FailedFiles)
	}
	switch {
	case d.Batched:
		header += "  [BATCH]"
	case d.Parallel > 1:
		header += fmt.Sprintf("  [%d/%d in flight]", d.InFlight, d.Parallel)
	}

	var rows []string
	rows = append(rows, header)

	if d.Batched {
		fileBytes := d.BatchFileBytes
		fileSize := d.BatchFileSize
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
		fileRealBytes := fileBytes - d.BatchFileBaseBytes
		if fileRealBytes < 0 {
			fileRealBytes = 0
		}
		fileRate := 0.0
		if d.BatchFileElapsed >= 100*time.Millisecond && fileRealBytes > 0 {
			fileRate = float64(fileRealBytes) / d.BatchFileElapsed.Seconds()
		}
		nameW := inner - lipgloss.Width(progressMark) - 1
		if nameW < 5 {
			nameW = 5
		}
		name := truncateName(d.BatchFile, nameW)
		rows = append(rows,
			progressMark+" "+name,
			fmt.Sprintf("File:  %s/%s  %s  ETA %s",
				model.FormatSize(fileBytes), model.FormatSize(fileSize),
				formatRateOrDash(fileRate), formatETA(fileSize-fileBytes, fileRate)),
			fmt.Sprintf("%s%s %3d%%", barIndent, progressBar(fileBytes, fileSize, totalBarWidth), filePct),
		)
	} else {
		nameW := inner - lipgloss.Width(barIndent) - pctStrLen - rateW - 2
		if nameW < 8 {
			nameW = 8
		}
		barW := 10
		if len(d.Slots) == 0 && !d.Listing {
			rows = append(rows, "  (waiting…)")
		}
		for _, s := range d.Slots {
			pct := 0
			if s.Size > 0 {
				cap := s.Bytes
				if cap > s.Size {
					cap = s.Size
				}
				pct = int(cap * 100 / s.Size)
			}
			realBytes := s.Bytes - s.BaseBytes
			if realBytes < 0 {
				realBytes = 0
			}
			rate := 0.0
			if s.Elapsed >= 100*time.Millisecond && realBytes > 0 {
				rate = float64(realBytes) / s.Elapsed.Seconds()
			}
			name := truncateName(s.File, nameW)
			namePad := nameW - lipgloss.Width(name)
			if namePad < 0 {
				namePad = 0
			}
			row := fmt.Sprintf("%s%s%s %s %3d%% %s",
				barIndent,
				name, strings.Repeat(" ", namePad),
				progressBar(s.Bytes, s.Size, barW), pct,
				formatRateOrDash(rate))
			rows = append(rows, row)
		}
	}

	rows = append(rows,
		fmt.Sprintf("Total: %s/%s  %s  ETA %s",
			model.FormatSize(d.BytesCopied), model.FormatSize(d.TotalBytes),
			formatRateOrDash(totalRate), formatETA(d.TotalBytes-d.BytesCopied, totalRate)),
		fmt.Sprintf("%s%s %3d%%", barIndent, progressBar(d.BytesCopied, d.TotalBytes, totalBarWidth), totalPct),
		"X=cancel",
	)

	return styleCopyPopup.Width(width).Render(padRows(rows, inner))
}

func truncateName(s string, w int) string {
	if s == "" {
		return "—"
	}
	if lipgloss.Width(s) <= w {
		return s
	}
	base := path.Base(s)
	if lipgloss.Width(base) <= w {
		return base
	}
	return ansi.Truncate(base, w, "…")
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
	return formatHMS(int64(float64(remaining)/rate + 0.5))
}

// popupWidth fits a popup to the terminal.
func popupWidth(termWidth int) int { return max(min(60, termWidth-4), 24) }

// overlayCentered draws popup over the middle of screen.
func overlayCentered(screen, popup string, width, height int) string {
	px := max((width-lipgloss.Width(strings.Split(popup, "\n")[0]))/2, 0)
	py := max((height-strings.Count(popup, "\n")-1)/2, 0)
	return overlayString(screen, popup, px, py)
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
	filled := min(max(int(done*int64(barWidth)/total), 0), barWidth)
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
	State           string
	DirsListed      int64
	DirsTotal       int64
	FilesScanned    int64
	TotalSize       int64
	ChecksumDone    int64
	ChecksumTotal   int64
	FilesDone       int64
	FilesTotal      int64
	BytesCopied     int64
	BaseBytes       int64
	Elapsed         time.Duration
	Errors          int
	Retries         int
	Recovered       int
	Failed          int
	ChecksumAlgo    string
	ChecksumEnabled bool
	Spinner         string
	Mem             MemUsage
	Objects         int64 // tree nodes behind Mem.Live, for the per-object figure
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

	counters := fmt.Sprintf("  CRC:%s err:%d ret:%d rec:%d fail:%d  mem:%s/%s gc:%d",
		crcLabel(info.ChecksumAlgo, info.ChecksumEnabled), info.Errors, info.Retries, info.Recovered, info.Failed,
		model.FormatSize(info.Mem.Live), model.FormatSize(info.Mem.Resident), info.Mem.GCCycles)

	right := "?=help"

	rightW := lipgloss.Width(right)
	inner := width - 2
	if inner < 0 {
		inner = 0
	}

	// Bytes-per-object is the number that matters when tuning for huge trees,
	// but it is the first thing to go when the bar runs out of room.
	if info.Objects > 0 {
		wide := counters + fmt.Sprintf(" (%s/obj)", model.FormatSize(info.Mem.Live/info.Objects))
		if inner-lipgloss.Width(left)-lipgloss.Width(wide)-rightW >= 1 {
			counters = wide
		}
	}

	gap := inner - lipgloss.Width(left) - lipgloss.Width(counters) - rightW
	if gap < 1 {
		// Counters are the fixed part and the part worth keeping — trim the
		// variable-length detail text instead of letting it push them off.
		if room := inner - lipgloss.Width(counters); room > 0 {
			left = ansi.Truncate(left, room, "")
			gap = inner - lipgloss.Width(left) - lipgloss.Width(counters)
		}
		if gap < 0 {
			return styleBar.Width(width).Render(ansi.Truncate(left+counters, inner, ""))
		}
		return styleBar.Width(width).Render(left + counters + strings.Repeat(" ", gap))
	}
	content := left + counters + strings.Repeat(" ", gap) + right
	return styleBar.Width(width).Render(content)
}
