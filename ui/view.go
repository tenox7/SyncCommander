package ui

import (
	"time"

	"github.com/charmbracelet/lipgloss"

	"sc/model"
	"sc/transport"
)

func (m *Model) layoutPanels() {
	panelWidth, panelHeight := m.width/2, m.height-2
	m.leftPanel.width, m.leftPanel.height = panelWidth, panelHeight
	m.rightPanel.width, m.rightPanel.height = m.width-panelWidth, panelHeight
	m.leftPanel.active = m.activeLeft
	m.rightPanel.active = !m.activeLeft
	m.leftPanel.clampOffset()
	m.rightPanel.clampOffset()
}

func (m *Model) View() string {
	if m.width == 0 {
		return "loading..."
	}
	progress := m.scanner.Progress()
	spinner := ""
	if (progress.Phase != "" && progress.Phase != model.PhaseDone) || m.busy() {
		spinner = spinnerFrames[m.spinFrame]
	}
	if d := m.openModal(); d != nil {
		m.logView.spinner = spinner
		return d.View(m.width, m.height)
	}

	operation := ""
	switch {
	case m.deleting:
		operation = spinner + " deleting..."
	case m.checksumming():
		operation = spinner + " checksumming..."
	}
	m.setPanelSpinners(spinner, progress)
	topBar := RenderPanelTopBar(m.cachedStats, true, operation, m.leftPanel.width) +
		RenderPanelTopBar(m.cachedStats, false, operation, m.rightPanel.width)
	status := m.buildStatus(progress)
	status.Spinner = spinner
	var left, right string
	m.readTree(func(*model.TreeNode) { left, right = m.leftPanel.View(), m.rightPanel.View() })
	panels := lipgloss.JoinHorizontal(lipgloss.Top, left, right)
	screen := lipgloss.JoinVertical(lipgloss.Left, topBar, panels, RenderStatusBar(status, m.width))

	if m.copying {
		screen = overlayCentered(screen, m.copyPopup(), m.width, m.height)
	}
	if m.deleting {
		screen = overlayCentered(screen, m.deletePopup(), m.width, m.height)
	}
	return screen
}

// setPanelSpinners marks the sides with work in flight: both during a
// transfer or checksum, otherwise whichever side the scanner is listing.
func (m *Model) setPanelSpinners(spinner string, progress model.ScanProgress) {
	left, right := spinner, spinner
	if !m.transferring() && !m.checksumming() {
		left, right = "", ""
		if progress.LeftActive {
			left = spinner
		}
		if progress.RightActive {
			right = spinner
		}
	}
	m.leftPanel.spinner, m.rightPanel.spinner = left, right
}

func (m *Model) copyPopup() string {
	p := m.copyProgress
	file, _ := p.File.Load().(string)
	batched := p.Batched.Load()
	bytes, baseBytes := p.Bytes.Load(), p.BaseBytes.Load()
	var slots []CopySlotView
	if !batched {
		for _, s := range p.SnapshotSlots() {
			slots = append(slots, CopySlotView{File: s.File, Size: s.Size, Bytes: s.Bytes, BaseBytes: s.BaseBytes, Elapsed: elapsedSince(s.Start)})
		}
	}
	return RenderCopyPopup(CopyPopupData{
		LeftToRight:        p.LeftToRight.Load(),
		Listing:            p.Listing.Load(),
		DoneFiles:          p.Done.Load(),
		FailedFiles:        p.Failed.Load(),
		TotalFiles:         p.Total.Load(),
		InFlight:           p.InFlight.Load(),
		Parallel:           p.Parallel.Load(),
		Batched:            batched,
		BatchFile:          file,
		BatchFileBytes:     bytes - p.FileStartBytes.Load(),
		BatchFileSize:      p.FileSize.Load(),
		BatchFileBaseBytes: baseBytes - p.FileStartBaseBytes.Load(),
		BatchFileElapsed:   elapsedSince(p.FileStart.Load()),
		Slots:              slots,
		BytesCopied:        bytes,
		TotalBytes:         p.TotalBytes.Load(),
		BaseBytes:          baseBytes,
		TotalElapsed:       elapsedSince(p.Start.Load()),
		Spinner:            spinnerFrames[m.copySpinFrame],
	}, popupWidth(m.width))
}

func (m *Model) deletePopup() string {
	p := m.deleteProgress
	file, _ := p.File.Load().(string)
	side, _ := p.Side.Load().(string)
	return RenderDeletePopup(file, side, p.Done.Load(), p.Failed.Load(), p.Total.Load(), elapsedSince(p.Start.Load()), popupWidth(m.width))
}

// elapsedSince is the time since a UnixNano start, zero when nothing started.
func elapsedSince(start int64) time.Duration {
	if start <= 0 {
		return 0
	}
	return time.Since(time.Unix(0, start))
}

func (m *Model) buildStatus(progress model.ScanProgress) StatusInfo {
	info := StatusInfo{
		Errors:          transport.Log.ErrCount(),
		Retries:         transport.Log.RetryCount(),
		Recovered:       transport.Log.RecoveredCount(),
		Failed:          transport.Log.FailedCount(),
		ChecksumAlgo:    m.scanner.ChecksumAlgo(),
		ChecksumEnabled: m.cmpOpts.Checksum,
		Mem:             readMemUsage(),
	}
	if m.cachedStats != nil {
		info.Objects = m.cachedStats.TotalFiles + m.cachedStats.TotalDirs
	}
	switch {
	case m.copying:
		info.State = "COPY"
		info.FilesDone = m.copyProgress.Done.Load()
		info.FilesTotal = m.copyProgress.Total.Load()
		info.BytesCopied = m.copyProgress.Bytes.Load()
		info.BaseBytes = m.copyProgress.BaseBytes.Load()
		info.Elapsed = elapsedSince(m.copyProgress.Start.Load())
	case m.deleting:
		info.State = "DELETE"
		info.FilesDone = m.deleteProgress.Done.Load()
		info.FilesTotal = m.deleteProgress.Total.Load()
		info.Elapsed = elapsedSince(m.deleteProgress.Start.Load())
	case m.checksumming() || progress.Phase == model.PhaseChecksumming:
		info.State = "CHECKSUM"
		info.ChecksumDone = progress.ChecksumDone
		info.ChecksumTotal = progress.ChecksumFiles
	case m.scanning() || progress.Phase == model.PhaseScanning:
		info.State = "DIR SCAN"
		info.DirsListed = progress.DirsListed
		info.DirsTotal = progress.DirsTotal
		info.FilesScanned = progress.TotalFiles
		if m.cachedStats != nil {
			info.TotalSize = m.cachedStats.TotalSize
		}
	default:
		info.State = "IDLE"
		if m.cachedStats != nil {
			info.DirsListed = m.cachedStats.TotalDirs
			info.FilesScanned = m.cachedStats.TotalFiles
			info.TotalSize = m.cachedStats.TotalSize
		}
	}
	return info
}
