package ui

import (
	"context"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transfer"
	"sc/transport"
)

type tickMsg time.Time

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type Model struct {
	left, right       model.Backend
	leftArg, rightArg string // what the backends were opened with; BasePath is display only
	insecure          bool

	scanner       *model.Scanner
	cmpOpts       *model.CompareOpts
	deepScan      bool
	scanParallel  int
	scan          *scanOp
	cksum         *scanOp
	opGen         uint64
	pendingRescan *rescanReq

	copying        bool
	deleting       bool
	copyProgress   *transfer.Progress
	deleteProgress *DeleteProgress
	pendingCopy    *pendingCopyInfo
	pendingDelete  *model.TreeNode
	copyParallel   int
	parallelMax    int
	batchTransfer  bool
	verifyResume   bool

	leftPanel, rightPanel *Panel
	activeLeft            bool
	width, height         int
	spinFrame             int
	copySpinFrame         int
	lastCopyBytes         int64
	tickActive            bool

	settings   *SettingsDialog
	input      *InputDialog
	confirm    *ConfirmDialog
	help       *HelpDialog
	info       *InfoDialog
	logView    *LogDialog
	openDlg    *OpenDialog
	diffView   *DiffView
	diffGen    uint64             // identity of the load the diff view is waiting for
	diffCancel context.CancelFunc // aborts that load when the view closes

	cachedStats    *TreeStats // rollup cache, see refreshTree
	statsRev       uint64
	lastPropagate  time.Time
	propagateEvery time.Duration
	lastFlatLen    int
}

func NewModel(left, right model.Backend, leftArg, rightArg string, cmpOpts *model.CompareOpts, insecure, deepScan bool, copyParallel, scanParallel int, batchTransfer, verifyResume bool) *Model {
	copyParallel, scanParallel = max(copyParallel, 1), max(scanParallel, 1)
	lp := NewPanel(left.BasePath(), model.SideLeft)
	rp := NewPanel(right.BasePath(), model.SideRight)
	m := &Model{
		leftPanel:      lp,
		rightPanel:     rp,
		left:           left,
		right:          right,
		leftArg:        leftArg,
		rightArg:       rightArg,
		scanner:        model.NewScanner(left, right, 4, scanParallel, deepScan),
		activeLeft:     true,
		cmpOpts:        cmpOpts,
		deepScan:       deepScan,
		copyParallel:   copyParallel,
		parallelMax:    copyParallel,
		scanParallel:   scanParallel,
		batchTransfer:  batchTransfer,
		verifyResume:   verifyResume,
		settings:       NewSettingsDialog(),
		input:          NewInputDialog(),
		confirm:        NewConfirmDialog(),
		help:           NewHelpDialog(),
		info:           NewInfoDialog(),
		logView:        NewLogDialog(),
		diffView:       NewDiffView(),
		openDlg:        NewOpenDialog(),
		copyProgress:   &transfer.Progress{},
		deleteProgress: &DeleteProgress{},
		insecure:       insecure,
		tickActive:     true,
	}
	lp.cmpOpts = m.cmpOpts
	rp.cmpOpts = m.cmpOpts
	m.settings.SetOptions([]Option{
		{Label: "Size", Value: &m.cmpOpts.Size},
		{Label: "Modify time", Value: &m.cmpOpts.ModTime},
		{Label: "Access time", Value: &m.cmpOpts.ATime},
		{Label: "Change time", Value: &m.cmpOpts.CTime},
		{Label: "Birth time", Value: &m.cmpOpts.BirthTime},
		{Label: "Permissions", Value: &m.cmpOpts.Mode},
		{Label: "Checksum", Value: &m.cmpOpts.Checksum},
		{Label: "Sub-second time precision", Value: &m.cmpOpts.SubSecond},
		{Label: "Time grace ±1s", Value: &m.cmpOpts.TimeGrace},
		{Label: "Ignore TZ/DST (hour-modulo)", Value: &m.cmpOpts.IgnoreTZDST},
		{Label: "Batch rsync+ssh transfer", Value: &m.batchTransfer},
		{Label: "Verify resumed copies (checksum)", Value: &m.verifyResume},
		{Label: "Parallel copies", IntValue: &m.copyParallel, IntMin: 1, IntMax: m.parallelMax},
		{Label: "Bandwidth limit in", GetRate: transport.BandwidthIn, SetRate: transport.SetBandwidthIn},
		{Label: "Bandwidth limit out", GetRate: transport.BandwidthOut, SetRate: transport.SetBandwidthOut},
	})
	return m
}

func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.tickCmd(), m.startScan())
}

func (m *Model) tickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *Model) ensureTick() tea.Cmd {
	if m.tickActive {
		return nil
	}
	m.tickActive = true
	return m.tickCmd()
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.layoutPanels()
	case tickMsg:
		return m, m.tick()
	case opDoneMsg:
		return m, m.finishOp(msg)
	case renameDoneMsg:
		return m, m.finishRename(msg)
	case touchDoneMsg:
		m.refreshTreeNow()
	case deleteDoneMsg:
		m.finishDelete()
	case copyDoneMsg:
		return m, m.finishCopy(msg)
	case diffLoadDoneMsg:
		m.finishDiffLoad(msg)
	}
	return m, nil
}

// tick drives the spinners and the throttled tree refresh while work runs.
// The loop stops itself once idle; ensureTick restarts it with the next op.
func (m *Model) tick() tea.Cmd {
	if !m.busy() {
		m.tickActive = false
		return nil
	}
	m.spinFrame = (m.spinFrame + 1) % len(spinnerFrames)
	m.tickCopy()
	if algo := m.scanner.ChecksumAlgo(); algo != "" {
		m.settings.UpdateChecksumLabel(algo)
	}
	m.autoOpenLog()
	m.refreshTree()
	return m.tickCmd()
}

func (m *Model) scanning() bool     { return m.scan != nil }
func (m *Model) checksumming() bool { return m.cksum != nil }
func (m *Model) transferring() bool { return m.copying || m.deleting }

// busy reports whether any background operation is live, which is what keeps
// the tick loop running.
func (m *Model) busy() bool { return m.scanning() || m.checksumming() || m.transferring() }

// autoOpenLog pops the log up when new errors have arrived.
func (m *Model) autoOpenLog() {
	m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
}

// cancelAll stops every background operation; used on quit.
func (m *Model) cancelAll() {
	cancelStored(&m.copyProgress.Cancel)
	cancelStored(&m.deleteProgress.Cancel)
	m.cancelOps()
}

// cancelStored fires and clears a cancel func shared with a worker goroutine.
func cancelStored(p *atomic.Pointer[context.CancelFunc]) {
	if c := p.Swap(nil); c != nil {
		(*c)()
	}
}
