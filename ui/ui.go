package ui

import (
	"context"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transfer"
	"sc/transport"
)

type tickMsg time.Time

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// maxCopyParallel caps + and the settings dialog, unless -parallel started
// higher.
const maxCopyParallel = 64

// pane is one side of the screen: the backend, what it was opened with,
// and the panel that draws it. A swap moves whole panes. It moved the
// backends and the panels but not the open arguments, so the next reopen
// (u, b, the location dialog) put the sides back without notice (U6).
type pane struct {
	backend model.Backend
	arg     string // what the backend was opened with; BasePath is display only
	panel   *Panel
}

type Model struct {
	left, right pane
	insecure    bool
	closing     sync.WaitGroup // backends a reopen replaced, still closing (closeLater)

	scanner      *model.Scanner
	cmpOpts      *model.CompareOpts
	deepScan     bool
	scanParallel int
	// The running operations, one per lane (mayStart), and the rescan that
	// waits for the scan.
	scan, cksum, write *op
	opGen              uint64
	pendingRescans     []rescanReq // rescans waiting for the running scan, in order

	copyProgress   *transfer.Progress
	deleteProgress *DeleteProgress
	pendingCopy    *pendingCopyInfo
	pendingDelete  *pendingDeleteInfo
	copyParallel   int
	parallelMax    int
	batchTransfer  bool
	verifyResume   bool
	// relativeCounts (\) shows the sizes and counts, in the top bars and on
	// each row, as how much more or less a side holds than the other side.
	relativeCounts bool
	// mouse is -mouse: sc may take the mouse. mouseOn is whether it has it
	// now: an open dialog gives it to the terminal (syncMouse).
	mouse, mouseOn bool

	activeLeft    bool
	lastSelect    time.Time // last click that only moved the cursor, for double-click
	width, height int
	spinFrame     int
	copySpinFrame int
	lastCopyBytes int64
	lastCopyTick  time.Time
	slotSpins     map[string]slotSpin
	speed         speedHistory
	copyNow       speedNow // the copy's speed now: the popup's total and the status bar
	copyETA       etaClock
	tickActive    bool

	settings   *SettingsDialog
	input      *InputDialog
	confirm    *ConfirmDialog
	help       *HelpDialog
	info       *InfoDialog
	logView    *LogDialog
	logWaits   bool // a dialog held the log back: it opens when that closes (U18)
	openDlg    *OpenDialog
	diffView   *DiffView
	diffGen    uint64             // identity of the load the diff view is waiting for
	diffCancel context.CancelFunc // aborts that load when the view closes

	cachedStats    *TreeStats // rollup cache, see refreshTree
	lastSearch     string     // the search the / dialog starts with
	statsRev       uint64
	statsOpts      model.CompareOpts // the options cachedStats was counted with
	lastPropagate  time.Time
	propagateEvery time.Duration
	lastFlatLen    int
	viewRev        uint64     // counts the folds, see changeView
	rowsFrom       rowsSource // what the panels' rows were built from
	rowsBuilt      bool
}

func NewModel(left, right model.Backend, leftArg, rightArg string, cmpOpts *model.CompareOpts, insecure, deepScan bool, copyParallel, scanParallel int, batchTransfer, verifyResume, relativeCounts bool) *Model {
	copyParallel, scanParallel = max(copyParallel, 1), max(scanParallel, 1)
	lp := NewPanel(left.BasePath(), model.SideLeft)
	rp := NewPanel(right.BasePath(), model.SideRight)
	m := &Model{
		left:           pane{backend: left, arg: leftArg, panel: lp},
		right:          pane{backend: right, arg: rightArg, panel: rp},
		scanner:        model.NewScanner(left, right, 4, scanParallel, deepScan),
		activeLeft:     true,
		cmpOpts:        cmpOpts,
		deepScan:       deepScan,
		copyParallel:   copyParallel,
		parallelMax:    max(copyParallel, maxCopyParallel),
		scanParallel:   scanParallel,
		batchTransfer:  batchTransfer,
		verifyResume:   verifyResume,
		relativeCounts: relativeCounts,
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
	lp.relative = &m.relativeCounts
	rp.relative = &m.relativeCounts
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
		{Label: "Ignore TZ/DST (1-14 whole hours)", Value: &m.cmpOpts.IgnoreTZDST},
		{Label: "Batch rsync+ssh transfer", Value: &m.batchTransfer},
		{Label: "Verify resumed copies (checksum)", Value: &m.verifyResume},
		{Label: "Parallel copies", IntValue: &m.copyParallel, IntMin: 1, IntMax: m.parallelMax},
		{Label: "Bandwidth limit in", GetRate: transport.BandwidthIn, SetRate: transport.SetBandwidthIn},
		{Label: "Bandwidth limit out", GetRate: transport.BandwidthOut, SetRate: transport.SetBandwidthOut},
		{Label: "Counts relative to the other side (\\)", Value: &m.relativeCounts},
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

// SetMouse says whether the program takes the mouse when it starts
// (-mouse).
func (m *Model) SetMouse(on bool) { m.mouse, m.mouseOn = on, on }

// Update handles msg. After it, an open dialog has the terminal take the
// mouse, and a closed one gives it back to sc (syncMouse): a dialog opens
// on keys, and the log also on a tick.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	mdl, cmd := m.update(msg)
	return mdl, tea.Batch(cmd, m.syncMouse())
}

// syncMouse gives the mouse to the terminal while a dialog is open, so
// the dialog's text can be selected and copied, and takes it back for the
// panels. sc then sees no wheel in a dialog: a terminal that sends the
// wheel as the arrow keys still scrolls the log.
func (m *Model) syncMouse() tea.Cmd {
	want := m.mouse && m.openModal() == nil
	if want == m.mouseOn {
		return nil
	}
	m.mouseOn = want
	if want {
		return tea.EnableMouseCellMotion
	}
	return tea.DisableMouse
}

func (m *Model) update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)
	case tea.MouseMsg:
		return m.handleMouse(msg)
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.layoutPanels()
	case tickMsg:
		return m, m.tick()
	case doneMsg:
		return m, m.finishOp(msg)
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
	m.followConnLimit()
	m.updateChecksumLabel()
	m.autoOpenLog()
	m.refreshTree()
	return m.tickCmd()
}

func (m *Model) scanning() bool     { return m.scan != nil }
func (m *Model) checksumming() bool { return m.cksum != nil }
func (m *Model) writing() bool      { return m.write != nil }
func (m *Model) copying() bool      { return m.running(opCopy) }
func (m *Model) deleting() bool     { return m.running(opDelete) }

// busy reports whether any background operation is live, which is what keeps
// the tick loop running.
func (m *Model) busy() bool { return m.scanning() || m.checksumming() || m.writing() }

// autoOpenLog pops the log up when new errors or failures have arrived.
// It waits while another dialog is open: the log came up over it and took
// the keys typed for it, so s saved a log file and e switched the filter
// in place of the new name being typed (U18). The problems stay unseen,
// and the log opens when that dialog closes (handleKey).
func (m *Model) autoOpenLog() {
	if m.dialogOpen() {
		m.logWaits = true
		return
	}
	m.logWaits = false
	m.logView.AutoOpen(problems(), transport.Log.FatalCount())
}

// problems counts the log lines a user must see: errors, and failures (a
// retry that gave up, a copy that ended with failures, a delete, rename or
// touch that failed). The log opened by itself on errors only, and the UI
// logs its own failures as FAIL (U9).
func problems() int { return transport.Log.ErrCount() + transport.Log.FailedCount() }
