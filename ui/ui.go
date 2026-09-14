package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"sc/model"
	"sc/transport"
)

type tickMsg time.Time

type opKind int

const (
	opScan opKind = iota
	opChecksum
)

// scanOp is one running scanner operation. gen tells a late done message from
// a cancelled predecessor apart from the current op, so a stale one can never
// clear the flag that keeps the tick loop alive.
type scanOp struct {
	gen    uint64
	cancel context.CancelFunc
}

type opDoneMsg struct {
	kind opKind
	gen  uint64
}

// rescanReq is a rescan that has to wait for the running scan to finish.
type rescanReq struct {
	node    *model.TreeNode
	changed *model.ChangedPaths
}
type touchDoneMsg struct{}
type diffLoadDoneMsg struct {
	left, right []byte
	err         error
}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type renameDoneMsg struct {
	err    error
	rescan *model.TreeNode
}
type deleteDoneMsg struct{}
type copyDoneMsg struct {
	rescanRoot *model.TreeNode
	changed    *model.ChangedPaths
}

type cancelFn struct{ f context.CancelFunc }

type CopyProgress struct {
	Total              atomic.Int64
	Done               atomic.Int64
	Failed             atomic.Int64
	InFlight           atomic.Int64
	Parallel           atomic.Int64
	Batched            atomic.Bool
	Bytes              atomic.Int64
	BaseBytes          atomic.Int64
	CompletedBytes     atomic.Int64
	CompletedBaseBytes atomic.Int64
	TotalBytes         atomic.Int64
	Start              atomic.Int64
	File               atomic.Value
	FileSize           atomic.Int64
	FileStart          atomic.Int64
	FileStartBytes     atomic.Int64
	FileStartBaseBytes atomic.Int64
	LeftToRight        atomic.Bool
	Listing            atomic.Bool // enumerating an unlisted subtree before the first file moves
	Cancel             atomic.Pointer[cancelFn]
	Sem                atomic.Pointer[dynSem]

	slotsMu sync.RWMutex
	slots   []*ProgressSlot
}

// ProgressSlot tracks one in-flight file copy for the multi-slot copy popup.
// Each parallel goroutine in copyOne claims a slot at start and releases it
// at end. The transport writes byte deltas into Bytes via the context counter.
type ProgressSlot struct {
	File      atomic.Value
	Size      atomic.Int64
	Bytes     atomic.Int64
	BaseBytes atomic.Int64
	Start     atomic.Int64
	Active    atomic.Bool
}

// Reset rearms every counter for a new transfer, so the popup never shows the
// previous copy's numbers while the subtree is still being listed.
func (p *CopyProgress) Reset(parallel, slots int, leftToRight bool) {
	for _, c := range []*atomic.Int64{&p.Total, &p.TotalBytes, &p.Done, &p.Failed, &p.InFlight, &p.Bytes, &p.BaseBytes,
		&p.CompletedBytes, &p.CompletedBaseBytes, &p.FileSize, &p.FileStart, &p.FileStartBytes, &p.FileStartBaseBytes} {
		c.Store(0)
	}
	p.Parallel.Store(int64(parallel))
	p.Batched.Store(false)
	p.Listing.Store(false)
	p.LeftToRight.Store(leftToRight)
	p.File.Store("")
	p.Start.Store(time.Now().UnixNano())
	p.ResetSlots(slots)
}

func (p *CopyProgress) ResetSlots(n int) {
	p.slotsMu.Lock()
	defer p.slotsMu.Unlock()
	p.slots = make([]*ProgressSlot, n)
	for i := range p.slots {
		p.slots[i] = &ProgressSlot{}
	}
}

func (p *CopyProgress) ClaimSlot() *ProgressSlot {
	p.slotsMu.RLock()
	defer p.slotsMu.RUnlock()
	for _, s := range p.slots {
		if s.Active.CompareAndSwap(false, true) {
			s.File.Store("")
			s.Size.Store(0)
			s.Bytes.Store(0)
			s.BaseBytes.Store(0)
			s.Start.Store(0)
			return s
		}
	}
	return nil
}

func (p *CopyProgress) ReleaseSlot(s *ProgressSlot) {
	if s == nil {
		return
	}
	p.CompletedBytes.Add(s.Bytes.Load())
	p.CompletedBaseBytes.Add(s.BaseBytes.Load())
	s.Active.Store(false)
}

func (p *CopyProgress) SnapshotSlots() []SlotSnapshot {
	p.slotsMu.RLock()
	defer p.slotsMu.RUnlock()
	out := make([]SlotSnapshot, 0, len(p.slots))
	for _, s := range p.slots {
		if !s.Active.Load() {
			continue
		}
		file, _ := s.File.Load().(string)
		out = append(out, SlotSnapshot{
			File:      file,
			Size:      s.Size.Load(),
			Bytes:     s.Bytes.Load(),
			BaseBytes: s.BaseBytes.Load(),
			Start:     s.Start.Load(),
		})
	}
	return out
}

type SlotSnapshot struct {
	File      string
	Size      int64
	Bytes     int64
	BaseBytes int64
	Start     int64
}

// SyncTotals refreshes Bytes/BaseBytes to be the sum of completed bytes plus
// the in-flight slot counters. The status bar reads progress.Bytes directly;
// per-slot writers update only their slot, so without this the status bar
// counter would freeze during parallel copies. Skip during batch mode —
// batch writes Bytes directly and has no slots to sum from.
func (p *CopyProgress) SyncTotals() {
	if p.Batched.Load() {
		return
	}
	bytes := p.CompletedBytes.Load()
	base := p.CompletedBaseBytes.Load()
	p.slotsMu.RLock()
	for _, s := range p.slots {
		if !s.Active.Load() {
			continue
		}
		bytes += s.Bytes.Load()
		base += s.BaseBytes.Load()
	}
	p.slotsMu.RUnlock()
	p.Bytes.Store(bytes)
	p.BaseBytes.Store(base)
}

func (p *CopyProgress) BeginFile(size int64) {
	p.FileSize.Store(size)
	p.FileStartBytes.Store(p.Bytes.Load())
	p.FileStartBaseBytes.Store(p.BaseBytes.Load())
	p.FileStart.Store(time.Now().UnixNano())
}

type DeleteProgress struct {
	Total       atomic.Int64
	Done        atomic.Int64
	File        atomic.Value
	Side        atomic.Value
	Start       atomic.Int64
	Enumerating atomic.Bool
	Cancel      atomic.Pointer[cancelFn]
}

type pendingCopyInfo struct {
	node        *model.TreeNode
	leftToRight bool
}

type Model struct {
	leftPanel      *Panel
	rightPanel     *Panel
	left           model.Backend
	right          model.Backend
	leftArg        string // the arguments the backends were opened with; BasePath is display only
	rightArg       string
	scanner        *model.Scanner
	activeLeft     bool
	scan           *scanOp
	cksum          *scanOp
	opGen          uint64
	pendingRescan  *rescanReq
	deleting       bool
	copying        bool
	copyProgress   *CopyProgress
	deleteProgress *DeleteProgress
	cmpOpts        *model.CompareOpts
	width          int
	height         int
	spinFrame      int
	copySpinFrame  int
	lastCopyBytes  int64
	settings       *SettingsDialog
	input          *InputDialog
	confirm        *ConfirmDialog
	help           *HelpDialog
	info           *InfoDialog
	logView        *LogDialog
	diffView       *DiffView
	pendingDelete  *model.TreeNode
	pendingCopy    *pendingCopyInfo
	openDlg        *OpenDialog
	insecure       bool
	deepScan       bool
	copyParallel   int
	parallelMax    int
	scanParallel   int
	batchTransfer  bool
	verifyResume   bool
	tickActive     bool
	cachedStats    *TreeStats
	statsRev       uint64
	lastPropagate  time.Time
	propagateEvery time.Duration
	lastFlatLen    int
}

func NewModel(left, right model.Backend, leftArg, rightArg string, cmpOpts *model.CompareOpts, insecure, deepScan bool, copyParallel, scanParallel int, batchTransfer, verifyResume bool) *Model {
	copyParallel, scanParallel = max(copyParallel, 1), max(scanParallel, 1)
	lp := NewPanel(left.BasePath())
	lp.isLeft = true
	rp := NewPanel(right.BasePath())
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
		copyProgress:   &CopyProgress{},
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
		{Label: "Birth time", Value: &m.cmpOpts.BTime},
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
		m.width = msg.Width
		m.height = msg.Height
		m.layoutPanels()
		return m, nil
	case tickMsg:
		if !m.busy() {
			m.tickActive = false
			return m, nil
		}
		m.spinFrame = (m.spinFrame + 1) % len(spinnerFrames)
		if m.copying {
			m.copyProgress.SyncTotals()
			cur := m.copyProgress.Bytes.Load()
			if cur != m.lastCopyBytes {
				m.copySpinFrame = (m.copySpinFrame + 1) % len(spinnerFrames)
				m.lastCopyBytes = cur
			}
		} else {
			m.lastCopyBytes = 0
		}
		if algo := m.scanner.ChecksumAlgo(); algo != "" {
			m.settings.UpdateChecksumLabel(algo)
		}
		m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		m.refreshTree()
		return m, m.tickCmd()
	case opDoneMsg:
		return m, m.finishOp(msg)
	case renameDoneMsg:
		if msg.err != nil {
			m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		}
		m.refreshTreeNow()
		if msg.rescan != nil {
			return m, m.queueRescan(msg.rescan, nil)
		}
		return m, nil
	case touchDoneMsg:
		m.refreshTreeNow()
		return m, nil
	case deleteDoneMsg:
		m.deleting = false
		if c := m.deleteProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		m.refreshTreeNow()
		return m, nil
	case copyDoneMsg:
		m.copying = false
		if c := m.copyProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		m.refreshTreeNow()
		if msg.rescanRoot != nil {
			return m, m.queueRescan(msg.rescanRoot, msg.changed)
		}
		return m, nil
	case diffLoadDoneMsg:
		if msg.err != nil {
			m.diffView.SetError(msg.err.Error())
		} else {
			m.diffView.LoadContent(msg.left, msg.right)
		}
		return m, nil
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "ctrl+c" {
		if c := m.copyProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		if c := m.deleteProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		m.cancelOps()
		return m, tea.Quit
	}
	if m.diffView.IsOpen() {
		return m.handleDiffViewKey(msg)
	}
	if m.logView.IsOpen() {
		switch msg.String() {
		case "esc", "ctrl+c", "q", "~", "`":
			m.logView.Close()
		case "up", "k":
			m.logView.ScrollUp()
		case "down", "j":
			m.logView.ScrollDown()
		case "pgup":
			m.logView.PageUp()
		case "pgdown":
			m.logView.PageDown()
		case "home":
			m.logView.Home()
		case "end":
			m.logView.End()
		case "e":
			m.logView.ToggleErrFilter()
		}
		return m, nil
	}
	if m.info.IsOpen() {
		switch msg.String() {
		case "esc", "ctrl+c", "q", "i":
			m.info.Close()
		}
		return m, nil
	}
	if m.help.IsOpen() {
		switch msg.String() {
		case "esc", "ctrl+c", "q", "?":
			m.help.Close()
		}
		return m, nil
	}
	if m.confirm.IsOpen() {
		return m.handleConfirmKey(msg)
	}
	if m.input.IsOpen() {
		return m.handleInputKey(msg)
	}
	if m.openDlg.IsOpen() {
		return m.handleOpenDlgKey(msg)
	}
	if m.settings.IsOpen() {
		return m.handleSettingsKey(msg)
	}
	if m.copying {
		switch msg.String() {
		case "x", "X", "ctrl+c":
			if c := m.copyProgress.Cancel.Load(); c != nil {
				transport.Log.Add("copy", "<<<", "user canceled transfer")
				c.f()
			}
		case "~", "`":
			m.logView.Open()
		case "-":
			m.adjustCopyParallel(-1)
		case "+", "=":
			m.adjustCopyParallel(+1)
		}
		return m, nil
	}
	if m.deleting {
		switch msg.String() {
		case "x", "X", "ctrl+c":
			if c := m.deleteProgress.Cancel.Load(); c != nil {
				transport.Log.Add("delete", "<<<", "user canceled delete")
				c.f()
			}
		case "~", "`":
			m.logView.Open()
		}
		return m, nil
	}
	switch msg.String() {
	case "q":
		m.cancelOps()
		return m, tea.Quit
	case "tab":
		m.activeLeft = !m.activeLeft
		m.leftPanel.active = m.activeLeft
		m.rightPanel.active = !m.activeLeft
	case "up", "k":
		m.activePanel().MoveUp()
		m.syncPanels()
	case "down", "j":
		m.activePanel().MoveDown()
		m.syncPanels()
	case "pgup":
		m.activePanel().PageUp()
		m.syncPanels()
	case "pgdown":
		m.activePanel().PageDown()
		m.syncPanels()
	case "enter", "right", "l":
		node := m.activePanel().CursorNode()
		lazyList := false
		m.mutateTree(func(*model.TreeNode) {
			if node != nil && node.IsDir && !node.Listed && !m.scanning() {
				node.Expanded = true
				lazyList = true
				return
			}
			m.activePanel().Toggle()
		})
		if lazyList {
			return m, m.listNode(node)
		}
		m.refreshTree()
	case "left", "h":
		node := m.activePanel().CursorNode()
		collapsed := false
		m.mutateTree(func(*model.TreeNode) {
			if node == nil {
				return
			}
			if !node.IsAttr && node.Expanded {
				node.Expanded = false
				collapsed = true
				return
			}
			p := m.activePanel()
			depth := node.Depth
			if node.IsAttr {
				depth = node.Depth - 1
			}
			for i := p.cursor - 1; i >= 0; i-- {
				n := p.nodes[i]
				if !n.IsAttr && n.IsDir && n.Depth < depth {
					p.cursor = i
					n.Expanded = false
					p.clampOffset()
					m.syncPanels()
					collapsed = true
					break
				}
			}
		})
		if collapsed {
			m.refreshTree()
		}
	case "n":
		if !m.busy() {
			m.jumpToNextDiff()
		}
	case "/":
		m.openSearch()
	case "}":
		m.mutateTree(func(tree *model.TreeNode) {
			if tree != nil {
				model.SetExpandedAll(tree, true)
			}
		})
		m.refreshTree()
	case "{":
		m.mutateTree(func(tree *model.TreeNode) {
			if tree == nil {
				return
			}
			model.SetExpandedAll(tree, false)
			tree.Expanded = true
		})
		m.refreshTree()
	case "r":
		node := m.activePanel().CursorNode()
		if m.scanning() || (node != nil && node.IsAttr) {
			break
		}
		return m, m.rescanWithTopLevel(node)
	case "R":
		tree := m.scanner.Tree()
		if tree == nil || m.scanning() {
			break
		}
		return m, m.deepRescanNode(tree)
	case "t":
		node := m.activePanel().CursorNode()
		touch := false
		m.readTree(func(*model.TreeNode) {
			touch = node != nil && !node.IsAttr && node.Compare.Presence == model.PresenceBoth
		})
		if touch {
			return m, m.touchNode(node)
		}
	case "c":
		if m.checksumming() {
			break
		}
		node := m.activePanel().CursorNode()
		if node != nil && node.IsAttr {
			node = m.parentFileNode()
		}
		if node != nil {
			return m, m.checksumNode(node)
		}
	case "e":
		node := m.activePanel().CursorNode()
		if node != nil && !node.IsAttr {
			m.openRename(node)
		}
	case "d":
		if m.deleting {
			break
		}
		node := m.activePanel().CursorNode()
		if node != nil && !node.IsAttr {
			m.openDelete(node)
		}
	case ">":
		if m.copying {
			break
		}
		node := m.activePanel().CursorNode()
		if node == nil || node.IsAttr || m.presence(node) == model.PresenceRightOnly {
			break
		}
		if lines, ok := m.copyConfirmLines(node, true); ok {
			m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: true}
			m.confirm.Open("\u26a0 COPY LEFT \u2192 RIGHT", lines, true)
			break
		}
		m.copying = true
		return m, tea.Batch(m.copyNode(node, true, false), m.ensureTick())
	case "<":
		if m.copying {
			break
		}
		node := m.activePanel().CursorNode()
		if node == nil || node.IsAttr || m.presence(node) == model.PresenceLeftOnly {
			break
		}
		if lines, ok := m.copyConfirmLines(node, false); ok {
			m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: false}
			m.confirm.Open("\u26a0 COPY RIGHT \u2192 LEFT", lines, true)
			break
		}
		m.copying = true
		return m, tea.Batch(m.copyNode(node, false, false), m.ensureTick())
	case "=":
		m.settings.Open()
	case "S", "s":
		if !m.scanning() && !m.copying && !m.deleting {
			m.swapSides()
		}
	case "w":
		m.leftPanel.wrap = !m.leftPanel.wrap
		m.rightPanel.wrap = !m.rightPanel.wrap
	case "ctrl+l":
		return m, tea.ClearScreen
	case "?":
		m.help.Open()
	case "~", "`":
		m.logView.Open()
	case "i":
		cl, cr := m.scanner.ChecksumInfo()
		algo := m.scanner.ChecksumAlgo()
		m.readTree(func(tree *model.TreeNode) {
			if tree == nil {
				return
			}
			m.info.Open(model.PropagateStatus(tree, m.cmpOpts), "L: "+m.left.BasePath(), "R: "+m.right.BasePath(), algo, cl, cr)
		})
	case "y":
		if m.copying || m.deleting {
			break
		}
		m.openDlg.Open(transport.MaskURLPassword(m.leftArg), transport.MaskURLPassword(m.rightArg))
	case "u":
		if m.copying || m.deleting {
			break
		}
		leftPath := transport.ParentPath(m.leftArg)
		rightPath := transport.ParentPath(m.rightArg)
		if leftPath == m.leftArg && rightPath == m.rightArg {
			break
		}
		cmd, _ := m.reopenBackends(leftPath, rightPath)
		if cmd != nil {
			return m, cmd
		}
	case "o":
		node := m.activePanel().CursorNode()
		if node != nil && node.IsAttr {
			node = m.parentFileNode()
		}
		if node != nil && !node.IsDir {
			var cmd tea.Cmd
			m.readTree(func(*model.TreeNode) {
				m.diffView.Open(node.Name)
				cmd = m.loadDiffContent(node)
			})
			return m, cmd
		}
	case "b":
		if m.copying || m.deleting {
			break
		}
		node := m.activePanel().CursorNode()
		if node == nil || !node.IsDir || node.IsAttr {
			break
		}
		leftPath, rightPath := m.leftArg, m.rightArg
		descend := false
		m.readTree(func(*model.TreeNode) {
			if node.Left == nil && node.Right == nil {
				return
			}
			descend = true
			if node.Left != nil {
				leftPath = childArg(leftPath, node.RelPath)
			}
			if node.Right != nil {
				rightPath = childArg(rightPath, node.RelPath)
			}
		})
		if !descend {
			break
		}
		cmd, _ := m.reopenBackends(leftPath, rightPath)
		if cmd != nil {
			return m, cmd
		}
	}
	return m, nil
}

func (m *Model) handleDiffViewKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c", "q":
		m.diffView.Close()
	case "up", "k":
		m.diffView.ScrollUp()
	case "down", "j":
		m.diffView.ScrollDown()
	case "pgup":
		m.diffView.PageUp()
	case "pgdown":
		m.diffView.PageDown()
	case "home":
		m.diffView.Home()
	case "end":
		m.diffView.End()
	case "n":
		m.diffView.NextDiff()
	case "p":
		m.diffView.PrevDiff()
	}
	return m, nil
}

func (m *Model) loadDiffContent(node *model.TreeNode) tea.Cmd {
	left := m.left
	right := m.right
	relPath := node.RelPath
	hasLeft := node.Left != nil
	hasRight := node.Right != nil
	return func() tea.Msg {
		ctx := context.Background()
		var leftData, rightData []byte
		var err error

		if hasLeft {
			leftData, err = readAll(ctx, left, relPath)
			if err != nil {
				return diffLoadDoneMsg{err: fmt.Errorf("left: %w", err)}
			}
		}
		if hasRight {
			rightData, err = readAll(ctx, right, relPath)
			if err != nil {
				return diffLoadDoneMsg{err: fmt.Errorf("right: %w", err)}
			}
		}
		return diffLoadDoneMsg{left: leftData, right: rightData}
	}
}

func readAll(ctx context.Context, backend model.Backend, relPath string) ([]byte, error) {
	rc, err := backend.Open(ctx, relPath)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (m *Model) adjustCopyParallel(delta int) {
	if m.copyProgress.Batched.Load() {
		return
	}
	cur := m.copyParallel
	next := cur + delta
	if next < 1 {
		next = 1
	}
	if next > m.parallelMax {
		next = m.parallelMax
	}
	if next == cur {
		return
	}
	m.copyParallel = next
	m.copyProgress.Parallel.Store(int64(next))
	if s := m.copyProgress.Sem.Load(); s != nil {
		s.Resize(next)
	}
	transport.Log.Add("copy", ">>>", fmt.Sprintf("parallel=%d", next))
}

func (m *Model) handleSettingsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c", "s", "q":
		m.settings.Close()
	case "up", "k":
		m.settings.MoveUp()
	case "down", "j":
		m.settings.MoveDown()
	case "left", "h", "-":
		m.settings.Adjust(-1)
	case "right", "l", "+", "=":
		m.settings.Adjust(1)
	case " ", "enter":
		m.settings.Toggle()
	}
	return m, nil
}

func (m *Model) handleInputKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c":
		m.input.Close()
	case "enter":
		return m, m.input.Confirm()
	default:
		m.input.HandleKey(msg)
	}
	return m, nil
}

// reopenBackends switches to new open arguments; an unchanged side keeps its
// connection. The change dialog shows masked arguments, so a value equal to
// the mask means "unchanged".
func (m *Model) reopenBackends(leftPath, rightPath string) (tea.Cmd, string) {
	if leftPath == transport.MaskURLPassword(m.leftArg) {
		leftPath = m.leftArg
	}
	if rightPath == transport.MaskURLPassword(m.rightArg) {
		rightPath = m.rightArg
	}
	var newLeft, newRight model.Backend
	var err error

	if leftPath != m.leftArg {
		newLeft, err = transport.TryOpenBackend(leftPath, m.insecure, m.copyParallel)
		if err != nil {
			return nil, "left: " + err.Error()
		}
	}
	if rightPath != m.rightArg {
		newRight, err = transport.TryOpenBackend(rightPath, m.insecure, m.copyParallel)
		if err != nil {
			if newLeft != nil {
				transport.CloseBackend(newLeft)
			}
			return nil, "right: " + err.Error()
		}
	}

	m.cancelOps()

	if newLeft != nil {
		oldLeft := m.left
		go func() { transport.CloseBackend(oldLeft) }()
		m.left, m.leftArg = newLeft, leftPath
	}
	if newRight != nil {
		oldRight := m.right
		go func() { transport.CloseBackend(oldRight) }()
		m.right, m.rightArg = newRight, rightPath
	}

	m.scanner = model.NewScanner(m.left, m.right, 4, m.scanParallel, m.deepScan)
	m.leftPanel.title = m.left.BasePath()
	m.rightPanel.title = m.right.BasePath()
	m.leftPanel.SetNodes(nil)
	m.rightPanel.SetNodes(nil)
	return m.startScan(), ""
}

func (m *Model) handleOpenDlgKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c":
		m.openDlg.Close()
	case "enter":
		leftPath := m.openDlg.leftValue
		rightPath := m.openDlg.rightValue
		if leftPath == "" || rightPath == "" {
			m.openDlg.SetError("both paths are required")
			return m, nil
		}
		cmd, errMsg := m.reopenBackends(leftPath, rightPath)
		if errMsg != "" {
			m.openDlg.SetError(errMsg)
			return m, nil
		}
		m.openDlg.Close()
		return m, cmd
	default:
		m.openDlg.HandleKey(msg)
	}
	return m, nil
}

func (m *Model) handleConfirmKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.confirm.choiceMode && m.pendingDelete != nil {
		var side model.Presence
		found := true
		switch msg.String() {
		case "a":
			side = model.PresenceLeftOnly
		case "l":
			side = model.PresenceRightOnly
		case "enter":
			side = model.PresenceBoth
		case "esc", "ctrl+c", "q":
			m.confirm.Close()
			m.pendingDelete = nil
			return m, nil
		default:
			found = false
		}
		if found {
			m.confirm.Close()
			node := m.pendingDelete
			m.pendingDelete = nil
			m.deleting = true
			return m, tea.Batch(m.deleteNode(node, side), m.ensureTick())
		}
		return m, nil
	}

	switch msg.String() {
	case "y", "Y":
		m.confirm.Close()
		if m.pendingDelete != nil {
			node := m.pendingDelete
			m.pendingDelete = nil
			m.deleting = true
			return m, tea.Batch(m.deleteNode(node, m.presence(node)), m.ensureTick())
		}
		if m.pendingCopy != nil {
			pc := m.pendingCopy
			m.pendingCopy = nil
			m.copying = true
			return m, tea.Batch(m.copyNode(pc.node, pc.leftToRight, true), m.ensureTick())
		}
	case "esc", "ctrl+c", "n", "N", "q":
		m.confirm.Close()
		m.pendingDelete = nil
		m.pendingCopy = nil
	}
	return m, nil
}

// presence reads one node's compare presence under the read lock; the scanner
// rewrites it whenever the node's directory is re-listed.
func (m *Model) presence(node *model.TreeNode) model.Presence {
	var p model.Presence
	m.readTree(func(*model.TreeNode) { p = node.Compare.Presence })
	return p
}

// copyConfirmLines counts collisions and mirror deletes across the subtree, so
// it runs the free function of the same name under the read lock.
func (m *Model) copyConfirmLines(node *model.TreeNode, leftToRight bool) ([]string, bool) {
	var lines []string
	var ok bool
	m.readTree(func(*model.TreeNode) { lines, ok = copyConfirmLines(node, leftToRight) })
	return lines, ok
}

// copyConfirmLines builds the destructive-copy confirmation body, or reports
// false when the copy destroys nothing and needs no confirmation. Counts come
// from the in-memory tree: an unlisted dir reads as empty, so the totals are a
// lower bound until the copy itself lists the subtree — say so rather than
// quote a number the delete pass will exceed.
func copyConfirmLines(node *model.TreeNode, leftToRight bool) ([]string, bool) {
	side := "right"
	if !leftToRight {
		side = "left"
	}
	collisions := len(model.CollectTypeCollisions(node, leftToRight))
	var delFiles, delDirs int
	if node.IsDir {
		delFiles, delDirs = model.CountMirrorDeletes(node, leftToRight)
	}
	if collisions == 0 && delFiles == 0 && delDirs == 0 {
		return nil, false
	}
	name := node.Name
	if node.IsDir {
		name += "/"
	}
	lines := []string{"", name}
	if collisions > 0 {
		lines = append(lines, fmt.Sprintf("Will replace %d type conflicts (file↔dir) on %s", collisions, side))
	}
	if delFiles > 0 || delDirs > 0 {
		lines = append(lines, fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on "+side)
	}
	if model.UnlistedDir(node) != nil {
		lines = append(lines, "", "⚠ subtree not fully scanned - actual counts may be higher")
	}
	return lines, true
}

func (m *Model) openDelete(node *model.TreeNode) {
	m.pendingDelete = node
	m.readTree(func(*model.TreeNode) { m.buildDeleteConfirm(node) })
}

func (m *Model) buildDeleteConfirm(node *model.TreeNode) {
	if node.Compare.Presence != model.PresenceBoth {
		sides := "← left side only"
		if node.Compare.Presence == model.PresenceRightOnly {
			sides = "right side only →"
		}
		if !node.IsDir {
			m.confirm.Open("Delete "+node.Name+"?", []string{"", sides}, false)
			return
		}
		files, dirs, complete := model.CountDescendants(node)
		countStr := fmt.Sprintf("%d files, %d folders", files, dirs)
		if !complete {
			countStr = fmt.Sprintf("%d+ files, %d+ folders (not fully scanned)", files, dirs)
		} else if files == 0 && dirs == 0 {
			countStr = "empty folder"
		}
		m.confirm.Open("\u26a0 RECURSIVE DELETE", []string{"", node.Name + "/", countStr, sides}, true)
		return
	}

	if !node.IsDir {
		m.confirm.OpenChoice("Delete "+node.Name+"?", []string{""}, false)
		return
	}
	files, dirs, complete := model.CountDescendants(node)
	countStr := fmt.Sprintf("%d files, %d folders", files, dirs)
	if !complete {
		countStr = fmt.Sprintf("%d+ files, %d+ folders (not fully scanned)", files, dirs)
	} else if files == 0 && dirs == 0 {
		countStr = "empty folder"
	}
	m.confirm.OpenChoice("\u26a0 RECURSIVE DELETE", []string{"", node.Name + "/", countStr}, true)
}

func (m *Model) deleteNode(node *model.TreeNode, side model.Presence) tea.Cmd {
	left := m.left
	right := m.right
	scanner := m.scanner
	opts := *m.cmpOpts
	isDir := node.IsDir
	var relPath string
	var files, dirs int
	m.readTree(func(*model.TreeNode) {
		relPath = node.RelPath
		files, dirs, _ = model.CountDescendants(node)
	})
	progress := m.deleteProgress
	delLeft := side != model.PresenceRightOnly
	delRight := side != model.PresenceLeftOnly
	sideLabel := "BOTH"
	if !delLeft {
		sideLabel = "RIGHT"
	} else if !delRight {
		sideLabel = "LEFT"
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	baseCtx = transport.ContextWithFatalCancel(baseCtx, cancel)
	progress.Cancel.Store(&cancelFn{f: cancel})
	progress.Done.Store(0)
	progress.File.Store(relPath)
	progress.Side.Store(sideLabel)
	progress.Start.Store(time.Now().UnixNano())
	progress.Enumerating.Store(false)

	perSide := int64(files + dirs)
	if isDir {
		perSide++
	}
	if perSide == 0 {
		perSide = 1
	}
	var sides int64
	if delLeft {
		sides++
	}
	if delRight {
		sides++
	}
	progress.Total.Store(perSide * sides)

	return func() tea.Msg {
		ctx := baseCtx

		if delLeft && ctx.Err() == nil {
			err := removeOne(ctx, left, relPath, isDir)
			if err != nil {
				transport.Log.Add("delete", "ERR", "left "+relPath+": "+err.Error())
			} else {
				transport.Log.Add("delete", "<<<", "left "+relPath)
			}
			progress.Done.Add(perSide)
		}
		if delRight && ctx.Err() == nil {
			err := removeOne(ctx, right, relPath, isDir)
			if err != nil {
				transport.Log.Add("delete", "ERR", "right "+relPath+": "+err.Error())
			} else {
				transport.Log.Add("delete", "<<<", "right "+relPath)
			}
			progress.Done.Add(perSide)
		}

		refreshCtx := context.Background()
		parentDir := model.DirOf(relPath)
		le, re, err := scanner.ListBothDir(refreshCtx, parentDir)
		if err == nil {
			scanner.RefreshDir(parentDir, le, re, opts)
		}
		return deleteDoneMsg{}
	}
}

func removeOne(ctx context.Context, backend model.Backend, relPath string, isDir bool) error {
	if isDir {
		return backend.RemoveAll(ctx, relPath)
	}
	return backend.Remove(ctx, relPath)
}

// copyItem and treeEntry are the snapshots a copy works from: the live tree is
// only readable under the scanner's lock, which a transfer cannot hold. Entry
// pointers are safe to keep — a rescan swaps a node's entry for a new one
// rather than rewriting the old.
type copyItem struct {
	relPath  string
	src, dst *model.FileEntry
}

type treeEntry struct {
	relPath string
	isDir   bool
}

func (m *Model) copyNode(node *model.TreeNode, leftToRight bool, mirror bool) tea.Cmd {
	left := m.left
	right := m.right
	scanner := m.scanner
	opts := *m.cmpOpts
	progress := m.copyProgress
	parallel := max(m.copyParallel, 1)
	parallelMax := max(m.parallelMax, parallel)
	batchEnabled, verifyResume := m.batchTransfer, m.verifyResume
	baseCtx, cancel := context.WithCancel(context.Background())
	baseCtx = transport.ContextWithFatalCancel(baseCtx, cancel)
	progress.Reset(parallel, parallelMax, leftToRight)
	progress.Cancel.Store(&cancelFn{f: cancel})
	nodeIsDir := node.IsDir
	var nodeRel string
	m.readTree(func(*model.TreeNode) { nodeRel = node.RelPath })
	return func() tea.Msg {
		ctx := transport.ContextWithProgress(baseCtx, &progress.Bytes)
		ctx = transport.ContextWithBaseProgress(ctx, &progress.BaseBytes)

		// Copy and mirror-delete enumerate the in-memory tree, which shows an
		// unlisted dir as empty. List the whole subtree first or the copy
		// silently skips it and mirror under-counts what to delete.
		progress.Listing.Store(true)
		listed := !nodeIsDir || scanner.EnsureSubtreeListed(ctx, node, opts)
		progress.Listing.Store(false)
		if !listed {
			transport.Log.Add("copy", "ERR", "aborted "+nodeRel+": subtree could not be fully listed")
			return copyDoneMsg{}
		}

		var dstBackend model.Backend
		if leftToRight {
			dstBackend = right
		} else {
			dstBackend = left
		}
		// A copy outlives any lock it could sensibly hold, so both enumerations
		// snapshot what they need from the live tree in one locked pass and the
		// transfer below never touches a node again.
		var collisions []treeEntry
		var files []copyItem
		var totalBytes int64
		scanner.ReadTree(func(*model.TreeNode) {
			for _, c := range model.CollectTypeCollisions(node, leftToRight) {
				dstEntry := c.Right
				if !leftToRight {
					dstEntry = c.Left
				}
				if dstEntry == nil {
					continue
				}
				collisions = append(collisions, treeEntry{relPath: c.RelPath, isDir: dstEntry.IsDir})
			}
			nodes := []*model.TreeNode{node}
			if nodeIsDir {
				nodes = model.CollectCopyFiles(node, &opts, leftToRight)
			}
			files = make([]copyItem, 0, len(nodes))
			for _, f := range nodes {
				it := copyItem{relPath: f.RelPath, src: f.Left, dst: f.Right}
				if !leftToRight {
					it.src, it.dst = f.Right, f.Left
				}
				if it.src != nil {
					totalBytes += it.src.Size
				}
				files = append(files, it)
			}
		})

		for _, c := range collisions {
			if ctx.Err() != nil {
				break
			}
			var err error
			if c.isDir {
				err = dstBackend.RemoveAll(ctx, c.relPath)
			} else {
				err = dstBackend.Remove(ctx, c.relPath)
			}
			if err != nil {
				progress.Failed.Add(1)
				transport.Log.Add("copy", "ERR", "type-collision cleanup "+c.relPath+": "+err.Error())
			} else {
				transport.Log.Add("copy", "<<<", "type-collision cleanup "+c.relPath)
			}
		}

		progress.Total.Store(int64(len(files)))
		progress.TotalBytes.Store(totalBytes)
		progress.Start.Store(time.Now().UnixNano())

		dstChanged := make(map[string]bool)
		changedDir := ""
		var changedMu sync.Mutex
		markChanged := func(relPath string) {
			changedMu.Lock()
			dstChanged[relPath] = true
			changedMu.Unlock()
		}

		srcBackend := left
		if !leftToRight {
			srcBackend = right
		}
		batched := false
		if batchEnabled && nodeIsDir {
			if bs, ok := dstBackend.(model.BatchSender); ok {
				if lp, ok := srcBackend.(model.LocalFS); ok {
					srcRoot := lp.LocalPath("")
					srcSubtree := filepath.Join(srcRoot, nodeRel)
					var batchFiles, batchBytes int64
					_ = filepath.WalkDir(srcSubtree, func(_ string, d fs.DirEntry, werr error) error {
						if werr != nil || d.IsDir() {
							return nil
						}
						info, ierr := d.Info()
						if ierr != nil {
							return nil
						}
						batchFiles++
						batchBytes += info.Size()
						return nil
					})
					if batchFiles > 0 {
						progress.Total.Store(batchFiles)
						progress.TotalBytes.Store(batchBytes)
						progress.InFlight.Store(1)
						progress.Parallel.Store(1)
						progress.Batched.Store(true)
						transport.Log.Add("copy", ">>>", fmt.Sprintf("BATCH %s (%d files, %s)", nodeRel, batchFiles, model.FormatSize(batchBytes)))
						bctx := transport.ContextWithFileSize(ctx, batchBytes)
						err := bs.SendLocalTree(bctx, srcRoot, nodeRel, func(name string) {
							progress.File.Store(name)
							if d := progress.Done.Load(); d < progress.Total.Load() {
								progress.Done.Add(1)
							}
						})
						progress.InFlight.Store(0)
						if err == nil {
							progress.Done.Store(progress.Total.Load())
							progress.CompletedBytes.Store(progress.Bytes.Load())
							progress.CompletedBaseBytes.Store(progress.BaseBytes.Load())
							// Batch rewrites every file in the subtree, not just
							// the diff set, so invalidate cached CRC for the whole
							// subtree rather than the diff paths alone.
							changedDir = nodeRel
							batched = true
							transport.Log.Add("copy", "<<<", "BATCH "+nodeRel+" OK")
						} else {
							if !errors.Is(err, transport.ErrUnsupported) {
								transport.Log.Add("copy", "ERR", "BATCH "+nodeRel+": "+err.Error())
							}
							progress.Total.Store(int64(len(files)))
							progress.TotalBytes.Store(totalBytes)
							progress.Done.Store(0)
							progress.Bytes.Store(0)
							progress.CompletedBytes.Store(0)
							progress.CompletedBaseBytes.Store(0)
						}
						progress.Batched.Store(false)
					}
				}
			}
		}

		if parallel > 1 && !batched {
			transport.Log.Add("copy", ">>>", fmt.Sprintf("parallel=%d", parallel))
		}
		sem := newDynSem(parallel)
		progress.Sem.Store(sem)
		defer progress.Sem.Store(nil)
		var wg sync.WaitGroup

		copyOne := func(f copyItem) {
			src, dst := left, right
			if !leftToRight {
				src, dst = right, left
			}
			srcEntry, dstEntry := f.src, f.dst
			if srcEntry == nil {
				progress.Done.Add(1)
				return
			}
			if srcEntry.IsDir {
				progress.File.Store(f.relPath)
				progress.BeginFile(0)
				if err := dst.Mkdir(ctx, f.relPath, srcEntry.Mode); err != nil {
					progress.Failed.Add(1)
					transport.Log.Add("copy", "ERR", "mkdir "+f.relPath+": "+err.Error())
				} else {
					transport.Log.Add("copy", "<<<", "mkdir "+f.relPath)
				}
				progress.Done.Add(1)
				return
			}
			if dstEntry != nil && dstEntry.IsDir != srcEntry.IsDir {
				var clearErr error
				if dstEntry.IsDir {
					clearErr = dst.RemoveAll(ctx, f.relPath)
				} else {
					clearErr = dst.Remove(ctx, f.relPath)
				}
				if clearErr != nil {
					progress.Failed.Add(1)
					transport.Log.Add("copy", "ERR", "clear dst type-mismatch "+f.relPath+": "+clearErr.Error())
					progress.Done.Add(1)
					return
				}
				transport.Log.Add("copy", "<<<", "cleared dst type-mismatch "+f.relPath)
				dstEntry = nil
			}
			slot := progress.ClaimSlot()
			defer progress.ReleaseSlot(slot)
			if slot != nil {
				slot.File.Store(f.relPath)
				slot.Size.Store(srcEntry.Size)
				slot.Start.Store(time.Now().UnixNano())
			}
			progress.File.Store(f.relPath)
			progress.BeginFile(srcEntry.Size)
			transport.Log.Add("copy", ">>>", fmt.Sprintf("COPY %s (%s)", f.relPath, model.FormatSize(srcEntry.Size)))

			slotBytes := &progress.Bytes
			slotBase := &progress.BaseBytes
			if slot != nil {
				slotBytes = &slot.Bytes
				slotBase = &slot.BaseBytes
			}
			fileCtx := transport.ContextWithProgress(ctx, slotBytes)
			fileCtx = transport.ContextWithBaseProgress(fileCtx, slotBase)
			fileCtx = transport.ContextWithFileSize(fileCtx, srcEntry.Size)
			fileCtx = transport.ContextWithModTime(fileCtx, srcEntry.ModTime)

			verify := resumeVerifier(verifyResume, scanner, src, dst, f.relPath, srcEntry.Size)
			setTimes := func() {
				if err := dst.SetTimes(fileCtx, f.relPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime); err != nil {
					transport.Log.Add("copy", "ERR", "settimes "+f.relPath+": "+err.Error())
				}
			}

			// Resume first when a partial dst body exists: append the missing
			// tail rather than overwrite the whole file. tryResumeCopy
			// self-gates (no-op for absent/full/oversized dst). The appended
			// prefix is never read back, so verify checks it afterwards.
			var resumeOK bool
			_ = transport.WithStallGuard(fileCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
				resumeOK = tryResumeCopy(attemptCtx, src, dst, f.relPath, srcEntry, dstEntry, slotBytes, slotBase, verify)
				return nil
			})
			if resumeOK {
				setTimes()
				markChanged(f.relPath)
				transport.Log.Add("copy", "<<<", "COPY "+f.relPath+" OK (resumed)")
				progress.Done.Add(1)
				return
			}

			var directOK bool
			_ = transport.WithStallGuard(fileCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
				directOK = tryDirectTransfer(attemptCtx, src, dst, f.relPath, srcEntry)
				return nil
			})
			if directOK {
				setTimes()
				markChanged(f.relPath)
				transport.Log.Add("copy", "<<<", "COPY "+f.relPath+" OK")
				progress.Done.Add(1)
				return
			}

			attempt := 0
			err := transport.Retry(fileCtx, "copy", "copy "+f.relPath, func() error {
				return transport.WithStallGuard(fileCtx, slotBytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
					attempt++
					if attempt > 1 {
						offset := peekDstSize(attemptCtx, dst, f.relPath)
						if offset > 0 && offset < srcEntry.Size {
							err := resumeAttempt(attemptCtx, src, dst, f.relPath, srcEntry, offset, slotBytes, slotBase, verify)
							if err == nil {
								return nil
							}
							if !errors.Is(err, transport.ErrUnsupported) && !errors.Is(err, errResumeMismatch) {
								return err
							}
						}
					}
					return fullCopyAttempt(attemptCtx, src, dst, f.relPath, srcEntry, slotBytes)
				})
			})
			if err == nil {
				setTimes()
				markChanged(f.relPath)
				transport.Log.Add("copy", "<<<", "COPY "+f.relPath+" OK")
			} else {
				progress.Failed.Add(1)
				transport.Log.Add("copy", "ERR", "COPY "+f.relPath+": "+err.Error())
			}
			progress.Done.Add(1)
		}

		if !batched {
		dispatch:
			for _, f := range files {
				if err := sem.Acquire(ctx); err != nil {
					break dispatch
				}
				wg.Add(1)
				progress.InFlight.Add(1)
				go func(f copyItem) {
					defer wg.Done()
					defer sem.Release()
					defer progress.InFlight.Add(-1)
					copyOne(f)
				}(f)
			}
			wg.Wait()
		}

		var rescanRoot *model.TreeNode
		if nodeIsDir {
			// Mirror deletes destination-only files, so it must only run once
			// every copy landed. A partial copy plus a full delete pass would
			// destroy data the source still holds.
			failed := progress.Failed.Load()
			switch {
			case !mirror:
			case ctx.Err() != nil:
				transport.Log.Add("copy", "ERR", "mirror delete skipped: copy canceled")
			case failed > 0:
				transport.Log.Add("copy", "ERR", fmt.Sprintf("mirror delete skipped: %d file(s) failed to copy", failed))
			default:
				delBackend := right
				if !leftToRight {
					delBackend = left
				}
				var deletes []treeEntry
				scanner.ReadTree(func(*model.TreeNode) {
					for _, d := range model.CollectMirrorDeletes(node, leftToRight) {
						deletes = append(deletes, treeEntry{relPath: d.RelPath, isDir: d.IsDir})
					}
				})
				for _, d := range deletes {
					if ctx.Err() != nil {
						break
					}
					var err error
					if d.isDir {
						err = delBackend.RemoveAll(ctx, d.relPath)
					} else {
						err = delBackend.Remove(ctx, d.relPath)
					}
					if err != nil {
						transport.Log.Add("copy", "ERR", "mirror delete "+d.relPath+": "+err.Error())
						continue
					}
					transport.Log.Add("copy", "<<<", "mirror delete "+d.relPath)
				}
			}
			rescanRoot = node
		} else {
			rescanRoot = scanner.FindNearestDestNode(model.DirOf(nodeRel), leftToRight)
		}
		if failed := progress.Failed.Load(); failed > 0 {
			transport.Log.Add("copy", "ERR", fmt.Sprintf("COPY finished with %d failure(s) of %d", failed, progress.Total.Load()))
		}
		changed := &model.ChangedPaths{}
		var changedDirs []string
		if batched {
			changedDirs = []string{changedDir}
		}
		if leftToRight {
			changed.Right, changed.RightDirs = dstChanged, changedDirs
		} else {
			changed.Left, changed.LeftDirs = dstChanged, changedDirs
		}
		return copyDoneMsg{rescanRoot: rescanRoot, changed: changed}
	}
}

// tryDirectTransfer attempts a path-to-path transfer (e.g. rsync directly
// between local filesystem and remote rsync daemon) when one side exposes a
// LocalFS path and the other side supports a direct send/receive. This avoids
// any intermediate tmp file and lets rsync do its own delta-sync resume
// against whatever already exists at the destination. Returns true on success.
func tryDirectTransfer(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry) bool {
	// src is local-backed and dst can pull a local file directly. Backends
	// implementing LocalSender self-credit progress.Bytes during the push and
	// top up to fileSize on success — see RsyncSSHBackend/RsyncBackend
	// SendLocalFile. Don't credit again here.
	if lp, ok := src.(model.LocalFS); ok {
		if r, ok2 := dst.(model.LocalSender); ok2 {
			err := r.SendLocalFile(ctx, lp.LocalPath(relPath), relPath, srcEntry.Mode)
			return err == nil
		}
	}
	// dst is local-backed and src can push directly to a local path.
	if lp, ok := dst.(model.LocalFS); ok {
		if r, ok2 := src.(model.LocalReceiver); ok2 {
			// RecvToLocalFile uses tailDirSize and credits counter as the
			// destination grows, so no manual top-up here.
			err := r.RecvToLocalFile(ctx, relPath, lp.LocalPath(relPath))
			return err == nil
		}
	}
	return false
}

// tryResumeCopy attempts to resume an interrupted upload by appending only the
// missing tail of srcEntry onto an existing partial dst file. Returns true on
// success; false if resume is not applicable, not supported by either backend,
// or if any step fails (caller should fall back to a full overwrite copy).
//
// On success the global Bytes counter ends up offset+(src.Size-offset) higher;
// the offset portion is also tracked in BaseBytes so it doesn't inflate the
// transfer-rate calculation. On failure any progress credited along the way is
// rolled back so a fallback CopyFrom can re-credit the full source size
// without double-counting.
func tryResumeCopy(ctx context.Context, src, dst model.Backend, relPath string, srcEntry, dstEntry *model.FileEntry, bytes, baseBytes *atomic.Int64, verify func(context.Context) error) bool {
	if dstEntry == nil || dstEntry.IsDir {
		return false
	}
	if dstEntry.Size <= 0 || dstEntry.Size >= srcEntry.Size {
		return false
	}
	if _, ok := dst.(model.Resumer); !ok {
		return false
	}
	// The scan-time size goes stale as soon as anything else writes to dst, and
	// appending at the wrong offset corrupts the file. Re-read it live; a dst
	// that has since grown past srcEntry.Size (or vanished) falls back to a
	// full copy.
	offset := peekDstSize(ctx, dst, relPath)
	if offset <= 0 || offset >= srcEntry.Size {
		return false
	}
	return resumeAttempt(ctx, src, dst, relPath, srcEntry, offset, bytes, baseBytes, verify) == nil
}

// errResumeMismatch reports that a resumed file did not match the source after
// the append. Callers treat it like ErrUnsupported: fall back to a full
// overwrite copy rather than retrying the append.
var errResumeMismatch = errors.New("resumed file does not match source")

// resumeVerifier returns the post-append check for a resumed copy, or nil when
// verification is off. Resume trusts whatever prefix already sits at the
// destination — same size, different bytes produces a wrong file that otherwise
// reports success — so compare both sides once the append lands.
func resumeVerifier(enabled bool, scanner *model.Scanner, src, dst model.Backend, relPath string, size int64) func(context.Context) error {
	if !enabled {
		return nil
	}
	return func(ctx context.Context) error {
		if got := peekDstSize(ctx, dst, relPath); got != size {
			return fmt.Errorf("%w: %s (%d bytes at destination, source has %d)", errResumeMismatch, relPath, got, size)
		}
		// Without a shared algorithm the two sides would hash differently and
		// every comparison would read as a mismatch; the size check above is
		// all the verification available.
		if !scanner.NegotiateChecksum() {
			transport.Log.Add("copy", "ERR", "verify "+relPath+": no checksum algorithm shared by both sides, resumed content unverified")
			return nil
		}
		srcSum, serr := src.Checksum(ctx, relPath)
		dstSum, derr := dst.Checksum(ctx, relPath)
		if serr != nil || derr != nil || srcSum == "" || dstSum == "" {
			transport.Log.Add("copy", "ERR", "verify "+relPath+": checksum unavailable, resumed content unverified")
			return nil
		}
		if srcSum != dstSum {
			return fmt.Errorf("%w: %s (src %s, dst %s)", errResumeMismatch, relPath, srcSum, dstSum)
		}
		return nil
	}
}

// peekDstSize returns the current size of relPath on dst, or 0 if it can't be
// determined. Used between retry attempts to find how many bytes of a partial
// upload survived so the next attempt can resume rather than restart.
func peekDstSize(ctx context.Context, dst model.Backend, relPath string) int64 {
	parent := model.DirOf(relPath)
	entries, err := dst.List(ctx, parent)
	if err != nil {
		return 0
	}
	name := path.Base(relPath)
	for i := range entries {
		if entries[i].Name == name && !entries[i].IsDir {
			return entries[i].Size
		}
	}
	return 0
}

// resumeAttempt resumes a partial upload by appending bytes from offset onward,
// then runs verify (if any) against the finished file. Returns
// transport.ErrUnsupported if dst can't append, errResumeMismatch if the
// result doesn't match the source; on any error the progress credited during
// the attempt is rolled back. The offset portion is also tracked in BaseBytes
// so it doesn't inflate the transfer-rate calculation.
func resumeAttempt(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry, offset int64, bytes, baseBytes *atomic.Int64, verify func(context.Context) error) error {
	resumer, ok := dst.(model.Resumer)
	if !ok {
		return transport.ErrUnsupported
	}
	bytes.Add(offset)
	baseBytes.Add(offset)
	var added atomic.Int64
	added.Store(offset)
	opener := &trackedRangeOpener{
		Backend:  src,
		RelPath:  relPath,
		FileSize: srcEntry.Size,
		Ctx:      ctx,
		Target:   bytes,
		Added:    &added,
	}
	rollback := func() {
		bytes.Add(-added.Load())
		baseBytes.Add(-offset)
	}
	if err := resumer.AppendFrom(ctx, relPath, opener, srcEntry.Mode, offset); err != nil {
		rollback()
		return err
	}
	if verify == nil {
		return nil
	}
	if err := verify(ctx); err != nil {
		transport.Log.Add("copy", "ERR", err.Error()+" — recopying in full")
		rollback()
		return err
	}
	return nil
}

// fullCopyAttempt opens src from byte 0 and writes the whole file via CopyFrom.
// On failure any progress credited during the attempt is rolled back.
func fullCopyAttempt(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry, counter *atomic.Int64) error {
	reader, err := src.Open(ctx, relPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	defer startCancelCloser(ctx, reader)()
	dstOwnsProgress := false
	if owner, ok := dst.(transport.ProgressOwner); ok && owner.OwnsCopyProgress() {
		dstOwnsProgress = true
	}
	var added atomic.Int64
	var srcReader io.Reader = sizedReader{Reader: reader, size: srcEntry.Size}
	if !transport.IsPreCounted(reader) && !dstOwnsProgress {
		srcReader = &trackedReader{r: srcReader, target: counter, added: &added}
	}
	srcReader = &cancelReader{r: srcReader, ctx: ctx}
	if err := dst.CopyFrom(ctx, relPath, srcReader, srcEntry.Mode); err != nil {
		counter.Add(-added.Load())
		return err
	}
	return nil
}

// trackedRangeOpener implements model.RangeOpener for resume flows. OpenAt
// wraps the returned reader so each tail byte read increments Target/Added.
// Open (used by rsync-style backends that own their own progress accounting)
// is left un-instrumented — those backends drive progress via
// transport.progressFromContext during the rsync push.
type trackedRangeOpener struct {
	Backend  model.Backend
	RelPath  string
	FileSize int64
	Ctx      context.Context
	Target   *atomic.Int64
	Added    *atomic.Int64
}

func (o *trackedRangeOpener) Size() int64 { return o.FileSize }

func (o *trackedRangeOpener) LocalPath() string {
	if lp, ok := o.Backend.(model.LocalFS); ok {
		return lp.LocalPath(o.RelPath)
	}
	return ""
}

func (o *trackedRangeOpener) Open(ctx context.Context) (io.ReadCloser, error) {
	rd, err := o.Backend.Open(ctx, o.RelPath)
	if err != nil {
		return nil, err
	}
	return &cancelReadCloser{rc: rd, ctx: o.Ctx}, nil
}

func (o *trackedRangeOpener) OpenAt(ctx context.Context, offset int64) (io.ReadCloser, error) {
	var rd io.ReadCloser
	if seeker, ok := o.Backend.(model.SeekableOpener); ok {
		r, err := seeker.OpenAt(ctx, o.RelPath, offset)
		if err != nil && !errors.Is(err, transport.ErrUnsupported) {
			return nil, err
		}
		rd = r
	}
	if rd == nil {
		r, err := o.Backend.Open(ctx, o.RelPath)
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			if _, err := io.CopyN(io.Discard, r, offset); err != nil {
				r.Close()
				return nil, err
			}
		}
		rd = r
	}
	if transport.IsPreCounted(rd) {
		return &cancelReadCloser{rc: rd, ctx: o.Ctx}, nil
	}
	return &cancelReadCloser{
		rc:  &trackedReadCloser{rc: rd, target: o.Target, added: o.Added},
		ctx: o.Ctx,
	}, nil
}

type trackedReadCloser struct {
	rc     io.ReadCloser
	target *atomic.Int64
	added  *atomic.Int64
}

func (t *trackedReadCloser) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		t.target.Add(int64(n))
		t.added.Add(int64(n))
	}
	return n, err
}

func (t *trackedReadCloser) Close() error { return t.rc.Close() }

type cancelReadCloser struct {
	rc  io.ReadCloser
	ctx context.Context
}

func (c *cancelReadCloser) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.rc.Read(p)
}

func (c *cancelReadCloser) Close() error { return c.rc.Close() }

type trackedReader struct {
	r      io.Reader
	target *atomic.Int64
	added  *atomic.Int64
}

func (t *trackedReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.target.Add(int64(n))
		t.added.Add(int64(n))
	}
	return n, err
}

type cancelReader struct {
	r   io.Reader
	ctx context.Context
}

func (c *cancelReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func startCancelCloser(ctx context.Context, c io.Closer) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func (m *Model) openRename(node *model.TreeNode) {
	var oldName string
	m.readTree(func(*model.TreeNode) { oldName = node.Name })
	m.input.Open("Rename: "+oldName, oldName, func(newName string) tea.Cmd {
		if newName == "" || newName == oldName {
			return nil
		}
		opts := *m.cmpOpts
		var oldRel string
		var presence model.Presence
		m.readTree(func(*model.TreeNode) {
			oldRel = node.RelPath
			presence = node.Compare.Presence
		})
		newRel := model.DirOf(oldRel)
		if newRel != "" {
			newRel += "/"
		}
		newRel += newName
		return func() tea.Msg {
			ctx := context.Background()
			var err error
			switch presence {
			case model.PresenceLeftOnly:
				err = m.left.Rename(ctx, oldRel, newRel)
			case model.PresenceRightOnly:
				err = m.right.Rename(ctx, oldRel, newRel)
			default:
				err = m.left.Rename(ctx, oldRel, newRel)
				if rerr := m.right.Rename(ctx, oldRel, newRel); err == nil {
					err = rerr
				}
			}
			if err != nil {
				transport.Log.Add("rename", "ERR", oldRel+" -> "+newRel+": "+err.Error())
				return renameDoneMsg{err: err}
			}
			if m.scanner.RenameNode(node, newName, newRel, oldRel, opts) {
				return renameDoneMsg{rescan: node}
			}
			return renameDoneMsg{}
		}
	})
}

func (m *Model) touchNode(node *model.TreeNode) tea.Cmd {
	left := m.left
	right := m.right
	scanner := m.scanner
	opts := *m.cmpOpts
	return func() tea.Msg {
		ctx := context.Background()
		var l, r *model.FileEntry
		var relPath string
		scanner.ReadTree(func(*model.TreeNode) { l, r, relPath = node.Left, node.Right, node.RelPath })
		if l == nil || r == nil {
			return touchDoneMsg{}
		}
		newer, older := l, r
		olderBackend := right
		touchedLeft := false
		if r.ModTime.After(l.ModTime) {
			newer, older = r, l
			olderBackend = left
			touchedLeft = true
		}
		if err := olderBackend.SetTimes(ctx, older.RelPath, newer.ModTime, newer.ATime, newer.BirthTime); err == nil {
			// Touch only changes metadata; the file body is unchanged. Roll the
			// cached CRC fingerprint forward to the new mtime so the preserving
			// merge below treats CRC as still valid.
			scanner.MutateTree(func(*model.TreeNode) {
				if touchedLeft {
					node.LeftCksumModTime = newer.ModTime
					return
				}
				node.RightCksumModTime = newer.ModTime
			})
		}
		parentDir := model.DirOf(relPath)
		le, re, err := scanner.ListBothDir(ctx, parentDir)
		if err == nil {
			scanner.RefreshDir(parentDir, le, re, opts)
		}
		return touchDoneMsg{}
	}
}

func (m *Model) openSearch() {
	m.input.Open("Search (regex):", "", func(query string) tea.Cmd {
		if query == "" {
			return nil
		}
		re, err := regexp.Compile(query)
		if err != nil {
			return nil
		}
		m.findAndJump(re)
		return nil
	})
}

func (m *Model) findAndJump(re *regexp.Regexp) {
	var target *model.TreeNode
	m.mutateTree(func(tree *model.TreeNode) {
		if tree == nil {
			return
		}
		target = model.FindByName(tree, nil, re)
		if target == nil {
			return
		}
		for n := target.Parent; n != nil; n = n.Parent {
			n.Expanded = true
		}
	})
	if target == nil {
		return
	}
	m.refreshTree()
	p := m.activePanel()
	for i, n := range p.nodes {
		if n == target {
			p.cursor = i
			p.clampOffset()
			break
		}
	}
	m.syncPanels()
}

func (m *Model) jumpToNextDiff() {
	p := m.activePanel()
	var after *model.TreeNode
	for i := p.cursor; i >= 0 && i < len(p.nodes); i-- {
		if !p.nodes[i].IsAttr {
			after = p.nodes[i]
			break
		}
	}
	var target *model.TreeNode
	m.mutateTree(func(tree *model.TreeNode) {
		if tree == nil {
			return
		}
		target = model.FindNextDiff(tree, after, m.cmpOpts)
		if target == nil {
			return
		}
		for n := target.Parent; n != nil; n = n.Parent {
			n.Expanded = true
		}
		target.Expanded = true
	})
	if target == nil {
		return
	}
	m.refreshTree()
	for i, n := range p.nodes {
		if n == target {
			p.cursor = i
			p.clampOffset()
			break
		}
	}
	m.syncPanels()
}

func (m *Model) parentFileNode() *model.TreeNode {
	p := m.activePanel()
	for i := p.cursor - 1; i >= 0; i-- {
		if !p.nodes[i].IsAttr {
			return p.nodes[i]
		}
	}
	return nil
}

func (m *Model) activePanel() *Panel {
	if m.activeLeft {
		return m.leftPanel
	}
	return m.rightPanel
}

func (m *Model) inactivePanel() *Panel {
	if m.activeLeft {
		return m.rightPanel
	}
	return m.leftPanel
}

func (m *Model) syncPanels() {
	src := m.activePanel()
	dst := m.inactivePanel()
	dst.cursor = src.cursor
	dst.offset = src.offset
}

func (m *Model) scanning() bool     { return m.scan != nil }
func (m *Model) checksumming() bool { return m.cksum != nil }

// busy reports whether any background operation is live, which is what keeps
// the tick loop running.
func (m *Model) busy() bool {
	return m.scanning() || m.checksumming() || m.copying || m.deleting
}

// startOp runs one scanner operation in the background under its own
// cancellable context and reports back with the op's generation.
func (m *Model) startOp(kind opKind, run func(ctx context.Context)) tea.Cmd {
	m.opGen++
	ctx, cancel := context.WithCancel(context.Background())
	op := &scanOp{gen: m.opGen, cancel: cancel}
	if kind == opChecksum {
		m.cksum = op
	} else {
		m.scan = op
	}
	return tea.Batch(func() tea.Msg {
		run(ctx)
		return opDoneMsg{kind: kind, gen: op.gen}
	}, m.ensureTick())
}

// finishOp clears the op a done message belongs to and ignores messages from
// cancelled predecessors. A rescan queued behind the scan starts now.
func (m *Model) finishOp(msg opDoneMsg) tea.Cmd {
	slot := &m.scan
	if msg.kind == opChecksum {
		slot = &m.cksum
	}
	if *slot == nil || (*slot).gen != msg.gen {
		return nil
	}
	(*slot).cancel()
	*slot = nil
	m.refreshTreeNow()
	if msg.kind == opScan && m.pendingRescan != nil {
		r := m.pendingRescan
		m.pendingRescan = nil
		return m.rescanNode(r.node, r.changed)
	}
	return nil
}

// queueRescan starts a rescan now, or once the running scan finishes. Two
// pending rescans collapse into one of the whole tree.
func (m *Model) queueRescan(node *model.TreeNode, changed *model.ChangedPaths) tea.Cmd {
	if !m.scanning() {
		return m.rescanNode(node, changed)
	}
	if m.pendingRescan != nil {
		node, changed = m.scanner.Tree(), nil
	}
	m.pendingRescan = &rescanReq{node: node, changed: changed}
	return nil
}

func (m *Model) cancelOps() {
	for _, op := range []*scanOp{m.scan, m.cksum} {
		if op != nil {
			op.cancel()
		}
	}
}

func (m *Model) startScan() tea.Cmd {
	opts, scanner := *m.cmpOpts, m.scanner
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("dir scan", "/", func() { scanner.Scan(ctx, opts) })
	})
}

func (m *Model) checksumNode(node *model.TreeNode) tea.Cmd {
	scanner := m.scanner
	return m.startOp(opChecksum, func(ctx context.Context) { scanner.ChecksumNode(ctx, node) })
}

func (m *Model) rescanNode(node *model.TreeNode, changed *model.ChangedPaths) tea.Cmd {
	opts, scanner, target := *m.cmpOpts, m.scanner, scanTargetLabel(node)
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("rescan", target, func() { scanner.RescanNode(ctx, node, opts, changed) })
	})
}

// rescanWithTopLevel rescans the cursor node and re-lists the root, so new
// top-level entries show up wherever the cursor sits.
func (m *Model) rescanWithTopLevel(node *model.TreeNode) tea.Cmd {
	opts, scanner, target := *m.cmpOpts, m.scanner, scanTargetLabel(node)
	rescanCursor := node != nil && node.RelPath != ""
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("rescan", target, func() {
			if rescanCursor {
				scanner.RescanNode(ctx, node, opts, nil)
			}
			scanner.RefreshTopLevel(ctx, opts)
		})
	})
}

func (m *Model) deepRescanNode(node *model.TreeNode) tea.Cmd {
	opts, scanner, target := *m.cmpOpts, m.scanner, scanTargetLabel(node)
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("deep scan", target, func() { scanner.DeepRescanNode(ctx, node, opts) })
	})
}

func (m *Model) listNode(node *model.TreeNode) tea.Cmd {
	opts, scanner, target := *m.cmpOpts, m.scanner, scanTargetLabel(node)
	return m.startOp(opScan, func(ctx context.Context) {
		timeScan("list", target, func() { scanner.ListNode(ctx, node, opts) })
	})
}

// childArg extends an open argument, URL or local path, by relPath.
func childArg(arg, relPath string) string {
	if transport.IsRemote(arg) {
		return strings.TrimRight(arg, "/") + "/" + relPath
	}
	return filepath.Join(arg, filepath.FromSlash(relPath))
}

func scanTargetLabel(node *model.TreeNode) string {
	if node == nil || node.RelPath == "" {
		return "/"
	}
	return node.RelPath
}

func timeScan(op, target string, fn func()) {
	transport.Log.Add("scan", ">>>", fmt.Sprintf("%s start: %s", op, target))
	t0 := time.Now()
	fn()
	transport.Log.Add("scan", "<<<", fmt.Sprintf("%s done:  %s (%s)", op, target, time.Since(t0).Round(time.Millisecond)))
}

// readTree and mutateTree fence the UI goroutine's tree access against the
// scanner's: readTree for walks and rendering, mutateTree for the edits the UI
// makes itself (expand/collapse, side swap). Neither is reentrant — nothing
// inside fn may call back into a Scanner method that locks, refreshTree
// included.
func (m *Model) readTree(fn func(root *model.TreeNode)) { m.scanner.ReadTree(fn) }

func (m *Model) mutateTree(fn func(root *model.TreeNode)) { m.scanner.MutateTree(fn) }

// refreshTree rebuilds the visible rows. The rollup walk behind them is
// O(whole tree) — at a million nodes it costs tens of milliseconds — so it runs
// only when the scanner has actually changed something, and never more often
// than four times its own cost. That caps it at a quarter of the frame budget
// no matter how big the tree gets; the flatten itself only touches expanded
// nodes and stays cheap.
func (m *Model) refreshTree() {
	rev := m.scanner.Rev()
	m.scanner.ReadTree(func(tree *model.TreeNode) {
		if tree == nil {
			m.cachedStats = nil
			return
		}
		if rev != m.statsRev && time.Since(m.lastPropagate) >= m.propagateEvery {
			t0 := time.Now()
			s := model.PropagateStatus(tree, m.cmpOpts)
			m.lastPropagate = time.Now()
			m.propagateEvery = min(4*m.lastPropagate.Sub(t0), 2*time.Second)
			m.statsRev = rev
			m.cachedStats = &s
		}
		flat := model.FlattenTree(tree, m.cmpOpts, m.lastFlatLen)
		m.lastFlatLen = len(flat)
		m.leftPanel.SetNodes(flat)
		m.rightPanel.SetNodes(flat)
	})
}

// refreshTreeNow bypasses the throttle. Used when an operation finishes or the
// user acts on the tree: no further tick may be coming, so a skipped rollup
// would leave stale numbers on screen until the next keypress.
func (m *Model) refreshTreeNow() {
	m.propagateEvery = 0
	m.refreshTree()
}

func (m *Model) swapSides() {
	m.leftPanel, m.rightPanel = m.rightPanel, m.leftPanel
	m.leftPanel.isLeft = true
	m.rightPanel.isLeft = false
	m.leftPanel.active = m.activeLeft
	m.rightPanel.active = !m.activeLeft

	m.left, m.right = m.right, m.left

	m.scanner.SwapSides()

	swapped := false
	m.mutateTree(func(tree *model.TreeNode) {
		if tree == nil {
			return
		}
		swapTreeData(tree)
		swapped = true
	})
	if swapped {
		m.refreshTreeNow()
	}
}

func swapTreeData(node *model.TreeNode) {
	node.Left, node.Right = node.Right, node.Left
	node.LeftChecksum, node.RightChecksum = node.RightChecksum, node.LeftChecksum
	node.LeftCksumSize, node.RightCksumSize = node.RightCksumSize, node.LeftCksumSize
	node.LeftCksumModTime, node.RightCksumModTime = node.RightCksumModTime, node.LeftCksumModTime
	node.LeftTotalSize, node.RightTotalSize = node.RightTotalSize, node.LeftTotalSize
	node.LeftTotalFiles, node.RightTotalFiles = node.RightTotalFiles, node.LeftTotalFiles
	node.LeftTotalDirs, node.RightTotalDirs = node.RightTotalDirs, node.LeftTotalDirs
	node.AttrLeftVal, node.AttrRightVal = node.AttrRightVal, node.AttrLeftVal
	node.AttrLeftRaw, node.AttrRightRaw = node.AttrRightRaw, node.AttrLeftRaw
	node.AttrWinner = -node.AttrWinner

	switch node.Compare.Presence {
	case model.PresenceLeftOnly:
		node.Compare.Presence = model.PresenceRightOnly
	case model.PresenceRightOnly:
		node.Compare.Presence = model.PresenceLeftOnly
	}
	switch node.AttrPresence {
	case model.PresenceLeftOnly:
		node.AttrPresence = model.PresenceRightOnly
	case model.PresenceRightOnly:
		node.AttrPresence = model.PresenceLeftOnly
	}

	for _, child := range node.Children {
		swapTreeData(child)
	}
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
		if start := m.copyProgress.Start.Load(); start > 0 {
			info.Elapsed = time.Since(time.Unix(0, start))
		}
	case m.deleting:
		info.State = "DELETE"
		info.FilesDone = m.deleteProgress.Done.Load()
		info.FilesTotal = m.deleteProgress.Total.Load()
		if start := m.deleteProgress.Start.Load(); start > 0 {
			info.Elapsed = time.Since(time.Unix(0, start))
		}
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

func (m *Model) layoutPanels() {
	panelWidth := m.width / 2
	panelHeight := m.height - 2
	m.leftPanel.width = panelWidth
	m.leftPanel.height = panelHeight
	m.rightPanel.width = m.width - panelWidth
	m.rightPanel.height = panelHeight
	m.leftPanel.active = m.activeLeft
	m.rightPanel.active = !m.activeLeft
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
	operation := ""
	if m.deleting {
		operation = spinner + " deleting..."
	} else if m.checksumming() {
		operation = spinner + " checksumming..."
	}

	stats := m.cachedStats
	leftPrefix := ""
	rightPrefix := ""
	if operation != "" {
		leftPrefix = operation
		rightPrefix = operation
	}
	leftTopBar := RenderPanelTopBar(stats, true, leftPrefix, m.leftPanel.width)
	rightTopBar := RenderPanelTopBar(stats, false, rightPrefix, m.rightPanel.width)
	topBar := leftTopBar + rightTopBar
	statusInfo := m.buildStatus(progress)
	statusInfo.Spinner = spinner
	bottomBar := RenderStatusBar(statusInfo, m.width)

	if m.deleting || m.copying || m.checksumming() {
		m.leftPanel.spinner = spinner
		m.rightPanel.spinner = spinner
	} else {
		m.leftPanel.spinner = ""
		m.rightPanel.spinner = ""
		if progress.LeftActive {
			m.leftPanel.spinner = spinner
		}
		if progress.RightActive {
			m.rightPanel.spinner = spinner
		}
	}
	var left, right string
	m.scanner.ReadTree(func(*model.TreeNode) {
		left = m.leftPanel.View()
		right = m.rightPanel.View()
	})
	panels := lipgloss.JoinHorizontal(lipgloss.Top, left, right)

	screen := lipgloss.JoinVertical(lipgloss.Left, topBar, panels, bottomBar)

	if m.copying {
		popupW := 60
		if max := m.width - 4; popupW > max {
			popupW = max
		}
		if popupW < 24 {
			popupW = 24
		}
		file, _ := m.copyProgress.File.Load().(string)
		batched := m.copyProgress.Batched.Load()
		bytes := m.copyProgress.Bytes.Load()
		baseBytes := m.copyProgress.BaseBytes.Load()
		fileSize := m.copyProgress.FileSize.Load()
		fileBytes := bytes - m.copyProgress.FileStartBytes.Load()
		fileBaseBytes := baseBytes - m.copyProgress.FileStartBaseBytes.Load()
		var elapsed time.Duration
		if start := m.copyProgress.Start.Load(); start > 0 {
			elapsed = time.Since(time.Unix(0, start))
		}
		var fileElapsed time.Duration
		if fStart := m.copyProgress.FileStart.Load(); fStart > 0 {
			fileElapsed = time.Since(time.Unix(0, fStart))
		}
		now := time.Now().UnixNano()
		var slotViews []CopySlotView
		if !batched {
			for _, s := range m.copyProgress.SnapshotSlots() {
				var e time.Duration
				if s.Start > 0 {
					e = time.Duration(now - s.Start)
				}
				slotViews = append(slotViews, CopySlotView{
					File:      s.File,
					Size:      s.Size,
					Bytes:     s.Bytes,
					BaseBytes: s.BaseBytes,
					Elapsed:   e,
				})
			}
		}
		popup := RenderCopyPopup(CopyPopupData{
			LeftToRight:        m.copyProgress.LeftToRight.Load(),
			Listing:            m.copyProgress.Listing.Load(),
			DoneFiles:          m.copyProgress.Done.Load(),
			FailedFiles:        m.copyProgress.Failed.Load(),
			TotalFiles:         m.copyProgress.Total.Load(),
			InFlight:           m.copyProgress.InFlight.Load(),
			Parallel:           m.copyProgress.Parallel.Load(),
			Batched:            batched,
			BatchFile:          file,
			BatchFileBytes:     fileBytes,
			BatchFileSize:      fileSize,
			BatchFileBaseBytes: fileBaseBytes,
			BatchFileElapsed:   fileElapsed,
			Slots:              slotViews,
			BytesCopied:        bytes,
			TotalBytes:         m.copyProgress.TotalBytes.Load(),
			BaseBytes:          baseBytes,
			TotalElapsed:       elapsed,
			Spinner:            spinnerFrames[m.copySpinFrame],
		}, popupW)
		px := (m.width - lipgloss.Width(strings.Split(popup, "\n")[0])) / 2
		py := (m.height - strings.Count(popup, "\n") - 1) / 2
		if px < 0 {
			px = 0
		}
		if py < 0 {
			py = 0
		}
		screen = overlayString(screen, popup, px, py)
	}

	if m.deleting {
		popupW := 60
		if max := m.width - 4; popupW > max {
			popupW = max
		}
		if popupW < 24 {
			popupW = 24
		}
		file, _ := m.deleteProgress.File.Load().(string)
		side, _ := m.deleteProgress.Side.Load().(string)
		done := m.deleteProgress.Done.Load()
		total := m.deleteProgress.Total.Load()
		enumerating := m.deleteProgress.Enumerating.Load()
		var elapsed time.Duration
		if start := m.deleteProgress.Start.Load(); start > 0 {
			elapsed = time.Since(time.Unix(0, start))
		}
		popup := RenderDeletePopup(file, side, done, total, enumerating, elapsed, popupW)
		px := (m.width - lipgloss.Width(strings.Split(popup, "\n")[0])) / 2
		py := (m.height - strings.Count(popup, "\n") - 1) / 2
		if px < 0 {
			px = 0
		}
		if py < 0 {
			py = 0
		}
		screen = overlayString(screen, popup, px, py)
	}

	if m.diffView.IsOpen() {
		return m.diffView.View(m.width, m.height)
	}
	if m.logView.IsOpen() {
		return m.logView.View(m.width, m.height, spinner)
	}
	if m.info.IsOpen() {
		return m.info.View(m.width, m.height)
	}
	if m.help.IsOpen() {
		return m.help.View(m.width, m.height)
	}
	if m.confirm.IsOpen() {
		return m.confirm.View(m.width, m.height)
	}
	if m.settings.IsOpen() {
		return m.settings.View(m.width, m.height)
	}
	if m.openDlg.IsOpen() {
		return m.openDlg.View(m.width, m.height)
	}
	if m.input.IsOpen() {
		return m.input.View(m.width, m.height)
	}

	return screen
}

// Size forwards the source length through the copy reader chain so
// destinations that must declare it up front (model.Sized) still see it.
func (t *trackedReader) Size() int64 { return sizeOf(t.r) }

func (c *cancelReader) Size() int64 { return sizeOf(c.r) }

func sizeOf(r io.Reader) int64 {
	if s, ok := r.(model.Sized); ok {
		return s.Size()
	}
	return -1
}

// sizedReader carries the known source length alongside the stream. Only the
// scanned entry knows it — Backend.Open returns a plain io.ReadCloser.
type sizedReader struct {
	io.Reader
	size int64
}

func (s sizedReader) Size() int64 { return s.size }
