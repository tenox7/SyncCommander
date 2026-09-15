package ui

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"sc/model"
	"sc/transfer"
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
	gen     uint64
	content *diffContent
	err     error
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

type DeleteProgress struct {
	Total  atomic.Int64
	Done   atomic.Int64
	File   atomic.Value
	Side   atomic.Value
	Start  atomic.Int64
	Cancel atomic.Pointer[cancelFn]
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
	copyProgress   *transfer.Progress
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
	diffGen        uint64             // identity of the load the diff view is waiting for
	diffCancel     context.CancelFunc // aborts that load when the view closes
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
		m.leftPanel.clampOffset()
		m.rightPanel.clampOffset()
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
			(*c)()
		}
		m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		m.refreshTreeNow()
		if msg.rescanRoot != nil {
			return m, m.queueRescan(msg.rescanRoot, msg.changed)
		}
		return m, nil
	case diffLoadDoneMsg:
		if msg.gen != m.diffGen || !m.diffView.IsOpen() {
			return m, nil
		}
		if msg.err != nil {
			m.diffView.SetError(msg.err.Error())
		} else {
			m.diffView.LoadContent(msg.content)
		}
		return m, nil
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "ctrl+c" {
		if c := m.copyProgress.Cancel.Swap(nil); c != nil {
			(*c)()
		}
		if c := m.deleteProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		m.cancelOps()
		return m, tea.Quit
	}
	if d := m.openModal(); d != nil {
		return m, m.handleModalKey(d, msg)
	}
	if m.copying {
		switch msg.String() {
		case "x", "X":
			if c := m.copyProgress.Cancel.Load(); c != nil {
				transport.Log.Add("copy", "<<<", "user canceled transfer")
				(*c)()
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
		case "x", "X":
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
		return m, m.startCopy(true)
	case "<":
		return m, m.startCopy(false)
	case "=":
		m.settings.Open()
	case "S", "s":
		if !m.busy() {
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
		m.refreshTreeNow()
		if m.cachedStats != nil {
			m.info.Open(*m.cachedStats, "L: "+m.left.BasePath(), "R: "+m.right.BasePath(), algo, cl, cr)
		}
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
		return m, m.reopenOrExplain(leftPath, rightPath)
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
		return m, m.reopenOrExplain(leftPath, rightPath)
	}
	return m, nil
}

// startCopy runs the cursor node in the given direction, asking first when the
// copy would destroy anything on the destination.
func (m *Model) startCopy(leftToRight bool) tea.Cmd {
	node := m.activePanel().CursorNode()
	blocked := model.PresenceRightOnly
	title := "\u26a0 COPY LEFT \u2192 RIGHT"
	if !leftToRight {
		blocked, title = model.PresenceLeftOnly, "\u26a0 COPY RIGHT \u2192 LEFT"
	}
	if m.copying || node == nil || node.IsAttr || m.presence(node) == blocked {
		return nil
	}
	if lines, ok := m.copyConfirmLines(node, leftToRight); ok {
		m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: leftToRight}
		m.confirm.Open(title, lines, true)
		return nil
	}
	m.copying = true
	return tea.Batch(m.copyNode(node, leftToRight, false), m.ensureTick())
}

func (m *Model) handleDiffViewKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc", "q":
		m.closeDiff()
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
	return nil
}

// diffMaxBytes caps what the diff view pulls into memory per side.
const diffMaxBytes = 8 << 20

// loadDiffContent reads both sides and builds the comparison off the UI
// goroutine. The load carries a generation so a slow earlier load cannot
// overwrite a newer one, and closing the view cancels it.
func (m *Model) loadDiffContent(node *model.TreeNode) tea.Cmd {
	m.closeDiffLoad()
	ctx, cancel := context.WithCancel(context.Background())
	m.diffCancel = cancel
	m.diffGen++
	gen := m.diffGen
	left, right, relPath := m.left, m.right, node.RelPath
	hasLeft, hasRight := node.Left != nil, node.Right != nil
	return func() tea.Msg {
		var leftData, rightData []byte
		var err error
		if hasLeft {
			leftData, err = readCapped(ctx, left, "left", relPath)
		}
		if err == nil && hasRight {
			rightData, err = readCapped(ctx, right, "right", relPath)
		}
		if err != nil {
			return diffLoadDoneMsg{gen: gen, err: err}
		}
		return diffLoadDoneMsg{gen: gen, content: buildDiffContent(leftData, rightData)}
	}
}

func (m *Model) closeDiffLoad() {
	if m.diffCancel != nil {
		m.diffCancel()
		m.diffCancel = nil
	}
}

func (m *Model) closeDiff() {
	m.closeDiffLoad()
	m.diffView.Close()
}

// readCapped reads a whole file for the diff view, refusing anything past
// diffMaxBytes rather than pulling gigabytes into memory.
func readCapped(ctx context.Context, backend model.Backend, side, relPath string) ([]byte, error) {
	rc, err := backend.Open(ctx, relPath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", side, err)
	}
	defer rc.Close()
	defer transport.CancelCloser(ctx, rc)()
	data, err := io.ReadAll(io.LimitReader(rc, diffMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", side, err)
	}
	if len(data) > diffMaxBytes {
		return nil, fmt.Errorf("%s: larger than %s, too big for the diff view", side, model.FormatSize(diffMaxBytes))
	}
	return data, nil
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

func (m *Model) handleSettingsKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc", "s", "q":
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
	return nil
}

func (m *Model) handleInputKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.input.Close()
	case "enter":
		return m.input.Confirm()
	default:
		m.input.HandleKey(msg)
	}
	return nil
}

// reopenOrExplain reopens both sides or, when one cannot be opened, shows the
// attempted locations in the URL dialog with the error, so a failed parent or
// descend key is never silent.
func (m *Model) reopenOrExplain(leftPath, rightPath string) tea.Cmd {
	cmd, errMsg := m.reopenBackends(leftPath, rightPath)
	if errMsg == "" {
		return cmd
	}
	m.openDlg.Open(transport.MaskURLPassword(leftPath), transport.MaskURLPassword(rightPath))
	m.openDlg.SetError(errMsg)
	return nil
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

func (m *Model) handleOpenDlgKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.openDlg.Close()
	case "enter":
		leftPath, rightPath := m.openDlg.Values()
		if leftPath == "" || rightPath == "" {
			m.openDlg.SetError("both paths are required")
			return nil
		}
		cmd, errMsg := m.reopenBackends(leftPath, rightPath)
		if errMsg != "" {
			m.openDlg.SetError(errMsg)
			return nil
		}
		m.openDlg.Close()
		return cmd
	default:
		m.openDlg.HandleKey(msg)
	}
	return nil
}

func (m *Model) handleConfirmKey(msg tea.KeyMsg) tea.Cmd {
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
		case "esc", "q":
			m.confirm.Close()
			m.pendingDelete = nil
			return nil
		default:
			found = false
		}
		if found {
			m.confirm.Close()
			node := m.pendingDelete
			m.pendingDelete = nil
			m.deleting = true
			return tea.Batch(m.deleteNode(node, side), m.ensureTick())
		}
		return nil
	}

	switch msg.String() {
	case "y", "Y":
		m.confirm.Close()
		if m.pendingDelete != nil {
			node := m.pendingDelete
			m.pendingDelete = nil
			m.deleting = true
			return tea.Batch(m.deleteNode(node, m.presence(node)), m.ensureTick())
		}
		if m.pendingCopy != nil {
			pc := m.pendingCopy
			m.pendingCopy = nil
			m.copying = true
			return tea.Batch(m.copyNode(pc.node, pc.leftToRight, true), m.ensureTick())
		}
	case "esc", "n", "N", "q":
		m.confirm.Close()
		m.pendingDelete = nil
		m.pendingCopy = nil
	}
	return nil
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
		m.confirm.Open("\u26a0 RECURSIVE DELETE", []string{"", node.Name + "/", describeSubtree(node), sides}, true)
		return
	}

	if !node.IsDir {
		m.confirm.OpenChoice("Delete "+node.Name+"?", []string{""}, false)
		return
	}
	m.confirm.OpenChoice("\u26a0 RECURSIVE DELETE", []string{"", node.Name + "/", describeSubtree(node)}, true)
}

// describeSubtree summarises what a recursive delete would remove.
func describeSubtree(node *model.TreeNode) string {
	files, dirs, complete := model.CountDescendants(node)
	switch {
	case !complete:
		return fmt.Sprintf("%d+ files, %d+ folders (not fully scanned)", files, dirs)
	case files == 0 && dirs == 0:
		return "empty folder"
	}
	return fmt.Sprintf("%d files, %d folders", files, dirs)
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

// copyNode hands the transfer engine a snapshot of what it needs and reports
// back with the rescan root and the changed paths.
func (m *Model) copyNode(node *model.TreeNode, leftToRight, mirror bool) tea.Cmd {
	var relPath string
	m.readTree(func(*model.TreeNode) { relPath = node.RelPath })
	src, dst := m.left, m.right
	if !leftToRight {
		src, dst = m.right, m.left
	}
	parallel := max(m.copyParallel, 1)
	req := transfer.Request{
		Src: src, Dst: dst, Scanner: m.scanner, Node: node, RelPath: relPath,
		LeftToRight: leftToRight, Mirror: mirror, Opts: *m.cmpOpts,
		Parallel: parallel, ParallelMax: max(m.parallelMax, parallel),
		Batch: m.batchTransfer, VerifyResume: m.verifyResume, Progress: m.copyProgress,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = transport.ContextWithFatalCancel(ctx, cancel)
	m.copyProgress.Reset(parallel, req.ParallelMax, leftToRight)
	m.copyProgress.Cancel.Store(&cancel)
	return func() tea.Msg {
		res := transfer.Copy(ctx, req)
		return copyDoneMsg{rescanRoot: res.RescanRoot, changed: res.Changed}
	}
}

func (m *Model) openRename(node *model.TreeNode) {
	var oldName string
	m.readTree(func(*model.TreeNode) { oldName = node.Name })
	m.input.Open("Rename: "+oldName, oldName, func(newName string) tea.Cmd {
		if newName == "" || newName == oldName {
			return nil
		}
		if strings.ContainsAny(newName, "/\\") {
			transport.Log.Add("rename", "ERR", oldName+": a name cannot contain a path separator")
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
		left, right, scanner := m.left, m.right, m.scanner
		return func() tea.Msg {
			ctx := context.Background()
			var err error
			switch presence {
			case model.PresenceLeftOnly:
				err = left.Rename(ctx, oldRel, newRel)
			case model.PresenceRightOnly:
				err = right.Rename(ctx, oldRel, newRel)
			default:
				err = left.Rename(ctx, oldRel, newRel)
				if rerr := right.Rename(ctx, oldRel, newRel); err == nil {
					err = rerr
				}
			}
			if err != nil {
				transport.Log.Add("rename", "ERR", oldRel+" -> "+newRel+": "+err.Error())
				// One side may have been renamed: re-list the parent so the
				// tree shows what is really there instead of the old row.
				parent := model.DirOf(oldRel)
				if le, re, lerr := scanner.ListBothDir(ctx, parent); lerr == nil {
					scanner.RefreshDir(parent, le, re, opts)
				}
				return renameDoneMsg{err: err}
			}
			if scanner.RenameNode(node, newName, newRel, oldRel, opts) {
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
	node.LeftChecksumDone, node.RightChecksumDone = node.RightChecksumDone, node.LeftChecksumDone
	node.LeftChecksumErr, node.RightChecksumErr = node.RightChecksumErr, node.LeftChecksumErr
	node.ChecksumPendingLeft, node.ChecksumPendingRight = node.ChecksumPendingRight, node.ChecksumPendingLeft
	node.ChecksumActiveLeft, node.ChecksumActiveRight = node.ChecksumActiveRight, node.ChecksumActiveLeft
	node.ChecksumInFlightLeft, node.ChecksumInFlightRight = node.ChecksumInFlightRight, node.ChecksumInFlightLeft
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

	if d := m.openModal(); d != nil {
		m.logView.spinner = spinner
		return d.View(m.width, m.height)
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
		popupW := popupWidth(m.width)
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
		screen = overlayCentered(screen, popup, m.width, m.height)
	}

	if m.deleting {
		file, _ := m.deleteProgress.File.Load().(string)
		side, _ := m.deleteProgress.Side.Load().(string)
		var elapsed time.Duration
		if start := m.deleteProgress.Start.Load(); start > 0 {
			elapsed = time.Since(time.Unix(0, start))
		}
		popup := RenderDeletePopup(file, side, m.deleteProgress.Done.Load(), m.deleteProgress.Total.Load(), elapsed, popupWidth(m.width))
		screen = overlayCentered(screen, popup, m.width, m.height)
	}

	return screen
}

// modal is a dialog that owns the screen and the keyboard while open.
type modal interface {
	IsOpen() bool
	View(width, height int) string
}

// openModal returns the open dialog, if any, in one fixed priority order
// shared by key routing and rendering.
func (m *Model) openModal() modal {
	for _, d := range []modal{m.diffView, m.logView, m.info, m.help, m.confirm, m.input, m.openDlg, m.settings} {
		if d.IsOpen() {
			return d
		}
	}
	return nil
}

func (m *Model) handleModalKey(d modal, msg tea.KeyMsg) tea.Cmd {
	switch d := d.(type) {
	case *DiffView:
		return m.handleDiffViewKey(msg)
	case *LogDialog:
		handleLogKey(d, msg)
	case *InfoDialog:
		if k := msg.String(); k == "esc" || k == "q" || k == "i" {
			d.Close()
		}
	case *HelpDialog:
		if k := msg.String(); k == "esc" || k == "q" || k == "?" {
			d.Close()
		}
	case *ConfirmDialog:
		return m.handleConfirmKey(msg)
	case *InputDialog:
		return m.handleInputKey(msg)
	case *OpenDialog:
		return m.handleOpenDlgKey(msg)
	case *SettingsDialog:
		return m.handleSettingsKey(msg)
	}
	return nil
}

func handleLogKey(d *LogDialog, msg tea.KeyMsg) {
	switch msg.String() {
	case "esc", "q", "~", "`":
		d.Close()
	case "up", "k":
		d.ScrollUp()
	case "down", "j":
		d.ScrollDown()
	case "pgup":
		d.PageUp()
	case "pgdown":
		d.PageDown()
	case "home":
		d.Home()
	case "end":
		d.End()
	case "e":
		d.ToggleErrFilter()
	}
}
