package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
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
type scanDoneMsg struct{}
type rescanDoneMsg struct{}
type checksumDoneMsg struct{}
type touchDoneMsg struct{}
type diffLoadDoneMsg struct {
	left, right []byte
	err         error
}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type renameDoneMsg struct{ err error }
type deleteDoneMsg struct{}
type copyDoneMsg struct {
	rescanRoot *model.TreeNode
	changed    *model.ChangedPaths
}

type cancelFn struct{ f context.CancelFunc }

type CopyProgress struct {
	Total              atomic.Int64
	Done               atomic.Int64
	InFlight           atomic.Int64
	Parallel           atomic.Int64
	Bytes              atomic.Int64
	BaseBytes          atomic.Int64
	TotalBytes         atomic.Int64
	Start              atomic.Int64
	File               atomic.Value
	FileSize           atomic.Int64
	FileStart          atomic.Int64
	FileStartBytes     atomic.Int64
	FileStartBaseBytes atomic.Int64
	LeftToRight        atomic.Bool
	Cancel             atomic.Pointer[cancelFn]
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
	scanner        *model.Scanner
	activeLeft     bool
	scanning       bool
	deleting       bool
	copying        bool
	checksumming   bool
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
	tickActive     bool
	cachedStats    *TreeStats
}

func NewModel(left, right model.Backend, cmpOpts *model.CompareOpts, insecure, deepScan bool, copyParallel int) Model {
	if copyParallel < 1 {
		copyParallel = 1
	}
	lp := NewPanel(left.BasePath())
	lp.isLeft = true
	rp := NewPanel(right.BasePath())
	m := Model{
		leftPanel:      lp,
		rightPanel:     rp,
		left:           left,
		right:          right,
		scanner:        model.NewScanner(left, right, 4, deepScan),
		activeLeft:     true,
		cmpOpts:        cmpOpts,
		deepScan:       deepScan,
		copyParallel:   copyParallel,
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
		scanning:       true,
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
	})
	return m
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(tea.EnterAltScreen, m.tickCmd(), m.startScan())
}

func (m Model) tickCmd() tea.Cmd {
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

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.layoutPanels()
		return m, nil
	case tickMsg:
		if !m.scanning && !m.copying && !m.deleting && !m.checksumming {
			m.tickActive = false
			return m, nil
		}
		m.spinFrame = (m.spinFrame + 1) % len(spinnerFrames)
		if m.copying {
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
	case scanDoneMsg:
		m.scanning = false
		m.refreshTree()
		return m, nil
	case rescanDoneMsg:
		m.scanning = false
		m.refreshTree()
		return m, nil
	case checksumDoneMsg:
		m.checksumming = false
		m.refreshTree()
		return m, nil
	case renameDoneMsg:
		m.refreshTree()
		return m, nil
	case touchDoneMsg:
		m.refreshTree()
		return m, nil
	case deleteDoneMsg:
		m.deleting = false
		if c := m.deleteProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		m.refreshTree()
		return m, nil
	case copyDoneMsg:
		m.copying = false
		if c := m.copyProgress.Cancel.Swap(nil); c != nil {
			c.f()
		}
		m.logView.AutoOpen(transport.Log.ErrCount(), transport.Log.FatalCount())
		m.refreshTree()
		if msg.rescanRoot != nil {
			m.scanning = true
			return m, tea.Batch(m.rescanNode(msg.rescanRoot, msg.changed), m.ensureTick())
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
		m.scanner.Cancel()
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
	case "q", "ctrl+c":
		m.scanner.Cancel()
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
		if node != nil && node.IsDir && !node.Listed && !m.scanning {
			m.scanning = true
			node.Expanded = true
			return m, tea.Batch(m.listNode(node), m.ensureTick())
		}
		m.activePanel().Toggle()
		m.refreshTree()
	case "left", "h":
		node := m.activePanel().CursorNode()
		if node != nil && !node.IsAttr && node.Expanded {
			node.Expanded = false
			m.refreshTree()
		} else if node != nil {
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
					m.refreshTree()
					break
				}
			}
		}
	case "n":
		if !m.scanning && !m.copying && !m.deleting && !m.checksumming {
			m.jumpToNextDiff()
		}
	case "/":
		m.openSearch()
	case "}":
		tree := m.scanner.Tree()
		if tree != nil {
			model.SetExpandedAll(tree, true)
			m.refreshTree()
		}
	case "{":
		tree := m.scanner.Tree()
		if tree != nil {
			model.SetExpandedAll(tree, false)
			tree.Expanded = true
			m.refreshTree()
		}
	case "r":
		node := m.activePanel().CursorNode()
		if node != nil && node.IsAttr {
			break
		}
		m.scanning = true
		return m, tea.Batch(m.rescanWithTopLevel(node), m.ensureTick())
	case "R":
		tree := m.scanner.Tree()
		if tree == nil {
			break
		}
		m.scanning = true
		return m, tea.Batch(m.deepRescanNode(tree), m.ensureTick())
	case "c":
		if m.checksumming {
			break
		}
		node := m.activePanel().CursorNode()
		if node != nil && node.IsAttr {
			node = m.parentFileNode()
		}
		if node != nil {
			m.checksumming = true
			return m, tea.Batch(m.checksumNode(node), m.ensureTick())
		}
	case "e":
		node := m.activePanel().CursorNode()
		if node != nil && !node.IsAttr {
			m.openRename(node)
		}
	case "t":
		node := m.activePanel().CursorNode()
		if node != nil && !node.IsAttr && node.Compare.Presence == model.PresenceBoth {
			return m, m.touchNode(node)
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
		if node == nil || node.IsAttr || node.Compare.Presence == model.PresenceRightOnly {
			break
		}
		collisions := len(model.CollectTypeCollisions(node, true))
		var delFiles, delDirs int
		if node.IsDir {
			delFiles, delDirs = model.CountMirrorDeletes(node, true)
		}
		if collisions > 0 || delFiles > 0 || delDirs > 0 {
			lines := []string{"", node.Name}
			if node.IsDir {
				lines[1] += "/"
			}
			if collisions > 0 {
				lines = append(lines, fmt.Sprintf("Will replace %d type conflicts (file\u2194dir) on right", collisions))
			}
			if delFiles > 0 || delDirs > 0 {
				lines = append(lines, fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on right")
			}
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
		if node == nil || node.IsAttr || node.Compare.Presence == model.PresenceLeftOnly {
			break
		}
		collisions := len(model.CollectTypeCollisions(node, false))
		var delFiles, delDirs int
		if node.IsDir {
			delFiles, delDirs = model.CountMirrorDeletes(node, false)
		}
		if collisions > 0 || delFiles > 0 || delDirs > 0 {
			lines := []string{"", node.Name}
			if node.IsDir {
				lines[1] += "/"
			}
			if collisions > 0 {
				lines = append(lines, fmt.Sprintf("Will replace %d type conflicts (file\u2194dir) on left", collisions))
			}
			if delFiles > 0 || delDirs > 0 {
				lines = append(lines, fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on left")
			}
			m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: false}
			m.confirm.Open("\u26a0 COPY RIGHT \u2192 LEFT", lines, true)
			break
		}
		m.copying = true
		return m, tea.Batch(m.copyNode(node, false, false), m.ensureTick())
	case "=":
		m.settings.Open()
	case "S", "s":
		if !m.scanning && !m.copying && !m.deleting {
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
		tree := m.scanner.Tree()
		if tree != nil {
			cl, cr := m.scanner.ChecksumInfo()
			m.info.Open(computeTreeStats(tree), "L: "+m.left.BasePath(), "R: "+m.right.BasePath(), m.scanner.ChecksumAlgo(), cl, cr)
		}
	case "y":
		if m.copying || m.deleting {
			break
		}
		m.openDlg.Open(m.left.BasePath(), m.right.BasePath())
	case "u":
		if m.copying || m.deleting {
			break
		}
		leftPath := transport.ParentPath(m.left.BasePath())
		rightPath := transport.ParentPath(m.right.BasePath())
		if leftPath == m.left.BasePath() && rightPath == m.right.BasePath() {
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
			m.diffView.Open(node.Name)
			return m, m.loadDiffContent(node)
		}
	case "b":
		if m.copying || m.deleting {
			break
		}
		node := m.activePanel().CursorNode()
		if node == nil || !node.IsDir || node.IsAttr {
			break
		}
		if node.Left == nil && node.Right == nil {
			break
		}
		leftPath := m.left.BasePath()
		rightPath := m.right.BasePath()
		if node.Left != nil {
			leftPath = leftPath + "/" + node.RelPath
		}
		if node.Right != nil {
			rightPath = rightPath + "/" + node.RelPath
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

func (m *Model) handleSettingsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c", "s", "q":
		m.settings.Close()
	case "up", "k":
		m.settings.MoveUp()
	case "down", "j":
		m.settings.MoveDown()
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
		m.input.Confirm()
	default:
		m.input.HandleKey(msg)
	}
	return m, nil
}

func (m *Model) reopenBackends(leftPath, rightPath string) (tea.Cmd, string) {
	var newLeft, newRight model.Backend
	var err error

	if leftPath != m.left.BasePath() {
		newLeft, err = transport.TryOpenBackend(leftPath, m.insecure)
		if err != nil {
			return nil, "left: " + err.Error()
		}
	}
	if rightPath != m.right.BasePath() {
		newRight, err = transport.TryOpenBackend(rightPath, m.insecure)
		if err != nil {
			if newLeft != nil {
				transport.CloseBackend(newLeft)
			}
			return nil, "right: " + err.Error()
		}
	}

	m.scanner.Cancel()

	if newLeft != nil {
		oldLeft := m.left
		go func() { transport.CloseBackend(oldLeft) }()
		m.left = newLeft
	}
	if newRight != nil {
		oldRight := m.right
		go func() { transport.CloseBackend(oldRight) }()
		m.right = newRight
	}

	m.scanner = model.NewScanner(m.left, m.right, 4, m.deepScan)
	m.leftPanel.title = m.left.BasePath()
	m.rightPanel.title = m.right.BasePath()
	m.leftPanel.SetNodes(nil)
	m.rightPanel.SetNodes(nil)
	return tea.Batch(m.startScan(), m.ensureTick()), ""
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
			return m, tea.Batch(m.deleteNode(node, node.Compare.Presence), m.ensureTick())
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

func (m *Model) openDelete(node *model.TreeNode) {
	m.pendingDelete = node

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
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	isDir := node.IsDir
	relPath := node.RelPath
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

	files, dirs, _ := model.CountDescendants(node)
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
		le, re := scanner.ListBothDir(refreshCtx, parentDir)
		scanner.RefreshDir(parentDir, le, re, subSecond, timeGrace, ignoreTZDST)
		return deleteDoneMsg{}
	}
}

func removeOne(ctx context.Context, backend model.Backend, relPath string, isDir bool) error {
	if isDir {
		return backend.RemoveAll(ctx, relPath)
	}
	return backend.Remove(ctx, relPath)
}

func (m *Model) copyNode(node *model.TreeNode, leftToRight bool, mirror bool) tea.Cmd {
	left := m.left
	right := m.right
	scanner := m.scanner
	opts := *m.cmpOpts
	progress := m.copyProgress
	parallel := m.copyParallel
	if parallel < 1 {
		parallel = 1
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	baseCtx = transport.ContextWithFatalCancel(baseCtx, cancel)
	progress.Cancel.Store(&cancelFn{f: cancel})
	return func() tea.Msg {
		ctx := transport.ContextWithProgress(baseCtx, &progress.Bytes)
		ctx = transport.ContextWithBaseProgress(ctx, &progress.BaseBytes)

		var dstBackend model.Backend
		if leftToRight {
			dstBackend = right
		} else {
			dstBackend = left
		}
		for _, c := range model.CollectTypeCollisions(node, leftToRight) {
			if ctx.Err() != nil {
				break
			}
			dstEntry := c.Right
			if !leftToRight {
				dstEntry = c.Left
			}
			if dstEntry == nil {
				continue
			}
			var err error
			if dstEntry.IsDir {
				err = dstBackend.RemoveAll(ctx, c.RelPath)
			} else {
				err = dstBackend.Remove(ctx, c.RelPath)
			}
			if err != nil {
				transport.Log.Add("copy", "ERR", "type-collision cleanup "+c.RelPath+": "+err.Error())
			} else {
				transport.Log.Add("copy", "<<<", "type-collision cleanup "+c.RelPath)
			}
		}

		var files []*model.TreeNode
		if node.IsDir {
			files = model.CollectCopyFiles(node, &opts, leftToRight)
		} else {
			files = []*model.TreeNode{node}
		}

		var totalBytes int64
		for _, f := range files {
			entry := f.Left
			if !leftToRight {
				entry = f.Right
			}
			if entry != nil {
				totalBytes += entry.Size
			}
		}
		progress.Total.Store(int64(len(files)))
		progress.TotalBytes.Store(totalBytes)
		progress.Done.Store(0)
		progress.InFlight.Store(0)
		progress.Parallel.Store(int64(parallel))
		progress.Bytes.Store(0)
		progress.BaseBytes.Store(0)
		progress.Start.Store(time.Now().UnixNano())
		progress.LeftToRight.Store(leftToRight)
		progress.File.Store("")
		progress.FileSize.Store(0)
		progress.FileStartBytes.Store(0)
		progress.FileStartBaseBytes.Store(0)
		progress.FileStart.Store(0)

		dstChanged := make(map[string]bool)
		var changedMu sync.Mutex
		markChanged := func(relPath string) {
			changedMu.Lock()
			dstChanged[relPath] = true
			changedMu.Unlock()
		}

		if parallel > 1 {
			transport.Log.Add("copy", ">>>", fmt.Sprintf("parallel=%d", parallel))
		}
		sem := make(chan struct{}, parallel)
		var wg sync.WaitGroup

		copyOne := func(f *model.TreeNode) {
			var src, dst model.Backend
			var srcEntry, dstEntry *model.FileEntry
			if leftToRight {
				src, dst = left, right
				srcEntry, dstEntry = f.Left, f.Right
			} else {
				src, dst = right, left
				srcEntry, dstEntry = f.Right, f.Left
			}
			if srcEntry == nil {
				progress.Done.Add(1)
				return
			}
			if srcEntry.IsDir {
				progress.File.Store(f.RelPath)
				progress.BeginFile(0)
				if err := dst.Mkdir(ctx, f.RelPath, srcEntry.Mode); err != nil {
					transport.Log.Add("copy", "ERR", "mkdir "+f.RelPath+": "+err.Error())
				} else {
					transport.Log.Add("copy", "<<<", "mkdir "+f.RelPath)
				}
				progress.Done.Add(1)
				return
			}
			if dstEntry != nil && dstEntry.IsDir != srcEntry.IsDir {
				var clearErr error
				if dstEntry.IsDir {
					clearErr = dst.RemoveAll(ctx, f.RelPath)
				} else {
					clearErr = dst.Remove(ctx, f.RelPath)
				}
				if clearErr != nil {
					transport.Log.Add("copy", "ERR", "clear dst type-mismatch "+f.RelPath+": "+clearErr.Error())
					progress.Done.Add(1)
					return
				}
				transport.Log.Add("copy", "<<<", "cleared dst type-mismatch "+f.RelPath)
				dstEntry = nil
			}
			progress.File.Store(f.RelPath)
			progress.BeginFile(srcEntry.Size)
			transport.Log.Add("copy", ">>>", fmt.Sprintf("COPY %s (%s)", f.RelPath, model.FormatSize(srcEntry.Size)))
			fileCtx := transport.ContextWithFileSize(ctx, srcEntry.Size)

			// Resume first when a partial dst body exists: append the missing
			// tail rather than overwrite the whole file. tryResumeCopy
			// self-gates (no-op for absent/full/oversized dst). Blind trust;
			// a post-sync CRC pass catches any divergence.
			var resumeOK bool
			_ = transport.WithStallGuard(fileCtx, &progress.Bytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
				resumeOK = tryResumeCopy(attemptCtx, src, dst, f.RelPath, srcEntry, dstEntry, progress)
				return nil
			})
			if resumeOK {
				_ = dst.SetTimes(fileCtx, f.RelPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime)
				markChanged(f.RelPath)
				transport.Log.Add("copy", "<<<", "COPY "+f.RelPath+" OK (resumed)")
				progress.Done.Add(1)
				return
			}

			var directOK bool
			_ = transport.WithStallGuard(fileCtx, &progress.Bytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
				directOK = tryDirectTransfer(attemptCtx, src, dst, f.RelPath, srcEntry)
				return nil
			})
			if directOK {
				_ = dst.SetTimes(fileCtx, f.RelPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime)
				markChanged(f.RelPath)
				transport.Log.Add("copy", "<<<", "COPY "+f.RelPath+" OK")
				progress.Done.Add(1)
				return
			}

			attempt := 0
			err := transport.Retry(fileCtx, "copy", "copy "+f.RelPath, func() error {
				return transport.WithStallGuard(fileCtx, &progress.Bytes, transport.StallTimeout(), func(attemptCtx context.Context) error {
					attempt++
					if attempt > 1 {
						offset := peekDstSize(attemptCtx, dst, f.RelPath)
						if offset > 0 && offset < srcEntry.Size {
							err := resumeAttempt(attemptCtx, src, dst, f.RelPath, srcEntry, offset, progress)
							if err == nil {
								return nil
							}
							if !errors.Is(err, transport.ErrResumeUnsupported) {
								return err
							}
						}
					}
					return fullCopyAttempt(attemptCtx, src, dst, f.RelPath, srcEntry, &progress.Bytes)
				})
			})
			if err == nil {
				_ = dst.SetTimes(fileCtx, f.RelPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime)
				markChanged(f.RelPath)
				transport.Log.Add("copy", "<<<", "COPY "+f.RelPath+" OK")
			} else {
				transport.Log.Add("copy", "ERR", "COPY "+f.RelPath+": "+err.Error())
			}
			progress.Done.Add(1)
		}

	dispatch:
		for _, f := range files {
			select {
			case <-ctx.Done():
				break dispatch
			case sem <- struct{}{}:
			}
			wg.Add(1)
			progress.InFlight.Add(1)
			go func(f *model.TreeNode) {
				defer wg.Done()
				defer func() { <-sem }()
				defer progress.InFlight.Add(-1)
				copyOne(f)
			}(f)
		}
		wg.Wait()

		var rescanRoot *model.TreeNode
		if node.IsDir {
			if mirror {
				deletes := model.CollectMirrorDeletes(node, leftToRight)
				var delBackend model.Backend
				if leftToRight {
					delBackend = right
				} else {
					delBackend = left
				}
				for _, d := range deletes {
					if ctx.Err() != nil {
						break
					}
					if d.IsDir {
						_ = delBackend.RemoveAll(ctx, d.RelPath)
					} else {
						_ = delBackend.Remove(ctx, d.RelPath)
					}
				}
			}
			rescanRoot = node
		} else {
			parentDir := model.DirOf(node.RelPath)
			rescanRoot = scanner.FindNearestDestNode(parentDir, leftToRight)
		}
		changed := &model.ChangedPaths{}
		if leftToRight {
			changed.Right = dstChanged
		} else {
			changed.Left = dstChanged
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
func tryResumeCopy(ctx context.Context, src, dst model.Backend, relPath string, srcEntry, dstEntry *model.FileEntry, progress *CopyProgress) bool {
	if dstEntry == nil || dstEntry.IsDir {
		return false
	}
	offset := dstEntry.Size
	if offset <= 0 || offset >= srcEntry.Size {
		return false
	}
	resumer, ok := dst.(model.Resumer)
	if !ok {
		return false
	}

	progress.Bytes.Add(offset)
	progress.BaseBytes.Add(offset)
	var added atomic.Int64
	added.Store(offset)
	opener := &trackedRangeOpener{
		Backend:  src,
		RelPath:  relPath,
		FileSize: srcEntry.Size,
		Ctx:      ctx,
		Target:   &progress.Bytes,
		Added:    &added,
	}
	if err := resumer.AppendFrom(ctx, relPath, opener, srcEntry.Mode, offset); err != nil {
		progress.Bytes.Add(-added.Load())
		progress.BaseBytes.Add(-offset)
		return false
	}
	return true
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

// resumeAttempt resumes a partial upload by appending bytes from offset onward.
// Returns transport.ErrResumeUnsupported if dst can't append; on any other
// error any progress credited during the attempt is rolled back. The offset
// portion is also tracked in BaseBytes so it doesn't inflate the transfer-rate
// calculation.
func resumeAttempt(ctx context.Context, src, dst model.Backend, relPath string, srcEntry *model.FileEntry, offset int64, progress *CopyProgress) error {
	resumer, ok := dst.(model.Resumer)
	if !ok {
		return transport.ErrResumeUnsupported
	}
	progress.Bytes.Add(offset)
	progress.BaseBytes.Add(offset)
	var added atomic.Int64
	added.Store(offset)
	opener := &trackedRangeOpener{
		Backend:  src,
		RelPath:  relPath,
		FileSize: srcEntry.Size,
		Ctx:      ctx,
		Target:   &progress.Bytes,
		Added:    &added,
	}
	if err := resumer.AppendFrom(ctx, relPath, opener, srcEntry.Mode, offset); err != nil {
		progress.Bytes.Add(-added.Load())
		progress.BaseBytes.Add(-offset)
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
	var srcReader io.Reader = reader
	if !transport.IsPreCounted(reader) && !dstOwnsProgress {
		srcReader = &trackedReader{r: reader, target: counter, added: &added}
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
		if err != nil && !errors.Is(err, transport.ErrResumeUnsupported) {
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
	m.input.Open("Rename: "+node.Name, node.Name, func(newName string) {
		if newName == "" || newName == node.Name {
			return
		}
		oldRel := node.RelPath
		newRel := model.DirOf(oldRel)
		if newRel != "" {
			newRel += "/"
		}
		newRel += newName
		go func() {
			ctx := context.Background()
			switch node.Compare.Presence {
			case model.PresenceLeftOnly:
				_ = m.left.Rename(ctx, oldRel, newRel)
			case model.PresenceRightOnly:
				_ = m.right.Rename(ctx, oldRel, newRel)
			default:
				_ = m.left.Rename(ctx, oldRel, newRel)
				_ = m.right.Rename(ctx, oldRel, newRel)
			}
			m.scanner.RenameNode(node, newName, newRel, oldRel)
		}()
	})
}

func (m *Model) touchNode(node *model.TreeNode) tea.Cmd {
	left := m.left
	right := m.right
	scanner := m.scanner
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	return func() tea.Msg {
		ctx := context.Background()
		l, r := node.Left, node.Right
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
			if touchedLeft {
				node.LeftCksumModTime = newer.ModTime
			} else {
				node.RightCksumModTime = newer.ModTime
			}
		}
		parentDir := model.DirOf(node.RelPath)
		le, re := scanner.ListBothDir(ctx, parentDir)
		scanner.RefreshDir(model.DirOf(node.RelPath), le, re, subSecond, timeGrace, ignoreTZDST)
		return touchDoneMsg{}
	}
}

func (m *Model) openSearch() {
	m.input.Open("Search (regex):", "", func(query string) {
		if query == "" {
			return
		}
		re, err := regexp.Compile(query)
		if err != nil {
			return
		}
		m.findAndJump(re)
	})
}

func (m *Model) findAndJump(re *regexp.Regexp) {
	tree := m.scanner.Tree()
	if tree == nil {
		return
	}
	target := model.FindByName(tree, nil, re)
	if target == nil {
		return
	}
	for n := target.Parent; n != nil; n = n.Parent {
		n.Expanded = true
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
	tree := m.scanner.Tree()
	if tree == nil {
		return
	}
	p := m.activePanel()
	var after *model.TreeNode
	for i := p.cursor; i >= 0 && i < len(p.nodes); i-- {
		if !p.nodes[i].IsAttr {
			after = p.nodes[i]
			break
		}
	}
	target := model.FindNextDiff(tree, after, m.cmpOpts)
	if target == nil {
		return
	}
	for n := target.Parent; n != nil; n = n.Parent {
		n.Expanded = true
	}
	target.Expanded = true
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

func (m *Model) startScan() tea.Cmd {
	m.scanning = true
	checksum := m.cmpOpts.Checksum
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	scanner := m.scanner
	return func() tea.Msg {
		timeScan("dir scan", "/", func() {
			scanner.Scan(context.Background(), checksum, subSecond, timeGrace, ignoreTZDST)
		})
		return scanDoneMsg{}
	}
}

func (m *Model) checksumNode(node *model.TreeNode) tea.Cmd {
	scanner := m.scanner
	return func() tea.Msg {
		scanner.ChecksumNode(context.Background(), node)
		return checksumDoneMsg{}
	}
}

func (m *Model) rescanNode(node *model.TreeNode, changed *model.ChangedPaths) tea.Cmd {
	checksum := m.cmpOpts.Checksum
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	scanner := m.scanner
	target := scanTargetLabel(node)
	return func() tea.Msg {
		timeScan("rescan", target, func() {
			scanner.RescanNode(context.Background(), node, checksum, subSecond, timeGrace, ignoreTZDST, changed)
		})
		return rescanDoneMsg{}
	}
}

func (m *Model) rescanWithTopLevel(node *model.TreeNode) tea.Cmd {
	checksum := m.cmpOpts.Checksum
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	scanner := m.scanner
	target := scanTargetLabel(node)
	rescanCursor := node != nil && node.RelPath != ""
	return func() tea.Msg {
		timeScan("rescan", target, func() {
			if rescanCursor {
				scanner.RescanNode(context.Background(), node, checksum, subSecond, timeGrace, ignoreTZDST, nil)
			}
			scanner.RefreshTopLevel(context.Background(), subSecond, timeGrace, ignoreTZDST)
		})
		return rescanDoneMsg{}
	}
}

func (m *Model) deepRescanNode(node *model.TreeNode) tea.Cmd {
	checksum := m.cmpOpts.Checksum
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	scanner := m.scanner
	target := scanTargetLabel(node)
	return func() tea.Msg {
		timeScan("deep scan", target, func() {
			scanner.DeepRescanNode(context.Background(), node, checksum, subSecond, timeGrace, ignoreTZDST)
		})
		return rescanDoneMsg{}
	}
}

func (m *Model) listNode(node *model.TreeNode) tea.Cmd {
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	ignoreTZDST := m.cmpOpts.IgnoreTZDST
	scanner := m.scanner
	target := scanTargetLabel(node)
	return func() tea.Msg {
		timeScan("list", target, func() {
			scanner.ListNode(context.Background(), node, subSecond, timeGrace, ignoreTZDST)
		})
		return rescanDoneMsg{}
	}
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

func (m *Model) refreshTree() {
	tree := m.scanner.Tree()
	if tree == nil {
		m.cachedStats = nil
		return
	}
	flat := model.FlattenTree(tree, m.cmpOpts)
	m.leftPanel.SetNodes(flat)
	m.rightPanel.SetNodes(flat)
	s := computeTreeStats(tree)
	m.cachedStats = &s
}

func (m *Model) swapSides() {
	m.leftPanel, m.rightPanel = m.rightPanel, m.leftPanel
	m.leftPanel.isLeft = true
	m.rightPanel.isLeft = false
	m.leftPanel.active = m.activeLeft
	m.rightPanel.active = !m.activeLeft

	m.left, m.right = m.right, m.left

	m.scanner.SwapSides()

	tree := m.scanner.Tree()
	if tree != nil {
		swapTreeData(tree)
		m.refreshTree()
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
	case m.checksumming || progress.Phase == "checksumming...":
		info.State = "CHECKSUM"
		info.ChecksumDone = progress.ChecksumDone
		info.ChecksumTotal = progress.ChecksumFiles
	case m.scanning || progress.Phase == "scanning...":
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

func (m Model) View() string {
	if m.width == 0 {
		return "loading..."
	}

	progress := m.scanner.Progress()

	spinner := ""
	if (progress.Phase != "" && progress.Phase != "done") || m.scanning || m.deleting || m.copying || m.checksumming {
		spinner = spinnerFrames[m.spinFrame]
	}
	operation := ""
	if m.deleting {
		operation = spinner + " deleting..."
	} else if m.checksumming {
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

	if m.deleting || m.copying || m.checksumming {
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
	left := m.leftPanel.View()
	right := m.rightPanel.View()
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
		ltr := m.copyProgress.LeftToRight.Load()
		done := m.copyProgress.Done.Load()
		total := m.copyProgress.Total.Load()
		inflight := m.copyProgress.InFlight.Load()
		parallel := m.copyProgress.Parallel.Load()
		bytes := m.copyProgress.Bytes.Load()
		baseBytes := m.copyProgress.BaseBytes.Load()
		totalBytes := m.copyProgress.TotalBytes.Load()
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
		progressSpinner := spinnerFrames[m.copySpinFrame]
		popup := RenderCopyPopup(file, ltr, done, total, inflight, parallel, fileBytes, fileSize, fileBaseBytes, bytes, totalBytes, baseBytes, fileElapsed, elapsed, progressSpinner, popupW)
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
