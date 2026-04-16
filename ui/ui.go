package ui

import (
	"context"
	"fmt"
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

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type renameDoneMsg struct{ err error }
type deleteDoneMsg struct{}
type copyDoneMsg struct{}

type CopyProgress struct {
	Total atomic.Int64
	Done  atomic.Int64
}

type pendingCopyInfo struct {
	node        *model.TreeNode
	leftToRight bool
}

type Model struct {
	leftPanel     *Panel
	rightPanel    *Panel
	left          model.Backend
	right         model.Backend
	scanner       *model.Scanner
	activeLeft    bool
	scanning      bool
	deleting      bool
	copying       bool
	copyProgress  *CopyProgress
	cmpOpts       model.CompareOpts
	width         int
	height        int
	spinFrame     int
	settings      *SettingsDialog
	input         *InputDialog
	confirm       *ConfirmDialog
	help          *HelpDialog
	info          *InfoDialog
	logView       *LogDialog
	pendingDelete *model.TreeNode
	pendingCopy   *pendingCopyInfo
	openDlg       *OpenDialog
	insecure      bool
}

func NewModel(left, right model.Backend, cksum, insecure bool) Model {
	lp := NewPanel(left.BasePath())
	lp.isLeft = true
	rp := NewPanel(right.BasePath())
	m := Model{
		leftPanel:    lp,
		rightPanel:   rp,
		left:         left,
		right:        right,
		scanner:      model.NewScanner(left, right, 4),
		activeLeft:   true,
		cmpOpts:      model.CompareOpts{Size: true, ModTime: true, Checksum: cksum, TimeGrace: true},
		settings:     NewSettingsDialog(),
		input:        NewInputDialog(),
		confirm:      NewConfirmDialog(),
		help:         NewHelpDialog(),
		info:         NewInfoDialog(),
		logView:      NewLogDialog(),
		openDlg:      NewOpenDialog(),
		copyProgress: &CopyProgress{},
		insecure:     insecure,
	}
	lp.cmpOpts = &m.cmpOpts
	rp.cmpOpts = &m.cmpOpts
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
		m.spinFrame = (m.spinFrame + 1) % len(spinnerFrames)
		if algo := m.scanner.ChecksumAlgo(); algo != "" {
			m.settings.UpdateChecksumLabel(algo)
		}
		m.logView.AutoOpen(transport.Log.ErrCount())
		m.refreshTree()
		return m, m.tickCmd()
	case scanDoneMsg:
		m.scanning = false
		m.refreshTree()
		return m, nil
	case rescanDoneMsg:
		m.refreshTree()
		return m, nil
	case checksumDoneMsg:
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
		m.refreshTree()
		return m, nil
	case copyDoneMsg:
		m.copying = false
		m.refreshTree()
		return m, nil
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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
	switch msg.String() {
	case "q", "esc", "ctrl+c":
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
		if node != nil {
			return m, m.rescanNode(node)
		}
		return m, m.startScan()
	case "c":
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
		if node.IsDir {
			delFiles, delDirs := model.CountMirrorDeletes(node, true)
			if delFiles > 0 || delDirs > 0 {
				m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: true}
				m.confirm.Open(
					"\u26a0 COPY LEFT \u2192 RIGHT",
					[]string{"", node.Name + "/", fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on right"},
					true,
				)
				break
			}
		}
		m.copying = true
		return m, m.copyNode(node, true, false)
	case "<":
		if m.copying {
			break
		}
		node := m.activePanel().CursorNode()
		if node == nil || node.IsAttr || node.Compare.Presence == model.PresenceLeftOnly {
			break
		}
		if node.IsDir {
			delFiles, delDirs := model.CountMirrorDeletes(node, false)
			if delFiles > 0 || delDirs > 0 {
				m.pendingCopy = &pendingCopyInfo{node: node, leftToRight: false}
				m.confirm.Open(
					"\u26a0 COPY RIGHT \u2192 LEFT",
					[]string{"", node.Name + "/", fmt.Sprintf("Will also delete %d files, %d folders", delFiles, delDirs), "that only exist on left"},
					true,
				)
				break
			}
		}
		m.copying = true
		return m, m.copyNode(node, false, false)
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
	case "u":
		if m.copying || m.deleting {
			break
		}
		m.openDlg.Open(m.left.BasePath(), m.right.BasePath())
	case "b":
		if m.copying || m.deleting {
			break
		}
		node := m.activePanel().CursorNode()
		if node == nil || !node.IsDir || node.IsAttr {
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
		m.input.HandleKey(msg.String())
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

	m.scanner = model.NewScanner(m.left, m.right, 4)
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
		m.openDlg.HandleKey(msg.String())
	}
	return m, nil
}

func (m *Model) handleConfirmKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.confirm.choiceMode && m.pendingDelete != nil {
		var side model.Presence
		found := true
		switch msg.String() {
		case ",":
			side = model.PresenceLeftOnly
		case ".":
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
			return m, m.deleteNode(node, side)
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
			return m, m.deleteNode(node, node.Compare.Presence)
		}
		if m.pendingCopy != nil {
			pc := m.pendingCopy
			m.pendingCopy = nil
			m.copying = true
			return m, m.copyNode(pc.node, pc.leftToRight, true)
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
		sides := "left side only"
		if node.Compare.Presence == model.PresenceRightOnly {
			sides = "right side only"
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
	isDir := node.IsDir
	relPath := node.RelPath
	return func() tea.Msg {
		ctx := context.Background()
		delLeft := side != model.PresenceRightOnly
		delRight := side != model.PresenceLeftOnly
		if isDir {
			if delLeft {
				_ = left.RemoveAll(ctx, relPath)
			}
			if delRight {
				_ = right.RemoveAll(ctx, relPath)
			}
		} else {
			if delLeft {
				_ = left.Remove(ctx, relPath)
			}
			if delRight {
				_ = right.Remove(ctx, relPath)
			}
		}
		parentDir := model.DirOf(relPath)
		le, re := scanner.ListBothDir(ctx, parentDir)
		scanner.RefreshDir(parentDir, le, re, subSecond, timeGrace)
		return deleteDoneMsg{}
	}
}

func (m *Model) copyNode(node *model.TreeNode, leftToRight bool, mirror bool) tea.Cmd {
	left := m.left
	right := m.right
	scanner := m.scanner
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	checksum := m.cmpOpts.Checksum
	opts := m.cmpOpts
	progress := m.copyProgress
	return func() tea.Msg {
		ctx := context.Background()

		var files []*model.TreeNode
		if node.IsDir {
			files = model.CollectCopyFiles(node, &opts, leftToRight)
		} else {
			files = []*model.TreeNode{node}
		}

		progress.Total.Store(int64(len(files)))
		progress.Done.Store(0)

		for _, f := range files {
			if ctx.Err() != nil {
				break
			}
			var src, dst model.Backend
			var srcEntry *model.FileEntry
			if leftToRight {
				src, dst = left, right
				srcEntry = f.Left
			} else {
				src, dst = right, left
				srcEntry = f.Right
			}
			if srcEntry == nil {
				progress.Done.Add(1)
				continue
			}
			reader, err := src.Open(ctx, f.RelPath)
			if err != nil {
				progress.Done.Add(1)
				continue
			}
			_ = dst.CopyFrom(ctx, f.RelPath, reader, srcEntry.Mode)
			reader.Close()
			_ = dst.SetTimes(ctx, f.RelPath, srcEntry.ModTime, srcEntry.ATime, srcEntry.BirthTime)
			progress.Done.Add(1)
		}

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
					if d.IsDir {
						_ = delBackend.RemoveAll(ctx, d.RelPath)
					} else {
						_ = delBackend.Remove(ctx, d.RelPath)
					}
				}
			}
			scanner.RescanNode(ctx, node, checksum, subSecond, timeGrace)
		} else {
			parentDir := model.DirOf(node.RelPath)
			ancestor := scanner.FindNearestDestNode(parentDir, leftToRight)
			if ancestor != nil {
				scanner.RescanNode(ctx, ancestor, checksum, subSecond, timeGrace)
			}
		}
		return copyDoneMsg{}
	}
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
	return func() tea.Msg {
		ctx := context.Background()
		l, r := node.Left, node.Right
		if l == nil || r == nil {
			return touchDoneMsg{}
		}
		newer, older := l, r
		olderBackend := right
		if r.ModTime.After(l.ModTime) {
			newer, older = r, l
			olderBackend = left
		}
		_ = olderBackend.SetTimes(ctx, older.RelPath, newer.ModTime, newer.ATime, newer.BirthTime)
		parentDir := model.DirOf(node.RelPath)
		le, re := scanner.ListBothDir(ctx, parentDir)
		scanner.RefreshDir(model.DirOf(node.RelPath), le, re, subSecond, timeGrace)
		return touchDoneMsg{}
	}
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
	scanner := m.scanner
	return func() tea.Msg {
		scanner.Scan(context.Background(), checksum, subSecond, timeGrace)
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

func (m *Model) rescanNode(node *model.TreeNode) tea.Cmd {
	checksum := m.cmpOpts.Checksum
	subSecond := m.cmpOpts.SubSecond
	timeGrace := m.cmpOpts.TimeGrace
	scanner := m.scanner
	return func() tea.Msg {
		scanner.RescanNode(context.Background(), node, checksum, subSecond, timeGrace)
		return rescanDoneMsg{}
	}
}

func (m *Model) refreshTree() {
	tree := m.scanner.Tree()
	if tree == nil {
		return
	}
	flat := model.FlattenTree(tree, &m.cmpOpts)
	m.leftPanel.SetNodes(flat)
	m.rightPanel.SetNodes(flat)
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
	tree := m.scanner.Tree()

	spinner := ""
	if (progress.Phase != "" && progress.Phase != "done") || m.deleting || m.copying {
		spinner = spinnerFrames[m.spinFrame]
	}
	operation := ""
	if m.deleting {
		operation = spinner + " deleting..."
	} else if m.copying {
		done := m.copyProgress.Done.Load()
		total := m.copyProgress.Total.Load()
		operation = fmt.Sprintf("%s copying... %s %d/%d", spinner, progressBar(done, total, 20), done, total)
	}

	var stats *TreeStats
	if tree != nil {
		s := computeTreeStats(tree)
		stats = &s
	}
	leftPrefix := ""
	rightPrefix := ""
	if operation != "" {
		leftPrefix = operation
		rightPrefix = operation
	}
	leftTopBar := RenderPanelTopBar(stats, true, leftPrefix, m.leftPanel.width)
	rightTopBar := RenderPanelTopBar(stats, false, rightPrefix, m.rightPanel.width)
	topBar := leftTopBar + rightTopBar
	bottomBar := RenderBottomBar(m.width)

	if m.deleting || m.copying {
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
