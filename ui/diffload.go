package ui

import (
	"context"
	"fmt"
	"io"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

type diffLoadDoneMsg struct {
	gen     uint64
	content *diffContent
	err     error
}

// diffMaxBytes caps what the diff view pulls into memory per side.
const diffMaxBytes = 8 << 20

// openDiff opens the diff view on a file and starts loading both sides.
func (m *Model) openDiff(node *model.TreeNode) tea.Cmd {
	if node == nil || node.IsDir {
		return nil
	}
	var cmd tea.Cmd
	m.readTree(func(*model.TreeNode) {
		m.diffView.Open(node.Name)
		cmd = m.loadDiffContent(node)
	})
	return cmd
}

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
	leftEntry, rightEntry := node.Entries()
	hasLeft, hasRight := leftEntry != nil, rightEntry != nil
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

// finishDiffLoad shows a finished load, unless the view has moved on.
func (m *Model) finishDiffLoad(msg diffLoadDoneMsg) {
	if msg.gen != m.diffGen || !m.diffView.IsOpen() {
		return
	}
	if msg.err != nil {
		m.diffView.SetError(msg.err.Error())
		return
	}
	m.diffView.LoadContent(msg.content)
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
