package ui

import (
	"context"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
)

// The UI walks and renders the live tree while scanner goroutines are still
// listing into it. Drives the real Update/View loop against a scan, a rescan
// and a concurrent copy-style enumeration; run under -race.
func TestUIRaceAgainstScanner(t *testing.T) {
	lb, err := transport.TryOpenBackend("fake://x?dirs=4&files=5&depth=3", false, 4)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := transport.TryOpenBackend("fake://x?dirs=4&files=5&depth=3&diff=0.2&drop=0.05", false, 4)
	if err != nil {
		t.Fatal(err)
	}

	opts := &model.CompareOpts{Size: true, ModTime: true, Checksum: true, TimeGrace: true}
	m := NewModel(lb, rb, "fake://x?dirs=4&files=5&depth=3", "fake://x?dirs=4&files=5&depth=3&diff=0.2&drop=0.05", opts, false, true, 2, 8, false, false)
	m.width, m.height = 120, 40
	m.layoutPanels()
	sc := m.scanner

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		sc.Scan(ctx, model.CompareOpts{Checksum: true, TimeGrace: true, IgnoreTZDST: true})
		if root := sc.Tree(); root != nil {
			sc.RescanNode(ctx, root, model.CompareOpts{Checksum: true, TimeGrace: true, IgnoreTZDST: true}, nil)
		}
	}()

	// A copy enumerates the tree from its own goroutine: a second reader.
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for ctx.Err() == nil {
			sc.ReadTree(func(root *model.TreeNode) {
				if root == nil {
					return
				}
				model.CollectCopyFiles(root, opts, true)
				model.CollectTypeCollisions(root, true)
			})
			select {
			case <-scanned:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	keys := []tea.KeyMsg{
		{Type: tea.KeyRunes, Runes: []rune{'}'}},
		{Type: tea.KeyDown},
		{Type: tea.KeyRunes, Runes: []rune{'i'}},
		{Type: tea.KeyRunes, Runes: []rune{'i'}},
		{Type: tea.KeyRight},
		{Type: tea.KeyLeft},
		{Type: tea.KeyRunes, Runes: []rune{'{'}},
		{Type: tea.KeyRunes, Runes: []rune{'n'}},
		{Type: tea.KeyRunes, Runes: []rune{'t'}},
	}

	pump := func(msg tea.Msg) { m.Update(msg) }
	for i := 0; ; i++ {
		pump(tickMsg(time.Now()))
		pump(keys[i%len(keys)])
		_ = m.View()
		select {
		case <-scanned:
			_ = m.View()
			<-collected
			return
		default:
		}
	}
}

func openBackendOrSkip(t *testing.T, path string) model.Backend {
	t.Helper()
	b, err := transport.TryOpenBackend(path, false, 4)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newTestModel(t *testing.T, left, right model.Backend) *Model {
	t.Helper()
	m := NewModel(left, right, left.BasePath(), right.BasePath(), &model.CompareOpts{Size: true, ModTime: true, TimeGrace: true}, false, true, 2, 8, false, false)
	m.width, m.height = 120, 40
	m.layoutPanels()
	return m
}
