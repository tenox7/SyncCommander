package ui

import (
	"fmt"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// panelRows builds root -> d/ (expanded) -> f0..f{n-1} present on both sides,
// with f0 expanded so its attribute rows follow it, shown in a 40x5 panel.
func panelRows(t *testing.T, n int) (*Panel, *model.TreeNode) {
	t.Helper()
	root := model.NewRootNode()
	both := func(rel string, dir bool) [2]model.SideState {
		e := &model.FileEntry{RelPath: rel, Name: rel[strings.LastIndex(rel, "/")+1:], IsDir: dir, Size: 1}
		return [2]model.SideState{{Entry: e}, {Entry: e}}
	}
	dir := &model.TreeNode{RelPath: "d", Name: "d", IsDir: true, Depth: 1, Parent: root, Listed: true, Expanded: true, Sides: both("d", true)}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("f%d", i)
		dir.Children = append(dir.Children, &model.TreeNode{RelPath: "d/" + name, Name: name, Depth: 2, Parent: dir, Sides: both("d/"+name, false), Expanded: i == 0})
	}
	root.Children = []*model.TreeNode{dir}
	p := NewPanel("root", model.SideLeft)
	p.cmpOpts = &model.CompareOpts{Size: true, ModTime: true}
	p.width, p.height = 40, 5
	p.SetRows(model.FlattenTree(root, p.cmpOpts, 0))
	return p, dir
}

func (p *Panel) reflatten() {
	root := p.rows[0].Node
	p.SetRows(model.FlattenTree(root, p.cmpOpts, len(p.rows)))
}

func TestPanelCursorAndOffsetClamp(t *testing.T) {
	p, _ := panelRows(t, 6)
	n, h := len(p.rows), p.height
	p.MoveUp()
	if p.cursor != 0 || p.offset != 0 {
		t.Fatalf("MoveUp at the top: cursor=%d offset=%d", p.cursor, p.offset)
	}
	for i := 0; i < n+3; i++ {
		p.MoveDown()
	}
	if p.cursor != n-1 || p.offset != n-h {
		t.Fatalf("MoveDown past the end: cursor=%d offset=%d, want %d/%d", p.cursor, p.offset, n-1, n-h)
	}
	p.MoveUp()
	if p.cursor != n-2 || p.offset != n-h {
		t.Fatalf("MoveUp inside the window moved the offset: cursor=%d offset=%d", p.cursor, p.offset)
	}
	p.PageUp()
	if p.cursor != n-2-h || p.offset != p.cursor {
		t.Fatalf("PageUp: cursor=%d offset=%d, want %d/%d", p.cursor, p.offset, n-2-h, n-2-h)
	}
	p.PageDown()
	if p.cursor != n-2 || p.offset != p.cursor-h+1 {
		t.Fatalf("PageDown: cursor=%d offset=%d, want %d/%d", p.cursor, p.offset, n-2, n-2-h+1)
	}
}

func TestPanelSetRowsFollowsTheCursorRow(t *testing.T) {
	p, dir := panelRows(t, 4)
	f0, f1, f2 := dir.Children[0], dir.Children[1], dir.Children[2]
	p.jumpTo(f2)
	if p.CursorNode() != f2 {
		t.Fatal("jumpTo missed f2")
	}
	f1.Expanded = true // inserts attribute rows above f2
	p.reflatten()
	if p.CursorNode() != f2 {
		t.Fatalf("cursor left f2 after rows shifted: %+v", p.rows[p.cursor])
	}
	for i, r := range p.rows {
		if r.Attr != nil {
			p.cursor = i
			break
		}
	}
	label := p.rows[p.cursor].Attr.Label
	if p.CursorNode() != nil || p.CursorFile() != f0 {
		t.Fatal("an attribute row must report no node and f0 as its file")
	}
	p.reflatten()
	if r, _ := p.CursorRow(); r.Attr == nil || r.Attr.Label != label || r.Node != f0 {
		t.Fatalf("attribute row not matched across a rebuild: %+v", r)
	}
	f0.Expanded, f1.Expanded, dir.Expanded = false, false, false
	p.reflatten()
	if p.cursor != len(p.rows)-1 || p.CursorNode() != dir {
		t.Fatalf("vanished row did not clamp the cursor: cursor=%d rows=%d", p.cursor, len(p.rows))
	}
}

func TestPanelViewFillsExactWidth(t *testing.T) {
	p, _ := panelRows(t, 3)
	p.cursor = 2
	p.clampOffset()
	lines := strings.Split(p.View(), "\n")
	if len(lines) != p.height {
		t.Fatalf("%d lines, want %d", len(lines), p.height)
	}
	for i, l := range lines {
		if w := lipgloss.Width(l); w != p.width {
			t.Fatalf("line %d is %d wide, want %d", i, w, p.width)
		}
	}
}

func TestPanelHidesRowsMissingOnItsSide(t *testing.T) {
	p, dir := panelRows(t, 2)
	f0 := dir.Children[0]
	f0.Sides[model.SideRight] = model.SideState{}
	f0.Compare.Presence = model.PresenceLeftOnly
	p.side = model.SideRight
	row := model.Row{Node: f0}
	if line := ansi.Strip(p.renderRow(row)); strings.Contains(line, f0.Name) {
		t.Fatalf("right panel shows a left-only file: %q", line)
	}
	p.side = model.SideLeft
	if line := ansi.Strip(p.renderRow(row)); !strings.Contains(line, f0.Name) {
		t.Fatalf("left panel hides its own file: %q", line)
	}
}
