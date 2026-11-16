package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// F4: a file equal only because its mtimes are whole hours apart (the
// time-zone rule) looked like any other equal file. Its row now shows ≈
// in the right panel's compare column, and its mtime row says how many
// hours apart the times are and why they count as equal.
func TestTimeZoneRuleEqualIsMarked(t *testing.T) {
	opts := &model.CompareOpts{Size: true, ModTime: true, TimeGrace: true, IgnoreTZDST: true}
	at := time.Date(2026, 10, 25, 1, 30, 0, 0, time.UTC)
	entry := func(name string, mtime time.Time) model.FileEntry {
		return model.FileEntry{RelPath: name, Name: name, Size: 10, ModTime: mtime}
	}
	root := model.NewRootNode()
	root.Children = model.MergeChildren(root,
		[]model.FileEntry{entry("shifted.txt", at), entry("same.txt", at)},
		[]model.FileEntry{entry("shifted.txt", at.Add(2*time.Hour)), entry("same.txt", at)}, 1, *opts, nil)
	root.Listed = true
	for _, c := range root.Children {
		c.Expanded = true
	}
	p := NewPanel("root", model.SideRight)
	p.cmpOpts = opts
	p.width, p.height = 120, 20
	p.SetRows(model.FlattenTree(root, opts, 0))
	var shiftedRow, sameRow, mtimeRows []string
	for _, r := range p.rows {
		line := ansi.Strip(p.renderRow(r))
		switch {
		case r.Attr != nil && r.Attr.Label == "mtime":
			mtimeRows = append(mtimeRows, line)
		case r.Attr == nil && r.Node.Name == "shifted.txt":
			shiftedRow = append(shiftedRow, line)
		case r.Attr == nil && r.Node.Name == "same.txt":
			sameRow = append(sameRow, line)
		}
	}
	if len(shiftedRow) != 1 || !strings.HasPrefix(shiftedRow[0], "≈") {
		t.Errorf("the file equal by the time-zone rule shows %q, want it to start with ≈", shiftedRow)
	}
	if len(sameRow) != 1 || strings.HasPrefix(sameRow[0], "≈") {
		t.Errorf("a plainly equal file shows %q, want no ≈", sameRow)
	}
	marked := 0
	for _, l := range mtimeRows {
		if strings.Contains(l, "≈") && strings.Contains(l, "2h apart: time-zone rule") {
			marked++
		}
	}
	if len(mtimeRows) != 2 || marked != 1 {
		t.Errorf("mtime rows %q: want two, one of them marked ≈ and \"2h apart: time-zone rule\"", mtimeRows)
	}
}
