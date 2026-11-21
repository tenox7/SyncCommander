package ui

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// 2026-11-20: a link compares by its target, never by its time. In the
// right panel its row shows ≡ for the same target, ≢ for another, and no
// mark when a target is not known: not ≈, the time-zone rule, which is
// about times.
func TestLinkRowMarks(t *testing.T) {
	opts := &model.CompareOpts{Size: true, ModTime: true, TimeGrace: true, IgnoreTZDST: true}
	at := time.Date(2026, 11, 20, 1, 30, 0, 0, time.UTC)
	link := func(name, target string, mtime time.Time) model.FileEntry {
		return model.FileEntry{RelPath: name, Name: name, Size: 5, ModTime: mtime, Mode: os.ModeSymlink | 0o777, Link: model.LinkTo(target)}
	}
	root := model.NewRootNode()
	root.Children = model.MergeChildren(root,
		[]model.FileEntry{link("same", "a.txt", at), link("other", "a.txt", at), link("unknown", "a.txt", at)},
		[]model.FileEntry{link("same", "a.txt", at.Add(2*time.Hour)), link("other", "b.txt", at), link("unknown", "", at.Add(2*time.Hour))}, 1, *opts, nil)
	root.Listed = true
	p := NewPanel("root", model.SideRight)
	p.cmpOpts = opts
	p.width, p.height = 120, 20
	p.SetRows(model.FlattenTree(root, opts, 0))
	want := map[string]string{"same": "≡", "other": "≢", "unknown": " "}
	seen := 0
	for _, r := range p.rows {
		w, ok := want[r.Node.Name]
		if r.Attr != nil || !ok {
			continue
		}
		seen++
		if line := ansi.Strip(p.renderRow(r)); !strings.HasPrefix(line, w) {
			t.Errorf("the link %s shows %q, want it to start with %q", r.Node.Name, line, w)
		}
	}
	if seen != len(want) {
		t.Errorf("%d link rows, want %d", seen, len(want))
	}
}
