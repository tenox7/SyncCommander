package ui

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

// rowTail is the last n words of node's row on p, as shown.
func rowTail(m *Model, p *Panel, node *model.TreeNode, n int) []string {
	var line string
	m.readTree(func(*model.TreeNode) { line = ansi.Strip(p.renderRow(model.Row{Node: node})) })
	f := strings.Fields(line)
	return f[max(len(f)-n, 0):]
}

// \ switches the sizes and counts, in both top bars and on each row,
// between what a side holds and how much more (+) or less (-) it holds
// than the other side. Relative, a row whose differences are all zero
// shows none. \ works while a copy runs, and Settings has it.
func TestBackslashSwitchesCountsToRelative(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "L"), filepath.Join(root, "R")
	for _, f := range []string{"sub/a", "sub/b", "sub/c", "only.txt", "same/x", "eq.txt"} {
		writeTestFile(t, filepath.Join(left, f), "1234")
	}
	writeTestFile(t, filepath.Join(left, "big.txt"), "123456")
	for _, f := range []string{"sub/a", "same/x", "eq.txt"} {
		writeTestFile(t, filepath.Join(right, f), "1234")
	}
	writeTestFile(t, filepath.Join(right, "big.txt"), "12")
	m := scannedModel(t, left, right)
	named := func(name string) *model.TreeNode {
		n := firstNode(m.scanner.Tree(), func(n *model.TreeNode) bool { return n.Name == name })
		if n == nil {
			t.Fatalf("no %s in the tree", name)
		}
		return n
	}
	sub, big, only, same, eq := named("sub"), named("big.txt"), named("only.txt"), named("same"), named("eq.txt")
	lp, rp := m.left.panel, m.right.panel

	type rows struct {
		sub, same  [2][]string // the last words
		big, eq    [2]string   // the last word
		leftOnlyTx string      // the last word of only.txt on the left
	}
	check := func(mode string, top [2]string, want rows) {
		t.Helper()
		bar := ansi.Strip(strings.SplitN(m.View(), "\n", 2)[0])
		for _, w := range top {
			if !strings.Contains(bar, w) {
				t.Errorf("%s: the top bars %q lack %q", mode, bar, w)
			}
		}
		for i, p := range []*Panel{lp, rp} {
			for _, c := range []struct {
				node *model.TreeNode
				want []string
			}{
				{sub, want.sub[i]}, {same, want.same[i]},
				{big, []string{want.big[i]}}, {eq, []string{want.eq[i]}},
			} {
				if got := rowTail(m, p, c.node, len(c.want)); !slices.Equal(got, c.want) {
					t.Errorf("%s: side %d shows %s as %q, want %q", mode, i, c.node.Name, got, c.want)
				}
			}
		}
		if got := rowTail(m, lp, only, 1); !slices.Equal(got, []string{want.leftOnlyTx}) {
			t.Errorf("%s: the left shows only.txt as %q, want %q", mode, got, want.leftOnlyTx)
		}
		if got := rowTail(m, rp, only, 3); strings.Contains(strings.Join(got, " "), "B") {
			t.Errorf("%s: the right shows a size for the left's only.txt: %q", mode, got)
		}
	}
	absolute := func() {
		t.Helper()
		check("absolute", [2]string{"2d 7f 30B", "2d 4f 14B"}, rows{
			sub:        [2][]string{{"0d", "3f", "12B"}, {"0d", "1f", "4B"}},
			same:       [2][]string{{"0d", "1f", "4B"}, {"0d", "1f", "4B"}},
			big:        [2]string{"6B", "2B"},
			eq:         [2]string{"4B", "4B"},
			leftOnlyTx: "4B",
		})
	}
	relative := func() {
		t.Helper()
		// same/ shows its name alone, and eq.txt its age alone.
		check("relative", [2]string{"0d +3f +16B", "0d -3f -16B"}, rows{
			sub:        [2][]string{{"0d", "+2f", "+8B"}, {"0d", "-2f", "-8B"}},
			same:       [2][]string{{"same/"}, {"same/"}},
			big:        [2]string{"+4B", "-4B"},
			eq:         [2]string{"ago", "ago"},
			leftOnlyTx: "+4B",
		})
	}

	absolute()
	press(m, "\\")
	relative()
	press(m, "\\")
	absolute()

	runningOp(m, opCopy, nil)
	press(m, "\\")
	m.write = nil
	if !m.relativeCounts {
		t.Error("\\ did nothing while a copy ran")
	}
	relative()

	press(m, "=")
	for range m.settings.options {
		press(m, "down")
	}
	press(m, " ")
	press(m, "esc")
	if m.relativeCounts {
		t.Error("the last Settings line did not switch the counts back")
	}
	absolute()
}
