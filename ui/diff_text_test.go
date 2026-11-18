package ui

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"sc/model"
)

func diffScreen(c *diffContent, width, height int) string {
	d := NewDiffView()
	d.Open("t")
	d.LoadContent(c)
	return ansi.Strip(d.View(width, height))
}

// U22: text that ends its lines in \r\n showed ^M at the end of every
// row. It no longer does where both sides end lines so; where the sides
// end them in two ways, the rows still show it.
func TestDiffShowsCRLFTextWithoutMarks(t *testing.T) {
	for _, c := range []struct {
		what        string
		left, right string
		diffs       int
		marks       bool
	}{
		{"both \\r\\n", "alpha\r\nbeta\r\n", "alpha\r\ngamma\r\n", 1, false},
		{"one side only", "alpha\r\nbeta\r\n", "", 2, false},
		{"\\r\\n against \\n", "alpha\r\nbeta\r\n", "alpha\nbeta\n", 2, true},
		{"a side with both", "alpha\r\nbeta\n", "alpha\r\nbeta\n", 0, true},
	} {
		content := buildDiffContent([]byte(c.left), []byte(c.right))
		if content.mode != diffModeText || len(content.diffIdxs) != c.diffs {
			t.Errorf("%s: mode %v, %d diffs; want text, %d", c.what, content.mode, len(content.diffIdxs), c.diffs)
		}
		screen := diffScreen(content, 60, 8)
		if got := strings.Contains(screen, "^M"); got != c.marks {
			t.Errorf("%s: ^M shown %v, want %v:\n%s", c.what, got, c.marks, screen)
		}
		if !strings.Contains(screen, "alpha") {
			t.Errorf("%s: no alpha row:\n%s", c.what, screen)
		}
	}
}

// U22: the text check read the first 512 bytes, and a cut through a
// character made valid UTF-8 text open as hex.
func TestDiffJudgesTextOnACharacterBoundary(t *testing.T) {
	for _, c := range []struct {
		what string
		data string
		mode diffMode
	}{
		{"é across byte 512", strings.Repeat("a", 511) + "é and more text", diffModeText},
		{"a 4-byte character from byte 510", strings.Repeat("a", 510) + "😀 and more", diffModeText},
		{"a 4-byte character from byte 511", strings.Repeat("a", 511) + "😀 and more", diffModeText},
		{"a lead byte alone at 511", strings.Repeat("a", 511) + "\xc3(" + strings.Repeat("b", 10), diffModeHex},
		{"stray bytes up to 512", strings.Repeat("a", 509) + "\x80\x80\x80\x80b" + strings.Repeat("b", 10), diffModeHex},
		{"stray bytes at 512 and 513", strings.Repeat("a", 512) + "\x80\x80b" + strings.Repeat("b", 10), diffModeHex},
		{"binary early", "abc\xff" + strings.Repeat("a", 600), diffModeHex},
		{"short text", "héllo", diffModeText},
		{"empty", "", diffModeText},
	} {
		if got := buildDiffContent([]byte(c.data), []byte(c.data)).mode; got != c.mode {
			t.Errorf("%s: mode %v, want %v", c.what, got, c.mode)
		}
	}
}

// U22: a screen 1 cell wide panicked with "strings: negative Repeat
// count", and the app ended. Every size now draws.
func TestDiffViewDrawsAtAnySize(t *testing.T) {
	contents := map[string]*diffContent{
		"text": buildDiffContent([]byte("a\nb\nc\n"), []byte("a\nx\nc\n")),
		"hex":  buildDiffContent([]byte("\xff\x00\x01"), []byte("\xfe\x00\x01")),
	}
	for name, c := range contents {
		for w := 0; w <= 12; w++ {
			for h := 0; h <= 6; h++ {
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("%s at %dx%d: panic %v", name, w, h, r)
						}
					}()
					diffScreen(c, w, h)
				}()
			}
		}
	}
}

// bigInTree lists big.txt as larger than the diff view takes, and counts
// the files opened.
type bigInTree struct {
	model.Backend
	opens *atomic.Int32
}

func (b bigInTree) List(ctx context.Context, rel string) ([]model.FileEntry, error) {
	entries, err := b.Backend.List(ctx, rel)
	for i := range entries {
		if entries[i].Name == "big.txt" {
			entries[i].Size = diffMaxBytes + 1
		}
	}
	return entries, err
}

func (b bigInTree) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	b.opens.Add(1)
	return b.Backend.Open(ctx, rel)
}

// U22: the sizes are in the tree, yet up to diffMaxBytes of each side
// came down before the view said "too big". A side the tree knows is too
// big is no longer read.
func TestDiffDoesNotReadAFileTheTreeKnowsIsTooBig(t *testing.T) {
	for _, bigOn := range []string{"both", "right"} {
		t.Run(bigOn, func(t *testing.T) { diffOfBig(t, bigOn) })
	}
}

func diffOfBig(t *testing.T, bigOn string) {
	root := t.TempDir()
	var opens atomic.Int32
	var sides [2]model.Backend
	for i, side := range []string{"L", "R"} {
		writeTestFile(t, filepath.Join(root, side, "big.txt"), "small on disk, big in the tree")
		b := openBackendOrSkip(t, filepath.Join(root, side))
		if bigOn == "both" || side == "R" {
			b = bigInTree{Backend: b, opens: &opens}
		}
		sides[i] = b
	}
	m := newTestModel(t, sides[0], sides[1])
	m.scanner.Scan(context.Background(), *m.cmpOpts)
	m.refreshTreeNow()
	settled(m)
	m.cursorOn(t, "big.txt")
	cmd := press(m, "o")
	if cmd == nil || !m.diffView.IsOpen() {
		t.Fatal("o opened no diff")
	}
	msg, ok := cmd().(diffLoadDoneMsg)
	if !ok {
		t.Fatalf("the load gave %T", msg)
	}
	if msg.err == nil || !strings.Contains(msg.err.Error(), "too big") {
		t.Errorf("the load ended with %v, want too big", msg.err)
	}
	if n := opens.Load(); n != 0 {
		t.Errorf("%d sides were opened", n)
	}
	m.Update(msg)
	if screen := ansi.Strip(m.View()); !strings.Contains(screen, "too big") {
		t.Errorf("the view does not say too big:\n%s", screen)
	}
}
