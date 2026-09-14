package ui

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	difflib "github.com/sergi/go-diff/diffmatchpatch"
)

type diffMode int

const (
	diffModeText diffMode = iota
	diffModeHex
)

type diffLineKind int

const (
	diffLineEqual diffLineKind = iota
	diffLineDeleted
	diffLineInserted
	diffLineModified
)

// diffLine is one row of the text view. The styled strings are rendered once
// at build time: the per-character diff of a modified row is far too costly
// to redo on every frame.
type diffLine struct {
	kind        diffLineKind
	leftStyled  string
	rightStyled string
	leftLineNo  int
	rightLineNo int
}

type hexDiffRow struct {
	offset   int
	leftHex  []byte
	rightHex []byte
	diffs    []bool // true = byte differs
}

// diffContent is a prepared comparison, built off the UI goroutine.
type diffContent struct {
	mode     diffMode
	lines    []diffLine
	diffIdxs []int // indices into lines where kind != equal
	hexLeft  []byte
	hexRight []byte
}

func buildDiffContent(left, right []byte) *diffContent {
	if isTextContent(left, right) {
		lines, idxs := buildTextDiff(left, right)
		return &diffContent{mode: diffModeText, lines: lines, diffIdxs: idxs}
	}
	return &diffContent{mode: diffModeHex, hexLeft: left, hexRight: right}
}

type DiffView struct {
	visible bool
	title   string
	offset  int
	width   int
	height  int
	loading bool
	err     string
	content *diffContent

	// hex rows depend on the terminal width and are rebuilt on resize
	hexBPR     int
	hexRows    []hexDiffRow
	hexDiffIdx []int // indices into hexRows with at least one diff

	// navigation cursor for n/p
	navPos int
}

func NewDiffView() *DiffView {
	return &DiffView{}
}

func (d *DiffView) Open(title string) {
	*d = DiffView{visible: true, loading: true, title: title, navPos: -1}
}

func (d *DiffView) Close()       { d.visible = false }
func (d *DiffView) IsOpen() bool { return d.visible }

func (d *DiffView) SetError(msg string) {
	d.loading = false
	d.err = msg
}

func (d *DiffView) LoadContent(c *diffContent) {
	d.loading = false
	d.content = c
	d.hexBPR = 0
}

func isTextContent(left, right []byte) bool {
	check := left
	if len(right) > len(check) {
		check = right
	}
	if len(check) == 0 {
		return true
	}
	return utf8.Valid(check[:min(len(check), 512)])
}

// --- text diff ---

func buildTextDiff(leftData, rightData []byte) (lines []diffLine, diffIdxs []int) {
	leftLines := splitLines(expandTabs(string(leftData)))
	rightLines := splitLines(expandTabs(string(rightData)))

	dmp := difflib.New()
	a, b, chars := dmp.DiffLinesToChars(strings.Join(leftLines, "\n"), strings.Join(rightLines, "\n"))
	diffs := dmp.DiffCharsToLines(dmp.DiffMain(a, b, false), chars)

	leftNo, rightNo := 1, 1
	add := func(l diffLine) {
		if l.kind != diffLineEqual {
			diffIdxs = append(diffIdxs, len(lines))
		}
		lines = append(lines, l)
	}
	for i := 0; i < len(diffs); i++ {
		op := diffs[i]
		opLines := splitLines(op.Text)
		switch op.Type {
		case difflib.DiffEqual:
			for _, l := range opLines {
				add(diffLine{kind: diffLineEqual, leftStyled: l, rightStyled: l, leftLineNo: leftNo, rightLineNo: rightNo})
				leftNo++
				rightNo++
			}
		case difflib.DiffDelete:
			if i+1 < len(diffs) && diffs[i+1].Type == difflib.DiffInsert {
				// A delete followed by an insert is a modification: pair the
				// rows and highlight the changed characters.
				insLines := splitLines(diffs[i+1].Text)
				i++
				for j := 0; j < max(len(opLines), len(insLines)); j++ {
					l := diffLine{kind: diffLineModified}
					var lt, rt string
					if j < len(opLines) {
						lt, l.leftLineNo = opLines[j], leftNo
						leftNo++
					}
					if j < len(insLines) {
						rt, l.rightLineNo = insLines[j], rightNo
						rightNo++
					}
					l.leftStyled, l.rightStyled = highlightCharDiff(lt, rt)
					add(l)
				}
				continue
			}
			for _, l := range opLines {
				add(diffLine{kind: diffLineDeleted, leftStyled: styleDiffDel.Render(l), leftLineNo: leftNo})
				leftNo++
			}
		case difflib.DiffInsert:
			for _, l := range opLines {
				add(diffLine{kind: diffLineInserted, rightStyled: styleDiffAdd.Render(l), rightLineNo: rightNo})
				rightNo++
			}
		}
	}
	return lines, diffIdxs
}

// highlightCharDiff renders both sides of a modified row with the changed
// characters coloured; an empty side stays empty.
func highlightCharDiff(leftText, rightText string) (left, right string) {
	if leftText == "" || rightText == "" {
		return styleDiffDel.Render(leftText), styleDiffAdd.Render(rightText)
	}
	dmp := difflib.New()
	diffs := dmp.DiffCleanupSemantic(dmp.DiffMain(leftText, rightText, true))
	var l, r strings.Builder
	for _, diff := range diffs {
		switch diff.Type {
		case difflib.DiffEqual:
			l.WriteString(diff.Text)
			r.WriteString(diff.Text)
		case difflib.DiffDelete:
			l.WriteString(styleDiffDel.Render(diff.Text))
		case difflib.DiffInsert:
			r.WriteString(styleDiffAdd.Render(diff.Text))
		}
	}
	return l.String(), r.String()
}

func expandTabs(s string) string {
	if !strings.ContainsRune(s, '\t') {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + len(s)/8)
	col := 0
	for _, r := range s {
		switch r {
		case '\n':
			sb.WriteRune(r)
			col = 0
		case '\t':
			n := 8 - (col % 8)
			sb.WriteString(strings.Repeat(" ", n))
			col += n
		default:
			sb.WriteRune(r)
			col++
		}
	}
	return sb.String()
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// --- hex diff ---

func (d *DiffView) rebuildHexRows(bpr int) {
	d.hexBPR = bpr
	d.hexRows = nil
	d.hexDiffIdx = nil
	leftData, rightData := d.content.hexLeft, d.content.hexRight
	for off := 0; off < max(len(leftData), len(rightData)); off += bpr {
		row := hexDiffRow{offset: off, diffs: make([]bool, bpr)}
		if off < len(leftData) {
			row.leftHex = leftData[off:min(off+bpr, len(leftData))]
		}
		if off < len(rightData) {
			row.rightHex = rightData[off:min(off+bpr, len(rightData))]
		}
		hasDiff := false
		for i := 0; i < bpr; i++ {
			pos := off + i
			lPresent, rPresent := pos < len(leftData), pos < len(rightData)
			if lPresent != rPresent || (lPresent && leftData[pos] != rightData[pos]) {
				row.diffs[i] = true
				hasDiff = true
			}
		}
		if hasDiff {
			d.hexDiffIdx = append(d.hexDiffIdx, len(d.hexRows))
		}
		d.hexRows = append(d.hexRows, row)
	}
}

// hexBytesPerRow fits "XXXXXXXX " (9) + "HH " per byte + "│" + ASCII + "│".
func hexBytesPerRow(panelWidth int) int {
	return max((panelWidth-11)/4, 4)
}

// --- scrolling ---

func (d *DiffView) viewHeight() int { return max(d.height-4, 1) } // title, rule, rule, keys

func (d *DiffView) totalRows() int {
	if d.content == nil {
		return 0
	}
	if d.content.mode == diffModeText {
		return len(d.content.lines)
	}
	return len(d.hexRows)
}

func (d *DiffView) diffIdxs() []int {
	if d.content == nil {
		return nil
	}
	if d.content.mode == diffModeText {
		return d.content.diffIdxs
	}
	return d.hexDiffIdx
}

func (d *DiffView) clampOffset() {
	d.offset = min(max(d.offset, 0), max(d.totalRows()-d.viewHeight(), 0))
}

func (d *DiffView) ScrollUp()   { d.offset -= d.viewHeight() / 2; d.clampOffset() }
func (d *DiffView) ScrollDown() { d.offset += d.viewHeight() / 2; d.clampOffset() }
func (d *DiffView) PageUp()     { d.offset -= d.viewHeight(); d.clampOffset() }
func (d *DiffView) PageDown()   { d.offset += d.viewHeight(); d.clampOffset() }
func (d *DiffView) Home()       { d.offset = 0 }
func (d *DiffView) End()        { d.offset = d.totalRows(); d.clampOffset() }

func (d *DiffView) NextDiff() { d.jumpDiff(1) }
func (d *DiffView) PrevDiff() { d.jumpDiff(-1) }

// jumpDiff moves the navigation cursor by delta, wrapping, and centres the
// row it lands on.
func (d *DiffView) jumpDiff(delta int) {
	idxs := d.diffIdxs()
	if len(idxs) == 0 {
		return
	}
	d.navPos = (d.navPos + delta + len(idxs)) % len(idxs)
	d.offset = idxs[d.navPos] - d.viewHeight()/2
	d.clampOffset()
}

// --- rendering ---

var (
	styleDiffAdd    = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleDiffDel    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleDiffLineNo = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	styleDiffTitle  = lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	styleDiffDim    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleDiffHexHL  = lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Bold(true)
	styleRowMarkAdd = lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Reverse(true)
	styleRowMarkDel = lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Reverse(true)
	styleRowMarkChg = lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Reverse(true)
)

func rowDiffMarker(kind diffLineKind) string {
	switch kind {
	case diffLineDeleted:
		return styleRowMarkDel.Render(">")
	case diffLineInserted:
		return styleRowMarkAdd.Render(">")
	case diffLineModified:
		return styleRowMarkChg.Render(">")
	}
	return " "
}

func (d *DiffView) View(width, height int) string {
	if !d.visible {
		return ""
	}
	d.width, d.height = width, height
	if d.loading {
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, styleDiffTitle.Render("Loading "+d.title+"..."))
	}
	if d.err != "" {
		msg := styleDiffDel.Render("Error: "+d.err) + styleDiffDim.Render("\nEsc=close")
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, msg)
	}
	if d.content.mode == diffModeText {
		return d.viewText(width)
	}
	return d.viewHex(width)
}

// frame lays out a two-pane screen: title, the rules with their junction at
// the pane split, the rows (already joined), filler and the key hints.
func (d *DiffView) frame(mode string, rows []string, width int) string {
	panelWidth := width / 2
	navInfo := ""
	if idxs := d.diffIdxs(); d.navPos >= 0 && len(idxs) > 0 {
		navInfo = fmt.Sprintf("  [%d/%d]", d.navPos+1, len(idxs))
	}
	title := fmt.Sprintf(" %s  %s  %d diffs%s", mode, d.title, len(d.diffIdxs()), navInfo)
	rule := func(junction string) string {
		return styleDiffDim.Render(strings.Repeat("─", panelWidth-1) + junction + strings.Repeat("─", width-panelWidth))
	}
	empty := strings.Repeat(" ", panelWidth-1) + styleDiffDim.Render("│") + strings.Repeat(" ", width-panelWidth)

	var sb strings.Builder
	sb.WriteString(styleDiffTitle.Render(ansi.Truncate(title, width-2, "")))
	sb.WriteString("\n")
	sb.WriteString(rule("┬"))
	sb.WriteString("\n")
	for _, r := range rows {
		sb.WriteString(r)
		sb.WriteString("\n")
	}
	for i := len(rows); i < d.viewHeight(); i++ {
		sb.WriteString(empty)
		sb.WriteString("\n")
	}
	sb.WriteString(rule("┴"))
	sb.WriteString("\n")
	sb.WriteString(styleDiffDim.Render("n=next diff  p=prev diff  ↑↓=scroll  PgUp/Dn=page  Home/End  q/Esc=close"))
	return sb.String()
}

// fitWidth pads or truncates a styled string to exactly w cells.
func fitWidth(s string, w int) string {
	if lipgloss.Width(s) > w {
		s = ansi.Truncate(s, w, "")
	}
	return s + strings.Repeat(" ", max(w-lipgloss.Width(s), 0))
}

func (d *DiffView) viewText(width int) string {
	const gutterWidth = 6
	panelWidth := width / 2
	leftW, rightW := panelWidth-2, width-panelWidth // row marker and divider take two cells
	lines := d.content.lines
	end := min(d.offset+d.viewHeight(), len(lines))
	rows := make([]string, 0, end-d.offset)
	for _, line := range lines[d.offset:end] {
		left := gutter(line.leftLineNo, gutterWidth) + line.leftStyled
		right := gutter(line.rightLineNo, gutterWidth) + line.rightStyled
		rows = append(rows, rowDiffMarker(line.kind)+fitWidth(left, leftW)+styleDiffDim.Render("│")+fitWidth(right, rightW))
	}
	return d.frame("TEXT", rows, width)
}

func gutter(lineNo, width int) string {
	if lineNo == 0 {
		return strings.Repeat(" ", width)
	}
	return styleDiffLineNo.Render(fmt.Sprintf("%*d ", width-1, lineNo))
}

func (d *DiffView) viewHex(width int) string {
	panelWidth := width / 2
	bpr := hexBytesPerRow(panelWidth - 2)
	if bpr != d.hexBPR {
		d.rebuildHexRows(bpr)
		d.navPos = -1
		d.clampOffset()
	}
	end := min(d.offset+d.viewHeight(), len(d.hexRows))
	rows := make([]string, 0, end-d.offset)
	for _, row := range d.hexRows[d.offset:end] {
		marker := " "
		for _, b := range row.diffs {
			if b {
				marker = styleRowMarkDel.Render(">")
				break
			}
		}
		left := fitWidth(renderHexPanel(row, row.leftHex, bpr), panelWidth-2)
		right := fitWidth(renderHexPanel(row, row.rightHex, bpr), width-panelWidth)
		rows = append(rows, marker+left+styleDiffDim.Render("│")+right)
	}
	return d.frame("HEX", rows, width)
}

func renderHexPanel(row hexDiffRow, data []byte, bpr int) string {
	var hexPart, asciiPart strings.Builder
	asciiPart.WriteString(styleDiffDim.Render("│"))
	for i := 0; i < bpr; i++ {
		if i >= len(data) {
			hexPart.WriteString("   ")
			asciiPart.WriteString(" ")
			continue
		}
		byteStr := fmt.Sprintf("%02x", data[i])
		display := "."
		if data[i] >= 0x20 && data[i] < 0x7f {
			display = string(data[i])
		}
		if row.diffs[i] {
			byteStr, display = styleDiffHexHL.Render(byteStr), styleDiffHexHL.Render(display)
		}
		hexPart.WriteString(byteStr + " ")
		asciiPart.WriteString(display)
	}
	asciiPart.WriteString(styleDiffDim.Render("│"))
	return styleDiffDim.Render(fmt.Sprintf("%08x ", row.offset)) + hexPart.String() + asciiPart.String()
}
