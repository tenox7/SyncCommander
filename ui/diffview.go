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

type diffLine struct {
	kind       diffLineKind
	leftText   string
	rightText  string
	leftLineNo int
	rightLineNo int
}

type hexDiffRow struct {
	offset   int
	leftHex  []byte
	rightHex []byte
	diffs    []bool // true = byte differs
}

type DiffView struct {
	visible    bool
	mode       diffMode
	title      string
	offset     int
	width      int
	height     int
	loading    bool
	err        string

	// text mode
	lines    []diffLine
	diffIdxs []int // indices into lines where kind != equal

	// hex mode
	hexLeftData  []byte
	hexRightData []byte
	hexBPR       int // bytes per row, computed from width
	hexRows      []hexDiffRow
	hexDiffIdx   []int // indices into hexRows with at least one diff

	// navigation cursor for n/p
	navPos int
}

func NewDiffView() *DiffView {
	return &DiffView{}
}

func (d *DiffView) Open(title string) {
	d.visible = true
	d.loading = true
	d.title = title
	d.offset = 0
	d.err = ""
	d.lines = nil
	d.diffIdxs = nil
	d.hexLeftData = nil
	d.hexRightData = nil
	d.hexBPR = 0
	d.hexRows = nil
	d.hexDiffIdx = nil
	d.navPos = -1
}

func (d *DiffView) Close()       { d.visible = false }
func (d *DiffView) IsOpen() bool { return d.visible }

func (d *DiffView) SetError(msg string) {
	d.loading = false
	d.err = msg
}

func (d *DiffView) LoadContent(leftData, rightData []byte) {
	d.loading = false
	if isTextContent(leftData, rightData) {
		d.mode = diffModeText
		d.buildTextDiff(leftData, rightData)
	} else {
		d.mode = diffModeHex
		d.buildHexDiff(leftData, rightData)
	}
}

func isTextContent(left, right []byte) bool {
	check := left
	if len(right) > len(check) {
		check = right
	}
	if len(check) == 0 {
		return true
	}
	sample := check
	if len(sample) > 512 {
		sample = sample[:512]
	}
	return utf8.Valid(sample)
}

// --- text diff ---

func (d *DiffView) buildTextDiff(leftData, rightData []byte) {
	leftLines := splitLines(string(leftData))
	rightLines := splitLines(string(rightData))

	dmp := difflib.New()
	a, b, lines := dmp.DiffLinesToChars(strings.Join(leftLines, "\n"), strings.Join(rightLines, "\n"))
	diffs := dmp.DiffMain(a, b, false)
	diffs = dmp.DiffCharsToLines(diffs, lines)

	d.lines = nil
	d.diffIdxs = nil
	leftNo := 1
	rightNo := 1

	for i := 0; i < len(diffs); i++ {
		op := diffs[i]
		opLines := splitLines(op.Text)

		switch op.Type {
		case difflib.DiffEqual:
			for _, l := range opLines {
				d.lines = append(d.lines, diffLine{
					kind:        diffLineEqual,
					leftText:    l,
					rightText:   l,
					leftLineNo:  leftNo,
					rightLineNo: rightNo,
				})
				leftNo++
				rightNo++
			}
		case difflib.DiffDelete:
			// check if next op is Insert -> pair as Modified
			if i+1 < len(diffs) && diffs[i+1].Type == difflib.DiffInsert {
				insLines := splitLines(diffs[i+1].Text)
				d.pairModifiedLines(opLines, insLines, &leftNo, &rightNo)
				i++ // skip the insert
			} else {
				for _, l := range opLines {
					idx := len(d.lines)
					d.lines = append(d.lines, diffLine{
						kind:       diffLineDeleted,
						leftText:   l,
						leftLineNo: leftNo,
					})
					d.diffIdxs = append(d.diffIdxs, idx)
					leftNo++
				}
			}
		case difflib.DiffInsert:
			for _, l := range opLines {
				idx := len(d.lines)
				d.lines = append(d.lines, diffLine{
					kind:        diffLineInserted,
					rightText:   l,
					rightLineNo: rightNo,
				})
				d.diffIdxs = append(d.diffIdxs, idx)
				rightNo++
			}
		}
	}
}

func (d *DiffView) pairModifiedLines(delLines, insLines []string, leftNo, rightNo *int) {
	maxLen := len(delLines)
	if len(insLines) > maxLen {
		maxLen = len(insLines)
	}
	for i := 0; i < maxLen; i++ {
		dl := diffLine{kind: diffLineModified}
		if i < len(delLines) {
			dl.leftText = delLines[i]
			dl.leftLineNo = *leftNo
			*leftNo++
		}
		if i < len(insLines) {
			dl.rightText = insLines[i]
			dl.rightLineNo = *rightNo
			*rightNo++
		}
		idx := len(d.lines)
		d.lines = append(d.lines, dl)
		d.diffIdxs = append(d.diffIdxs, idx)
	}
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	// remove trailing empty from final newline
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// --- hex diff ---

func (d *DiffView) buildHexDiff(leftData, rightData []byte) {
	d.hexLeftData = leftData
	d.hexRightData = rightData
	d.hexBPR = 0
	d.hexRows = nil
	d.hexDiffIdx = nil
}

func (d *DiffView) rebuildHexRows(bpr int) {
	d.hexBPR = bpr
	d.hexRows = nil
	d.hexDiffIdx = nil

	leftData := d.hexLeftData
	rightData := d.hexRightData
	maxLen := len(leftData)
	if len(rightData) > maxLen {
		maxLen = len(rightData)
	}

	for off := 0; off < maxLen; off += bpr {
		lEnd := off + bpr
		if lEnd > len(leftData) {
			lEnd = len(leftData)
		}
		rEnd := off + bpr
		if rEnd > len(rightData) {
			rEnd = len(rightData)
		}

		var lSlice, rSlice []byte
		if off < len(leftData) {
			lSlice = leftData[off:lEnd]
		}
		if off < len(rightData) {
			rSlice = rightData[off:rEnd]
		}

		row := hexDiffRow{
			offset:   off,
			leftHex:  lSlice,
			rightHex: rSlice,
			diffs:    make([]bool, bpr),
		}

		hasDiff := false
		for i := 0; i < bpr; i++ {
			pos := off + i
			lPresent := pos < len(leftData)
			rPresent := pos < len(rightData)
			if lPresent != rPresent {
				row.diffs[i] = true
				hasDiff = true
			} else if lPresent && rPresent && leftData[pos] != rightData[pos] {
				row.diffs[i] = true
				hasDiff = true
			}
		}

		idx := len(d.hexRows)
		d.hexRows = append(d.hexRows, row)
		if hasDiff {
			d.hexDiffIdx = append(d.hexDiffIdx, idx)
		}
	}
}

func hexBytesPerRow(panelWidth int) int {
	// layout: "XXXXXXXX " (9) + "HH " * N (3N) + "│" (1) + ASCII (N) + "│" (1) = 11 + 4N
	n := (panelWidth - 11) / 4
	if n < 4 {
		n = 4
	}
	return n
}

// --- scrolling ---

func (d *DiffView) viewHeight() int {
	h := d.height - 4 // title + separator + bottom separator + keys
	if h < 1 {
		h = 1
	}
	return h
}

func (d *DiffView) totalRows() int {
	if d.mode == diffModeText {
		return len(d.lines)
	}
	return len(d.hexRows)
}

func (d *DiffView) ScrollUp() {
	d.offset -= d.viewHeight() / 2
	if d.offset < 0 {
		d.offset = 0
	}
}

func (d *DiffView) ScrollDown() {
	maxOff := d.totalRows() - d.viewHeight()
	if maxOff < 0 {
		maxOff = 0
	}
	d.offset += d.viewHeight() / 2
	if d.offset > maxOff {
		d.offset = maxOff
	}
}

func (d *DiffView) PageUp() {
	d.offset -= d.viewHeight()
	if d.offset < 0 {
		d.offset = 0
	}
}

func (d *DiffView) PageDown() {
	maxOff := d.totalRows() - d.viewHeight()
	if maxOff < 0 {
		maxOff = 0
	}
	d.offset += d.viewHeight()
	if d.offset > maxOff {
		d.offset = maxOff
	}
}

func (d *DiffView) Home() {
	d.offset = 0
}

func (d *DiffView) End() {
	maxOff := d.totalRows() - d.viewHeight()
	if maxOff < 0 {
		maxOff = 0
	}
	d.offset = maxOff
}

func (d *DiffView) NextDiff() {
	idxs := d.diffIdxs
	if d.mode == diffModeHex {
		idxs = d.hexDiffIdx
	}
	if len(idxs) == 0 {
		return
	}
	d.navPos++
	if d.navPos >= len(idxs) {
		d.navPos = 0
	}
	d.scrollToRow(idxs[d.navPos])
}

func (d *DiffView) PrevDiff() {
	idxs := d.diffIdxs
	if d.mode == diffModeHex {
		idxs = d.hexDiffIdx
	}
	if len(idxs) == 0 {
		return
	}
	d.navPos--
	if d.navPos < 0 {
		d.navPos = len(idxs) - 1
	}
	d.scrollToRow(idxs[d.navPos])
}

func (d *DiffView) scrollToRow(row int) {
	vh := d.viewHeight()
	// center the row in the viewport
	d.offset = row - vh/2
	if d.offset < 0 {
		d.offset = 0
	}
	maxOff := d.totalRows() - vh
	if maxOff < 0 {
		maxOff = 0
	}
	if d.offset > maxOff {
		d.offset = maxOff
	}
}

func (d *DiffView) diffCount() int {
	if d.mode == diffModeText {
		return len(d.diffIdxs)
	}
	return len(d.hexDiffIdx)
}

// --- rendering ---

var (
	styleDiffAdd    = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleDiffDel    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleDiffChg    = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	styleDiffLineNo = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	styleDiffTitle  = lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	styleDiffDim    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleDiffHexHL  = lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Bold(true)
)

func (d *DiffView) View(width, height int) string {
	if !d.visible {
		return ""
	}
	d.width = width
	d.height = height

	if d.loading {
		msg := styleDiffTitle.Render("Loading " + d.title + "...")
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, msg)
	}
	if d.err != "" {
		msg := styleDiffDel.Render("Error: " + d.err)
		hint := styleDiffDim.Render("\nEsc=close")
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, msg+hint)
	}

	if d.mode == diffModeText {
		return d.viewText(width, height)
	}
	return d.viewHex(width, height)
}

func (d *DiffView) viewText(width, height int) string {
	vh := d.viewHeight()
	panelWidth := width / 2
	gutterWidth := 6 // line number gutter
	contentWidth := panelWidth - gutterWidth - 3 // gutter + space + border
	if contentWidth < 10 {
		contentWidth = 10
	}

	var sb strings.Builder

	// title bar
	navInfo := ""
	if d.navPos >= 0 && len(d.diffIdxs) > 0 {
		navInfo = fmt.Sprintf("  [%d/%d]", d.navPos+1, len(d.diffIdxs))
	}
	modeStr := "TEXT"
	title := fmt.Sprintf(" %s  %s  %d diffs%s", modeStr, d.title, len(d.diffIdxs), navInfo)
	sb.WriteString(styleDiffTitle.Render(ansi.Truncate(title, width-2, "")))
	sb.WriteString("\n")
	sb.WriteString(styleDiffDim.Render(strings.Repeat("─", panelWidth-1) + "┬" + strings.Repeat("─", width-panelWidth)))
	sb.WriteString("\n")

	end := d.offset + vh
	if end > len(d.lines) {
		end = len(d.lines)
	}

	for i := d.offset; i < end; i++ {
		line := d.lines[i]
		leftPart := d.renderTextLine(line, true, gutterWidth, contentWidth)
		rightPart := d.renderTextLine(line, false, gutterWidth, contentWidth)
		sb.WriteString(leftPart)
		sb.WriteString(styleDiffDim.Render("│"))
		sb.WriteString(rightPart)
		sb.WriteString("\n")
	}

	// fill remaining rows
	emptyLeft := strings.Repeat(" ", panelWidth-1)
	emptyRight := strings.Repeat(" ", width-panelWidth)
	for i := end - d.offset; i < vh; i++ {
		sb.WriteString(emptyLeft)
		sb.WriteString(styleDiffDim.Render("│"))
		sb.WriteString(emptyRight)
		sb.WriteString("\n")
	}

	sb.WriteString(styleDiffDim.Render(strings.Repeat("─", panelWidth-1) + "┴" + strings.Repeat("─", width-panelWidth)))
	sb.WriteString("\n")
	sb.WriteString(styleDiffDim.Render("n=next diff  p=prev diff  ↑↓=scroll  PgUp/Dn=page  Home/End  q/Esc=close"))

	return sb.String()
}

func (d *DiffView) renderTextLine(line diffLine, isLeft bool, gutterWidth, contentWidth int) string {
	panelWidth := d.width / 2
	if !isLeft {
		panelWidth = d.width - d.width/2
	}
	availWidth := panelWidth
	if isLeft {
		availWidth-- // room for center divider
	}

	lineNo := line.leftLineNo
	text := line.leftText
	if !isLeft {
		lineNo = line.rightLineNo
		text = line.rightText
	}

	// gutter
	gutter := ""
	if lineNo > 0 {
		gutter = styleDiffLineNo.Render(fmt.Sprintf("%*d ", gutterWidth-1, lineNo))
	} else {
		gutter = styleDiffDim.Render(strings.Repeat(" ", gutterWidth))
	}

	marker := " "
	styledText := text

	switch line.kind {
	case diffLineEqual:
		styledText = text
	case diffLineDeleted:
		if isLeft {
			marker = styleDiffDel.Render("-")
			styledText = styleDiffDel.Render(text)
		} else {
			styledText = ""
		}
	case diffLineInserted:
		if !isLeft {
			marker = styleDiffAdd.Render("+")
			styledText = styleDiffAdd.Render(text)
		} else {
			styledText = ""
		}
	case diffLineModified:
		if isLeft && text != "" {
			marker = styleDiffChg.Render("~")
			styledText = d.highlightCharDiff(line.leftText, line.rightText, true)
		} else if !isLeft && text != "" {
			marker = styleDiffChg.Render("~")
			styledText = d.highlightCharDiff(line.leftText, line.rightText, false)
		} else {
			styledText = ""
		}
	}

	result := gutter + marker + styledText
	visLen := lipgloss.Width(result)
	if visLen > availWidth {
		result = ansi.Truncate(result, availWidth, "")
		visLen = lipgloss.Width(result)
	}
	if visLen < availWidth {
		result += strings.Repeat(" ", availWidth-visLen)
	}
	return result
}

func (d *DiffView) highlightCharDiff(leftText, rightText string, showLeft bool) string {
	dmp := difflib.New()
	diffs := dmp.DiffMain(leftText, rightText, true)
	diffs = dmp.DiffCleanupSemantic(diffs)

	var sb strings.Builder
	for _, diff := range diffs {
		switch diff.Type {
		case difflib.DiffEqual:
			sb.WriteString(diff.Text)
		case difflib.DiffDelete:
			if showLeft {
				sb.WriteString(styleDiffDel.Render(diff.Text))
			}
		case difflib.DiffInsert:
			if !showLeft {
				sb.WriteString(styleDiffAdd.Render(diff.Text))
			}
		}
	}
	return sb.String()
}

// --- hex view ---

func (d *DiffView) viewHex(width, height int) string {
	vh := d.viewHeight()
	panelWidth := width / 2

	// recompute rows if terminal width changed
	bpr := hexBytesPerRow(panelWidth - 1) // -1 for center divider
	if bpr != d.hexBPR {
		d.rebuildHexRows(bpr)
		d.navPos = -1
	}

	var sb strings.Builder

	// title bar
	navInfo := ""
	if d.navPos >= 0 && len(d.hexDiffIdx) > 0 {
		navInfo = fmt.Sprintf("  [%d/%d]", d.navPos+1, len(d.hexDiffIdx))
	}
	title := fmt.Sprintf(" HEX  %s  %d diffs%s", d.title, len(d.hexDiffIdx), navInfo)
	sb.WriteString(styleDiffTitle.Render(ansi.Truncate(title, width-2, "")))
	sb.WriteString("\n")
	sb.WriteString(styleDiffDim.Render(strings.Repeat("─", panelWidth-1) + "┬" + strings.Repeat("─", width-panelWidth)))
	sb.WriteString("\n")

	end := d.offset + vh
	if end > len(d.hexRows) {
		end = len(d.hexRows)
	}

	for i := d.offset; i < end; i++ {
		row := d.hexRows[i]
		leftPart := d.renderHexPanel(row, true, panelWidth-1, bpr)
		rightPart := d.renderHexPanel(row, false, width-panelWidth, bpr)
		sb.WriteString(leftPart)
		sb.WriteString(styleDiffDim.Render("│"))
		sb.WriteString(rightPart)
		sb.WriteString("\n")
	}

	emptyLeft := strings.Repeat(" ", panelWidth-1)
	emptyRight := strings.Repeat(" ", width-panelWidth)
	for i := end - d.offset; i < vh; i++ {
		sb.WriteString(emptyLeft)
		sb.WriteString(styleDiffDim.Render("│"))
		sb.WriteString(emptyRight)
		sb.WriteString("\n")
	}

	sb.WriteString(styleDiffDim.Render(strings.Repeat("─", panelWidth-1) + "┴" + strings.Repeat("─", width-panelWidth)))
	sb.WriteString("\n")
	sb.WriteString(styleDiffDim.Render("n=next diff  p=prev diff  ↑↓=scroll  PgUp/Dn=page  Home/End  q/Esc=close"))

	return sb.String()
}

func (d *DiffView) renderHexPanel(row hexDiffRow, isLeft bool, availWidth int, bpr int) string {
	data := row.leftHex
	if !isLeft {
		data = row.rightHex
	}
	dataLen := len(data)

	// offset: 8 chars + space
	offsetStr := styleDiffDim.Render(fmt.Sprintf("%08x ", row.offset))

	// hex bytes
	var hexPart strings.Builder
	for i := 0; i < bpr; i++ {
		if i < dataLen {
			byteStr := fmt.Sprintf("%02x", data[i])
			if row.diffs[i] {
				hexPart.WriteString(styleDiffHexHL.Render(byteStr))
			} else {
				hexPart.WriteString(byteStr)
			}
			hexPart.WriteString(" ")
		} else {
			hexPart.WriteString("   ")
		}
	}

	// ascii
	var asciiPart strings.Builder
	asciiPart.WriteString(styleDiffDim.Render("│"))
	for i := 0; i < bpr; i++ {
		if i < dataLen {
			ch := data[i]
			display := "."
			if ch >= 0x20 && ch < 0x7f {
				display = string(ch)
			}
			if row.diffs[i] {
				asciiPart.WriteString(styleDiffHexHL.Render(display))
			} else {
				asciiPart.WriteString(display)
			}
		} else {
			asciiPart.WriteString(" ")
		}
	}
	asciiPart.WriteString(styleDiffDim.Render("│"))

	result := offsetStr + hexPart.String() + asciiPart.String()
	visLen := lipgloss.Width(result)
	if visLen > availWidth {
		result = ansi.Truncate(result, availWidth, "")
		visLen = lipgloss.Width(result)
	}
	if visLen < availWidth {
		result += strings.Repeat(" ", availWidth-visLen)
	}
	return result
}
