package ui

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"sc/model"
	"sc/transport"
)

var sgr = regexp.MustCompile("\x1b\\[[0-9;:]*m")

// terminalSafe returns where s would act on a terminal: a control other
// than a line break, outside the colour sequences the UI draws itself, or
// a byte that is not UTF-8.
func terminalSafe(s string) error {
	s = sgr.ReplaceAllString(s, "")
	for i, r := range s {
		bad := r < 0x20 && r != '\n' || r == 0x7f || 0x80 <= r && r <= 0x9f ||
			0x202a <= r && r <= 0x202e || 0x2066 <= r && r <= 0x2069
		if _, size := utf8.DecodeRuneInString(s[i:]); r == utf8.RuneError && size == 1 {
			bad = true
		}
		if bad {
			return fmt.Errorf("%U at byte %d, in %q", r, i, s[max(i-30, 0):min(i+30, len(s))])
		}
	}
	return nil
}

func TestShownEscapesControls(t *testing.T) {
	for _, c := range []struct{ in, line, screen string }{
		{"plain name.txt", "plain name.txt", "plain name.txt"},
		{"café ✓ 日本.txt", "café ✓ 日本.txt", "café ✓ 日本.txt"},
		{"evil\x1b]0;pwned\x07.txt", "evil^[]0;pwned^G.txt", "evil^[]0;pwned^G.txt"},
		{"clip\x1b]52;c;cHduZWQ=\x1b\\", "clip^[]52;c;cHduZWQ=^[\\", "clip^[]52;c;cHduZWQ=^[\\"},
		{"\x1b[2Jcls", "^[[2Jcls", "^[[2Jcls"},
		{"two\nlines\r", "two^Jlines^M", "two\nlines^M"},
		{"tab\tdel\x7fnul\x00", "tab^Idel^?nul^@", "tab^Idel^?nul^@"},
		{"c1\u009b2J", "c1<U+009B>2J", "c1<U+009B>2J"},
		{"bidi\u202etxt.exe\u2066", "bidi<U+202E>txt.exe<U+2066>", "bidi<U+202E>txt.exe<U+2066>"},
		{"latin\xe9t\xe9", "latin<E9>t<E9>", "latin<E9>t<E9>"},
		// The colours the UI draws itself stay on a screen, not in a line.
		{"\x1b[1;38;5;243mdim\x1b[0m", "^[[1;38;5;243mdim^[[0m", "\x1b[1;38;5;243mdim\x1b[0m"},
	} {
		if got := shown(c.in); got != c.line {
			t.Errorf("shown(%q) = %q, want %q", c.in, got, c.line)
		}
		if got := shownScreen(c.in); got != c.screen {
			t.Errorf("shownScreen(%q) = %q, want %q", c.in, got, c.screen)
		}
		if err := terminalSafe(shown(c.in)); err != nil {
			t.Errorf("shown(%q) acts on a terminal: %v", c.in, err)
		}
	}
}

// hostile names, as a server can send them, and how they are drawn.
var hostile = map[string]string{
	"evil\x1b]0;pwned\x07.txt": "evil^[]0;pwned^G.txt",
	"\x1b[2Jcls":               "^[[2Jcls",
	"two\nlines":               "two^Jlines",
	"cr\rover":                 "cr^Mover",
	"c1\u009b2J":               "c1<U+009B>2J",
	"bidi\u202etxt.exe":        "bidi<U+202E>txt.exe",
	"latin\xe9":                "latin<E9>",
}

// U8: names, log lines and file content went to the terminal as they came.
// A name with an escape sequence could set the window title, clear the
// screen or write the clipboard; \r and \n broke the rows. Now every such
// byte is drawn as text: in the panels, the dialogs, the log and the diff.
func TestHostileNamesAreDrawnAsText(t *testing.T) {
	ctx := context.Background()
	src, err := transport.NewFakeBackend("fake://src?dirs=0&files=0")
	if err != nil {
		t.Fatal(err)
	}
	for name := range hostile {
		if err := src.CopyFrom(ctx, name, strings.NewReader("x"), model.PutParams{Size: 1, Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	dst, err := transport.NewFakeBackend("fake://dst?dirs=0&files=0")
	if err != nil {
		t.Fatal(err)
	}
	m := newTestModel(t, src, dst)
	m.scanner.Scan(ctx, *m.cmpOpts)
	m.refreshTreeNow()

	screen := m.View()
	if err := terminalSafe(screen); err != nil {
		t.Errorf("the panels: %v", err)
	}
	for name, drawn := range hostile {
		if !strings.Contains(screen, drawn) {
			t.Errorf("the panels do not show %q as %q", name, drawn)
		}
	}

	transport.Log.Add("copy", transport.DirErr, "COPY evil\x1b]0;pwned\x07.txt: refused")
	press(m, "~")
	if log := m.View(); terminalSafe(log) != nil || !strings.Contains(log, "COPY evil^[]0;pwned^G.txt: refused") {
		t.Errorf("the log: %v\n%s", terminalSafe(log), log)
	}
	press(m, "esc")

	// A dialog draws the name as one line: the frame's guard keeps line
	// breaks, so a \n in a name broke the dialog's rows.
	cursorTo(t, m, "two\nlines", false)
	press(m, "d")
	if dlg := m.View(); terminalSafe(dlg) != nil || !strings.Contains(dlg, "Delete two^Jlines?") {
		t.Errorf("the delete dialog: %v\n%s", terminalSafe(dlg), dlg)
	}
	press(m, "esc")
	press(m, "e")
	if dlg := m.View(); terminalSafe(dlg) != nil || !strings.Contains(dlg, "Rename: two^Jlines") || !strings.Contains(dlg, "two^Jline") {
		t.Errorf("the rename dialog: %v\n%s", terminalSafe(dlg), dlg)
	}
	press(m, "esc")
}

// U8, file content: the diff compares the bytes as they are and draws
// them as text.
func TestDiffDrawsControlsAsText(t *testing.T) {
	lines, _ := buildTextDiff([]byte("same\x1b[8m\nleft \x1b]0;pwned\x07\n"), []byte("same\x1b[8m\nright \x1b[2J\r\n"))
	var all []string
	for _, l := range lines {
		all = append(all, l.leftStyled, l.rightStyled)
	}
	text := strings.Join(all, "\n")
	if err := terminalSafe(text); err != nil {
		t.Errorf("the diff: %v", err)
	}
	for _, want := range []string{"same^[[8m", "left ^[]0;pwned^G", "right ^[[2J^M"} {
		if !strings.Contains(sgr.ReplaceAllString(text, ""), want) {
			t.Errorf("the diff does not show %q:\n%q", want, text)
		}
	}
}

// The last guard: text that reaches the screen without being escaped where
// it is drawn (here a settings label, which is never data) is escaped on
// the way out all the same.
func TestScreenGuardCatchesMissedText(t *testing.T) {
	m := scannedModel(t, "fake://tiny", "fake://tiny")
	on := true
	m.settings.SetOptions([]Option{{Label: "missed\x1b]0;pwned\x07", Value: &on}})
	press(m, "=")
	if s := m.View(); terminalSafe(s) != nil || !strings.Contains(s, "missed^[]0;pwned^G") {
		t.Errorf("the settings dialog: %v\n%s", terminalSafe(s), s)
	}
}
