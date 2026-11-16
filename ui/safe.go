package ui

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Text from a server or a file is shown, never obeyed (U8). A name, a log
// line, a checksum or a file's content can hold bytes a terminal takes as
// commands: an escape sequence sets the window title, clears the screen or
// writes the clipboard, and \r and \n break the rows. Such bytes are drawn
// as text, as less draws them: ^[ for ESC and the other C0 controls, ^?
// for DEL, <U+009B> for the C1 controls and for the bidi controls that
// reorder a line, <E9> for a byte that is not UTF-8.

// shown is s as one line of text: every control in it escaped. Each place
// that draws a string from a backend or a file draws shown(s).
func shown(s string) string { return escapeControls(s, false) }

// shownScreen is the last guard on a whole screen, for text a draw site
// missed: it keeps the line breaks and the colours the UI draws itself and
// escapes anything else that would act on the terminal.
func shownScreen(s string) string { return escapeControls(s, true) }

func escapeControls(s string, screen bool) string {
	var b *strings.Builder // made at the first escape: clean text is returned as it is
	start := 0             // s[start:i] is clean and not written yet
	escape := func(i int, with string) {
		if b == nil {
			b = &strings.Builder{}
			b.Grow(len(s) + 16)
		}
		b.WriteString(s[start:i])
		b.WriteString(with)
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case screen && c == '\n':
			i++
		case screen && c == 0x1b && sgrLen(s[i:]) > 0:
			i += sgrLen(s[i:])
		case c < 0x20:
			escape(i, "^"+string(rune(c+'@')))
			i++
			start = i
		case c == 0x7f:
			escape(i, "^?")
			i++
			start = i
		case c < utf8.RuneSelf:
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			switch {
			case r == utf8.RuneError && size == 1:
				escape(i, fmt.Sprintf("<%02X>", c))
			case 0x80 <= r && r <= 0x9f, 0x202a <= r && r <= 0x202e, 0x2066 <= r && r <= 0x2069:
				escape(i, fmt.Sprintf("<U+%04X>", r))
			default:
				i += size
				continue
			}
			i += size
			start = i
		}
	}
	if b == nil {
		return s
	}
	b.WriteString(s[start:])
	return b.String()
}

// sgrLen is the length of the colour sequence (CSI, digits and separators,
// m) that s starts with; 0 when s starts with anything else.
func sgrLen(s string) int {
	if len(s) < 3 || s[0] != 0x1b || s[1] != '[' {
		return 0
	}
	for i := 2; i < len(s); i++ {
		switch c := s[i]; {
		case c == 'm':
			return i + 1
		case '0' <= c && c <= '9', c == ';', c == ':':
		default:
			return 0
		}
	}
	return 0
}
