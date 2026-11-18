package model

import (
	"strings"
	"testing"
)

// L25: names Windows cannot hold as they are, and why. Each refused name
// landed on another file there, or on none, or failed with no reason; the
// names held are their near misses.
func TestWindowsName(t *testing.T) {
	for _, c := range []struct {
		name, why string // why: a part of the reason; "" for a name Windows holds
	}{
		{`a\b`, `\ separates folders`},
		{`x:y`, ": names a stream"},
		{"a*b", "no *"},
		{"a?b", "no ?"},
		{`a"b`, `no "`},
		{"a<b", "no <"},
		{"a>b", "no >"},
		{"a|b", "no |"},
		{"a\x01b", "control characters"},
		{"tab\there", "control characters"},
		{"a.", "trailing dot or space"},
		{"a ", "trailing dot or space"},
		{"...", "trailing dot or space"},
		{"NUL", "NUL is a device"},
		{"nul", "NUL is a device"},
		{"Nul.txt", "NUL is a device"},
		{"NUL .txt", "NUL is a device"},
		{"con", "CON is a device"},
		{"PRN", "PRN is a device"},
		{"aux.tar.gz", "AUX is a device"},
		{"COM1", "COM1 is a device"},
		{"com9.log", "COM9 is a device"},
		{"COM0", "COM0 is a device"},
		{"lpt1", "LPT1 is a device"},
		{"LPT¹", "LPT¹ is a device"},
		{"com².txt", "COM² is a device"},
		{"CONIN$", "CONIN$ is a device"},
		{"conout$.x", "CONOUT$ is a device"},

		{"a.b", ""},
		{".hidden", ""},
		{" leading", ""},
		{"a b", ""},
		{"café", ""},
		{"con2", ""},
		{"nul_", ""},
		{"NULL", ""},
		{"auxiliary", ""},
		{"console.log", ""},
		{"COM10", ""},
		{"COM", ""},
		{"LPTX", ""},
		{"a#b%c", ""},
	} {
		got := WindowsName(c.name)
		switch {
		case c.why == "" && got != "":
			t.Errorf("%q: refused (%s); Windows holds it", c.name, got)
		case c.why != "" && got == "":
			t.Errorf("%q: held; Windows cannot hold it (%s)", c.name, c.why)
		case c.why != "" && !strings.Contains(got, c.why):
			t.Errorf("%q: refused as %q; want a reason with %q", c.name, got, c.why)
		}
		if r := (NameRule{Case: Folded, Forms: Folded}).Refuses(c.name); r != "" {
			t.Errorf("%q: refused (%s) by a rule that is not Windows'", c.name, r)
		}
		if r := (NameRule{Windows: true}).Refuses(c.name); r != got {
			t.Errorf("%q: the Windows rule says %q, WindowsName %q", c.name, r, got)
		}
	}
}
