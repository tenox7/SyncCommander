package transport

import (
	"errors"
	"testing"
)

// Names may hold tabs and newlines; the record puts the name last and ends in
// NUL so nothing inside it can be mistaken for a separator.
func TestParseFindRecordKeepsTabsAndNewlinesInNames(t *testing.T) {
	name, e, ok := parseFindRecord("12\t1700000000.5\t1700000001\t1700000002\t644\tf\tweird\tname\nhere")
	if !ok || name != "weird\tname\nhere" {
		t.Fatalf("name = %q ok=%v", name, ok)
	}
	if e.Size != 12 || e.IsDir || e.Mode != 0o644 || e.ModTime.Unix() != 1700000000 || e.ModTime.Nanosecond() != 500000000 {
		t.Errorf("entry = %+v", e)
	}
	if _, _, ok := parseFindRecord("1\t2\t3\t4\t777\tl\tlink"); ok {
		t.Error("symlink record must be skipped")
	}
	if _, d, ok := parseFindRecord("0\t2\t3\t4\t755\td\tdir"); !ok || !d.IsDir || !d.Mode.IsDir() {
		t.Errorf("dir record = %+v ok=%v", d, ok)
	}
}

func TestExpandHome(t *testing.T) {
	home := func() (string, error) { return "/home/u", nil }
	broken := func() (string, error) { return "", errors.New("no shell") }
	for _, tc := range []struct {
		in, want string
		home     func() (string, error)
	}{
		{"", "/home/u", home}, {"/", "/home/u", home}, {"/~", "/home/u", home},
		{"/~/x/y", "/home/u/x/y", home}, {"/srv", "/srv", home},
		{"/", "/", broken}, {"/~/x", "/~/x", broken},
	} {
		if got := expandHome(tc.in, tc.home); got != tc.want {
			t.Errorf("expandHome(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestShellQuoteAndTruncateCmd(t *testing.T) {
	if got := shellQuote("it's"); got != `'it'\''s'` {
		t.Errorf("shellQuote = %s", got)
	}
	if got := truncateCmd("/a b", 42); got != "dd if=/dev/null of='/a b' bs=1 seek=42 2>/dev/null" {
		t.Errorf("truncateCmd = %s", got)
	}
}
