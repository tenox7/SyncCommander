package transport

import (
	"strings"
	"testing"
)

func TestModulePath(t *testing.T) {
	b, err := NewRsyncBackend("rsync://u:p%40ss@host:8730/mod/base dir")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.modulePath("a#1/b?c/100%.txt", false); got != `mod/base dir/a#1/b\?c/100%.txt` || err != nil {
		t.Errorf("modulePath = %q, %v", got, err)
	}
	if got, err := b.modulePath("x", true); got != "mod/base dir/x/" || err != nil {
		t.Errorf("modulePath = %q, %v", got, err)
	}
}

// R1: a stock daemon matches each part of a path that holds *, ? or [ as a
// pattern, and splits the path at " <module>/". daemonPath writes such a
// part as a pattern only its own name matches, with no space in it.
func TestDaemonPathIsOnePath(t *testing.T) {
	for in, want := range map[string]string{
		"":                  "",
		"a/b c/d.txt":       "a/b c/d.txt",
		`back\slash/x`:      `back\slash/x`,
		"s2/d[1]":           `s2/d\[1\]`,
		"a*b/q?":            `a\*b/q\?`,
		`x\[y]`:             `x\\\[y\]`,
		"Movie [1080p].mkv": `Movie[ ]\[1080p\].mkv`,
		"old testmod/x.txt": "old[ ]testmod/x.txt",
		"a/my old testmod":  "a/my[ ]old[ ]testmod",
		" testmod/y":        "[ ]testmod/y",
		"old testmodx/y":    "old testmodx/y",
		"oldtestmod/y":      "oldtestmod/y",
	} {
		if got, err := daemonPath("testmod", in); got != want || err != nil {
			t.Errorf("daemonPath(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"a\nb", "a/b\r", "\n"} {
		if got, err := daemonPath("testmod", in); err == nil {
			t.Errorf("daemonPath(%q) = %q, want an error", in, got)
		}
	}
	b, err := NewRsyncBackend("rsync://host/testmod")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.modulePath(strings.Repeat("[", 1100), false); err == nil {
		t.Error("modulePath took a pattern longer than a daemon reads")
	}
	if _, err := b.modulePath(strings.Repeat("a", 3000), false); err != nil {
		t.Errorf("modulePath refused a long plain name: %v", err)
	}
	if _, err := NewRsyncBackend("rsync://host/testmod/a\nb"); err == nil {
		t.Error("NewRsyncBackend took a base with a line break")
	}
}

func TestDaemonProtocolGreeting(t *testing.T) {
	for in, want := range map[string]int{
		"@RSYNCD: 27": 27,
		"@RSYNCD: 31.0 sha512 sha256 sha1 md5 md4": 31,
	} {
		if got, err := daemonProtocol(in); err != nil || got != want {
			t.Errorf("daemonProtocol(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "hello", "@RSYNCD: x"} {
		if _, err := daemonProtocol(in); err == nil {
			t.Errorf("daemonProtocol(%q) accepted", in)
		}
	}
}
