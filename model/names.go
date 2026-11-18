package model

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// NameRule is how the file system under a root matches names (D10), and
// which names it cannot hold. The tree matches names byte for byte, but a
// file system that folds letter case or Unicode forms takes two of those
// names for one: a copy of both writes one file twice, the second over the
// first. What a backend cannot tell is FoldUnknown, as for a remote until
// its server says (sc/1 names=), and then nothing is assumed either way.
type NameRule struct {
	Case  Fold // A.txt and a.txt
	Forms Fold // é as one code point, or as e and a combining accent
	// Windows is set for a root reached through Windows, which cannot hold
	// some names as they are (WindowsName).
	Windows bool
}

// Refuses says why the file system of the rule cannot hold name, or "".
func (r NameRule) Refuses(name string) string {
	if r.Windows {
		return WindowsName(name)
	}
	return ""
}

// WindowsName says why Windows cannot hold name as it is, or "". Some of
// these names land on another file there, or on none, and a copy into one
// said OK (L25): \ separates folders, so a\b was the file b in a folder a;
// a : names a stream, so x:y went into a hidden part of the file x; a
// trailing dot or space is dropped, so "a." was the file a; and NUL, CON,
// COM1 and the other device names, with an extension or without, were the
// device. The other characters Windows refuses fail anyway: they fail
// here, with the reason. Git for Windows refuses the same names
// (core.protectNTFS).
func WindowsName(name string) string {
	for _, r := range name {
		switch {
		case r == '\\':
			return `\ separates folders on Windows`
		case r == ':':
			return ": names a stream of a file on Windows"
		case r < ' ':
			return "Windows takes no control characters in a name"
		case strings.ContainsRune(`*?"<>|`, r):
			return "Windows takes no " + string(r) + " in a name"
		}
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return "Windows drops a trailing dot or space"
	}
	if dev := windowsDevice(name); dev != "" {
		return dev + " is a device on Windows"
	}
	return ""
}

// windowsDevice returns the device name is to Windows, or "". What follows
// a dot does not count, nor spaces before it: nul.txt and "NUL .txt" are
// NUL. A superscript ¹, ² or ³ is a digit there.
func windowsDevice(name string) string {
	base, _, _ := strings.Cut(name, ".")
	base = strings.ToUpper(strings.TrimRight(base, " "))
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return base
	}
	if len(base) > 3 && (base[:3] == "COM" || base[:3] == "LPT") {
		switch base[3:] {
		case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³":
			return base
		}
	}
	return ""
}

// Fold is whether names that differ in one way are one name.
type Fold uint8

const (
	FoldUnknown Fold = iota // the backend cannot tell
	Apart                   // two names
	Folded                  // one name
)

// Folds reports whether the rule takes some different names for one.
func (r NameRule) Folds() bool { return r.Case == Folded || r.Forms == Folded }

// Key is what the file system matches name by: names with the same Key
// are one name there.
func (r NameRule) Key(name string) string {
	if r.Forms == Folded {
		name = norm.NFD.String(name)
	}
	if r.Case == Folded {
		name = strings.Map(foldRune, name)
	}
	return name
}

// foldRune is the least rune that folds with r, so two names compare as
// strings.EqualFold compares them.
func foldRune(r rune) rune {
	least := r
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		least = min(least, f)
	}
	return least
}
