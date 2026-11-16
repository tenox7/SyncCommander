package model

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// NameRule is how the file system under a root matches names (D10). The
// tree matches names byte for byte, but a file system that folds letter
// case or Unicode forms takes two of those names for one: a copy of both
// writes one file twice, the second over the first. What a backend cannot
// tell is FoldUnknown, as for a remote until its server says (sc/1
// names=), and then nothing is assumed either way.
type NameRule struct {
	Case  Fold // A.txt and a.txt
	Forms Fold // é as one code point, or as e and a combining accent
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
