package model

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"
	"strings"
	"unicode/utf8"
)

// A copy is written into a part file next to the real one and renamed over
// it only once complete and checked, so the real name never holds half a
// copy (review L14). The part is ".<name>.sc-part", the name sc/1 uses for
// its upload parts (PROTOCOLS.md Part 2); sc/1 keeps a have-list beside one
// as ".<name>.sc-part.have".
const (
	partSuffix  = ".sc-part"
	haveSuffix  = ".have"
	maxPartName = 255 // NAME_MAX of most file systems, in bytes
)

// PartName names the part file a copy of name is written into. A name too
// long for the prefix and suffix keeps its start and gets a hash of the
// whole name, so it is still one part name per file name.
func PartName(name string) string {
	if p := "." + name + partSuffix; len(p) <= maxPartName {
		return p
	}
	sum := sha256.Sum256([]byte(name))
	tag := "~" + hex.EncodeToString(sum[:8])
	keep := maxPartName - len(".") - len(tag) - len(partSuffix)
	for keep > 0 && !utf8.RuneStart(name[keep]) {
		keep--
	}
	return "." + name[:keep] + tag + partSuffix
}

// PartPath is PartName for a path: the part sits in the file's own folder,
// so the rename that puts it in place never crosses a file system.
func PartPath(relPath string) string {
	return path.Join(DirOf(relPath), PartName(path.Base(relPath)))
}

// IsPartName reports whether name is a part file sc writes a copy into, or
// the have-list sc/1 keeps beside one. Such names belong to sc: they are
// listed but never copied.
func IsPartName(name string) bool {
	name = strings.TrimSuffix(name, haveSuffix)
	return len(name) > len("."+partSuffix) && name[0] == '.' && strings.HasSuffix(name, partSuffix)
}

// PartMode is the mode a part file is written with: the file's own, made
// readable and writable by its owner, so a copy cut off half way can still
// be checked and resumed. CommitPart gives the file its real mode.
func PartMode(mode os.FileMode) os.FileMode { return mode.Perm() | 0600 }
