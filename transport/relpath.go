package transport

import (
	"path"
	"strings"
)

// cleanRel is relPath as a clean path under a backend's base: no leading or
// trailing slash, no "." part, and a ".." part that would climb above the
// base stops at it; "" is the base itself. Every backend joins its base with
// cleanRel's path and tests for the base with it (isBaseRel), so the test
// and the join agree and no relPath leaves the base (L7).
func cleanRel(relPath string) string {
	return strings.TrimPrefix(path.Clean("/"+relPath), "/")
}

// isBaseRel reports whether relPath names the backend base itself.
func isBaseRel(relPath string) bool { return cleanRel(relPath) == "" }
