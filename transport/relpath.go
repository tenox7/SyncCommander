package transport

import (
	"fmt"
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

// renameBase refuses a rename from or onto the base itself, which no
// caller means: the UI asks for none (U11). Asked for one, rclone moved
// every object of a remote without folder moves into the new name and
// joined the paths wrongly ("d/c.txt" became "xd/c.txt").
func renameBase(proto, oldRelPath, newRelPath string) error {
	if isBaseRel(oldRelPath) || isBaseRel(newRelPath) {
		return fmt.Errorf("%s: refusing to rename the base directory", proto)
	}
	return nil
}
