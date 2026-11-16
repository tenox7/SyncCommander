package transport

import (
	"context"
	"fmt"
	"io/fs"
	"path"

	"sc/model"
)

// A rename never replaces an existing name: most protocols would delete the
// target silently (WebDAV a whole folder). The UI checks the tree first, and
// each backend checks again because the tree can be hours old.

// renameTargetTaken is the error for a rename onto a name in use. It wraps
// fs.ErrExist, which Retry treats as permanent.
func renameTargetTaken(proto, relPath string) error {
	return fmt.Errorf("%s: rename: %s already exists: %w", proto, relPath, fs.ErrExist)
}

// nameTaken lists relPath's folder and reports whether its name is in use,
// for protocols with no rename that refuses an existing target.
func nameTaken(ctx context.Context, list func(context.Context, string) ([]model.FileEntry, error), relPath string) (bool, error) {
	entries, err := list(ctx, parentDir(relPath))
	if err != nil {
		return false, err
	}
	name := path.Base(relPath)
	for _, e := range entries {
		if e.Name == name {
			return true, nil
		}
	}
	return false, nil
}
