//go:build unix

package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"sc/model"
)

// localLinks: a local disk takes links (LocalBackend.Symlink), and so does
// an rsync daemon, sent one made here (RsyncBackend.Symlink).
const localLinks = true

var (
	_ model.Linker = (*LocalBackend)(nil)
	_ model.Linker = (*RsyncBackend)(nil)
)

// Symlink makes relPath a link to target, the text as it is
// (model.Linker). A link there is replaced; a file or a folder there is
// refused: the copy removes such a twin itself, and only with the user's
// yes.
func (b *LocalBackend) Symlink(_ context.Context, target, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("local: refusing to make the base directory a link")
	}
	p, err := b.path(relPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	if fi, err := os.Lstat(p); err == nil {
		if fi.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("local: %s is a %s, not replaced with a link", p, model.KindName(fi.Mode()))
		}
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return os.Symlink(target, p)
}

// Symlink makes relPath a link to target on the daemon (model.Linker). The
// daemon protocol has no request for it: the link, made here under its
// full path in a temp folder, is sent with -l, and -r makes the folders
// above it, as for a file (stageUploadPath). rsync puts the link where a
// file or an empty folder is: a Stat first refuses anything there that is
// no link, as checkDirs refuses a file where a folder goes (R21).
func (b *RsyncBackend) Symlink(ctx context.Context, target, relPath string) error {
	if isBaseRel(relPath) {
		return errors.New("rsync: refusing to make the base directory a link")
	}
	if _, err := b.checkDirs(ctx, parentDir(relPath)); err != nil {
		return err
	}
	switch e, err := b.Stat(ctx, relPath); {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return err
	case !e.IsLink():
		return fmt.Errorf("rsync: %s is a %s on the daemon, not replaced with a link", relPath, e.Kind())
	}
	tmpDir, err := tempDir("rsync-link-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	stageTop, leaf, err := stageUploadPath(tmpDir, relPath)
	if err != nil {
		return err
	}
	if err := os.Symlink(target, leaf); err != nil {
		return err
	}
	dest, err := b.pushTarget(ctx, "")
	if err != nil {
		return err
	}
	Log.Add("rsync", DirOut, "SYMLINK "+relPath+" -> "+target)
	err = b.transfer(ctx, []string{"-r", "-l", "-O"}, dest+"/", []string{stageTop}, true, nil)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("rsync", DirErr, "SYMLINK "+relPath+": "+err.Error())
		return err
	}
	b.sawDirs(parentDir(relPath))
	return nil
}
