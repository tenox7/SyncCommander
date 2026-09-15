package transport

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	rsyncpkg "github.com/gokrazy/rsync"
	"github.com/gokrazy/rsync/rsyncclient"

	"sc/model"
)

// rsyncTransferFlags are passed to every rsync invocation that moves file
// data. -t preserves mtime; --inplace writes directly to the destination
// (resume keeps the partial body in place); --partial keeps interrupted
// files for the next run; -W disables the delta-sync algorithm so the sender
// streams the whole file. -W is intentional: gokrazy/rsync's hashSearch path
// (internal/sender/fileio.go) misclassifies read errors as "file has changed
// mid-transfer", and on this workload delta-sync saves no bandwidth anyway
// (mostly immutable archives). --ignore-times bypasses rsync's quick-check
// (skip when size+mtime match): SC only invokes these calls on user-initiated
// copies, so we must transfer unconditionally, otherwise a same-size,
// same-mtime, different-content file is silently skipped.
var rsyncTransferFlags = []string{"-t", "--inplace", "--partial", "-W", "--ignore-times"}

func transferFlags() []string { return slices.Clone(rsyncTransferFlags) }

// md4ListFlags asks the sender for a dry run that still computes every MD4
// (-c) and ships it in the file list; without -n rsync would download each
// body only to discard it.
func md4ListFlags(recursive bool) []string {
	flags := []string{"-c", "-n"}
	if recursive {
		flags = append(flags, "-r")
	}
	return flags
}

// newRsyncClient builds an in-process rsync client with its console output
// silenced; the library would otherwise write into the TUI.
func newRsyncClient(flags []string, opts ...rsyncclient.Option) (*rsyncclient.Client, error) {
	base := []rsyncclient.Option{rsyncclient.WithStdout(io.Discard), rsyncclient.WithStderr(io.Discard)}
	return rsyncclient.New(flags, append(base, opts...)...)
}

// countingRW wraps a session stream so bytes written credit adder; nil when
// there is no progress to report.
func countingRW(adder *CappedAdder) func(io.ReadWriter) io.ReadWriter {
	if adder == nil {
		return nil
	}
	return func(rw io.ReadWriter) io.ReadWriter { return &CountingReadWriter{RW: rw, Adder: adder} }
}

// md4Cache holds the MD4 sums an rsync file list delivers in bulk. It has no
// TTL, so every write that changes a file's body must drop its entry.
type md4Cache struct {
	mu sync.Mutex
	m  map[string]string
}

func (c *md4Cache) get(relPath string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sum, ok := c.m[relPath]
	return sum, ok
}

func (c *md4Cache) invalidate(relPath string) {
	c.mu.Lock()
	delete(c.m, relPath)
	c.mu.Unlock()
}

func (c *md4Cache) invalidateTree(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.m {
		if k == prefix || strings.HasPrefix(k, prefix+"/") {
			delete(c.m, k)
		}
	}
}

// lookup serves Checksum for md4: the cache first, then one fetch scoped to
// the file.
func (c *md4Cache) lookup(ctx context.Context, relPath string, fetch func(context.Context, string, bool) (map[string]string, error)) (string, error) {
	if sum, ok := c.get(relPath); ok {
		return sum, nil
	}
	got, err := fetch(ctx, relPath, false)
	if err != nil {
		return "", err
	}
	sum, ok := got[relPath]
	if !ok {
		return "", fmt.Errorf("md4: no checksum for %s", relPath)
	}
	return sum, nil
}

// fromFileList stores the non-zero checksums of a dry-run file list keyed by
// path relative to the backend base and returns them.
func (c *md4Cache) fromFileList(scope string, recursive bool, list []rsyncpkg.FileInfo) map[string]string {
	prefix := ""
	if recursive {
		prefix = scope
	} else if scope != "" {
		prefix = parentDir(scope)
	}
	got := make(map[string]string, len(list))
	var zero [16]byte
	for _, fi := range list {
		if fi.Checksum != zero {
			got[path.Join(prefix, fi.Name)] = hex.EncodeToString(fi.Checksum[:])
		}
	}
	c.mu.Lock()
	if c.m == nil {
		c.m = make(map[string]string, len(got))
	}
	for k, v := range got {
		c.m[k] = v
	}
	c.mu.Unlock()
	return got
}

func rsyncModeToFileMode(m int32) os.FileMode {
	mode := os.FileMode(m) & os.ModePerm
	switch m & rsyncpkg.S_IFMT {
	case rsyncpkg.S_IFDIR:
		mode |= os.ModeDir
	case rsyncpkg.S_IFLNK:
		mode |= os.ModeSymlink
	}
	return mode
}

// fileInfoEntry converts one dry-run file list item to an entry under scope;
// false for the "." self entry and for symlinks.
func fileInfoEntry(scope string, fi rsyncpkg.FileInfo) (model.FileEntry, bool) {
	if fi.Name == "" || fi.Name == "." || fi.Mode&rsyncpkg.S_IFMT == rsyncpkg.S_IFLNK {
		return model.FileEntry{}, false
	}
	mode := rsyncModeToFileMode(fi.Mode)
	return model.FileEntry{
		RelPath: path.Join(scope, fi.Name),
		Name:    path.Base(fi.Name),
		Size:    fi.Length,
		ModTime: fi.ModTime,
		IsDir:   mode.IsDir(),
		Mode:    mode,
	}, true
}

// fileListEntries converts a one-level dry-run file list into relDir's children.
func fileListEntries(relDir string, list []rsyncpkg.FileInfo) []model.FileEntry {
	entries := make([]model.FileEntry, 0, len(list))
	for _, fi := range list {
		if e, ok := fileInfoEntry(relDir, fi); ok {
			entries = append(entries, e)
		}
	}
	return entries
}

// emitFileList feeds a recursive dry-run file list into the list cache.
func emitFileList(scope string, list []rsyncpkg.FileInfo, emit func(string, []model.FileEntry)) {
	g := &emitGrouper{emit: emit}
	for _, fi := range list {
		if e, ok := fileInfoEntry(scope, fi); ok {
			g.add(e)
		}
	}
	g.finish()
}

// tailProgress credits growth of dir/base to the context's progress counter
// until the returned stop runs. Without a counter it is a no-op.
func tailProgress(ctx context.Context, dir, base string) func() {
	counter := progressFromContext(ctx)
	if counter == nil {
		return func() {}
	}
	size, ok := fileSizeFromContext(ctx)
	if !ok || size <= 0 {
		size = 1 << 62
	}
	var baseAdder *CappedAdder
	if bp := baseProgressFromContext(ctx); bp != nil {
		baseAdder = NewCappedAdder(bp, size)
	}
	stop := make(chan struct{})
	go tailDirSize(stop, dir, base, NewCappedAdder(counter, size), baseAdder)
	return func() { close(stop) }
}

// rsyncOpenViaTemp receives relPath into a fresh temp dir through recv and
// returns the file, which removes the dir on Close. With a progress counter
// the reader is marked pre-counted since the tail already credited the body.
func rsyncOpenViaTemp(ctx context.Context, relPath string, recv func(dstDir string) error) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-dl-*")
	if err != nil {
		return nil, err
	}
	stop := tailProgress(ctx, tmpDir, filepath.Base(relPath))
	err = recv(tmpDir + "/")
	stop()
	var f *os.File
	if err == nil {
		f, err = os.Open(filepath.Join(tmpDir, filepath.Base(relPath)))
	}
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	rc := &tempReadCloser{File: f, tmpDir: tmpDir}
	if progressFromContext(ctx) != nil {
		return WrapPreCounted(rc), nil
	}
	return rc, nil
}

type tempReadCloser struct {
	*os.File
	tmpDir string
}

func (t *tempReadCloser) Close() error {
	t.File.Close()
	return os.RemoveAll(t.tmpDir)
}

// rsyncRecvToLocal receives relPath straight into dstPath's directory through
// recv and verifies a regular file landed there.
func rsyncRecvToLocal(ctx context.Context, proto, relPath, dstPath string, recv func(dstDir string) error) error {
	parent := filepath.Dir(dstPath)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	if fi, err := os.Stat(dstPath); err == nil && fi.IsDir() {
		return fmt.Errorf("%s: refuse to receive into existing directory %s", proto, dstPath)
	}
	stop := tailProgress(ctx, parent, filepath.Base(dstPath))
	err := recv(parent + "/")
	stop()
	if err != nil {
		return err
	}
	fi, err := os.Stat(dstPath)
	switch {
	case err != nil:
		Log.Add(proto, DirErr, "RECV "+relPath+": dst missing after rsync: "+err.Error())
		return fmt.Errorf("%s: dst missing after recv: %w", proto, err)
	case fi.IsDir():
		Log.Add(proto, DirErr, "RECV "+relPath+": dst is a directory after rsync")
		return fmt.Errorf("%s: dst became a directory after recv: %s", proto, dstPath)
	}
	Log.Add(proto, DirIn, fmt.Sprintf("RECV %s OK (%d bytes)", relPath, fi.Size()))
	return nil
}

// stageUpload spools src into tmpFile so rsync can send from disk, crediting
// half the progress budget for the read (the push credits the rest). Returns
// the push-phase adder and its budget, nil without progress.
func stageUpload(ctx context.Context, src io.Reader, tmpFile string, mode os.FileMode) (*CappedAdder, int64, error) {
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return nil, 0, err
	}
	counter := progressFromContext(ctx)
	fileSize, _ := fileSizeFromContext(ctx)
	var readAdder, pushAdder *CappedAdder
	var pushBudget int64
	if counter != nil && fileSize > 0 && !IsPreCounted(src) {
		readBudget := fileSize / 2
		pushBudget = fileSize - readBudget
		readAdder = NewCappedAdder(counter, readBudget)
		pushAdder = NewCappedAdder(counter, pushBudget)
	}
	var dst io.Writer = f
	if readAdder != nil {
		dst = &CountingWriter{W: f, Adder: readAdder}
	}
	_, err = io.Copy(dst, src)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		if readAdder != nil {
			counter.Add(-readAdder.Used())
		}
		return nil, 0, err
	}
	return pushAdder, pushBudget, nil
}

// spoolTo writes the whole source into path so rsync can send it from disk.
func spoolTo(ctx context.Context, src model.RangeOpener, path string, mode os.FileMode) error {
	rd, err := src.Open(ctx)
	if err != nil {
		return err
	}
	defer rd.Close()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, rd)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// settlePush reconciles the push-phase credit: rolled back on error, topped
// up to budget on success (delta-sync may have sent less than the body).
func settlePush(ctx context.Context, adder *CappedAdder, budget int64, err error) {
	if adder == nil {
		return
	}
	if err != nil {
		progressFromContext(ctx).Add(-adder.Used())
		return
	}
	adder.Add(budget)
}
