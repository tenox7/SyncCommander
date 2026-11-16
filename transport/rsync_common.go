package transport

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

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
	base := []rsyncclient.Option{rsyncclient.WithStdout(io.Discard), rsyncclient.WithStderr(nopWriteCloser{io.Discard})}
	return rsyncclient.New(flags, append(base, opts...)...)
}

// nopWriteCloser lets a plain writer stand in where gorsync wants a closer it
// never closes.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// countingRW wraps a session stream so bytes written credit adder; nil when
// there is no progress to report.
func countingRW(adder *CappedAdder) func(io.ReadWriteCloser) io.ReadWriteCloser {
	if adder == nil {
		return nil
	}
	return func(rw io.ReadWriteCloser) io.ReadWriteCloser { return &CountingReadWriter{RW: rw, Adder: adder} }
}

// appendVerify reports whether flags ask the receiver to verify an appended
// prefix. rsync keeps a failed verification as a per-file error that the
// run as a whole survives; sc wants it to fail the append so the engine
// recopies in full, hence the strict check at the call sites.
func appendVerify(flags []string) bool { return slices.Contains(flags, "--append-verify") }

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

// fileListStream feeds a recursive dry run into the list cache entry by
// entry, as the file list arrives. Stock rsync sends each directory's
// children as one run and descends afterwards, gorsync's sender walks in
// pre-order; both keep a subtree contiguous, so a dir is complete once an
// entry outside it arrives, but only its first child (lateChildren) proves
// the run has begun. add goes to rsyncclient.WithFileListCallback; finish
// runs after Run returns. The stream holds back releases while hold reports
// true; nil never holds.
func fileListStream(scope string, emit func(string, []model.FileEntry), complete func(string), hold func() bool) (add func(rsyncpkg.FileInfo), finish func()) {
	l := newRsyncListing(scope, emit, complete, nil)
	l.g.hold = hold
	return l.add, l.g.finish
}

// ioErrGeneral is rsync's IOERR_GENERAL bit of the io_error flag that ends
// a file list: the sender could not read something and left it out. rsync
// skips --delete when it is set. IOERR_VANISHED alone, a file deleted while
// the sender listed, leaves the list right.
const ioErrGeneral = 1

// peerMsgs collects, as a run's stderr, the messages the rsync peer sends:
// gorsync logs each as one line holding "rsync: <message>". On a listing
// run, stock rsync sends one for everything it could not read and left out
// of the file list, and it sends it ahead of any list data it writes later
// (io.c perform_io flushes pending messages first).
type peerMsgs struct {
	seen atomic.Bool
	mu   sync.Mutex
	msgs []string
}

func (p *peerMsgs) Write(b []byte) (int, error) {
	for line := range strings.Lines(string(b)) {
		i := strings.Index(line, "rsync: ")
		if i < 0 || i > 0 && !strings.HasSuffix(line[:i], ": ") {
			continue
		}
		// rsyserr messages carry their own "rsync: " prefix.
		msg := strings.TrimPrefix(strings.TrimSpace(line[i+len("rsync: "):]), "rsync: ")
		p.seen.Store(true)
		p.mu.Lock()
		p.msgs = append(p.msgs, msg)
		p.mu.Unlock()
	}
	return len(b), nil
}

func (p *peerMsgs) Close() error { return nil }

// missed reports error messages the peer counted but this report did not
// see, a line format it does not know: nothing can be pinned on them.
func (p *peerMsgs) missed(res *rsyncclient.Result) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return res.XferErrors > len(p.msgs)
}

// problems returns the messages about something left out of the file list.
// "file has vanished" is not one: that file is gone, so leaving it out is
// right. The "rsync error: ... (code 23)" summary repeats the others.
func (p *peerMsgs) problems() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, m := range p.msgs {
		if strings.HasPrefix(m, "file has vanished: ") || strings.HasPrefix(m, "rsync error: ") {
			continue
		}
		out = append(out, m)
	}
	return out
}

// listErr fails a listing run whose file list the sender could not build in
// full, the run rsync itself ends with exit code 23 and no deletes.
func listErr(label string, res *rsyncclient.Result, peer *peerMsgs) error {
	probs := peer.problems()
	if len(probs) == 0 && res.IOErrors&ioErrGeneral == 0 && !peer.missed(res) {
		return nil
	}
	return incompleteListing(label, probs)
}

// ErrIncompleteListing marks a listing the server could not build in full.
// Retry treats it as permanent: what the server cannot read is on its disk.
var ErrIncompleteListing = errors.New("the server could not read everything here, so this listing is incomplete")

func incompleteListing(label string, probs []string) error {
	err := fmt.Errorf("%s: %w", label, ErrIncompleteListing)
	if len(probs) > 0 {
		err = fmt.Errorf("%w: %s", err, strings.Join(probs[:min(len(probs), 3)], "; "))
	}
	return err
}

// relUnder maps p, an absolute path, to a relPath under base; false when it
// is not under base.
func relUnder(base, p string) (string, bool) {
	if !strings.HasPrefix(p, "/") {
		return "", false
	}
	base, p = path.Clean(base), path.Clean(p)
	switch {
	case p == base:
		return "", true
	case base == "/":
		return p[1:], true
	case strings.HasPrefix(p, base+"/"):
		return p[len(base)+1:], true
	}
	return "", false
}

// rsyncListing is one recursive listing run: the file list stream into the
// cache, and the peer's messages as the run's stderr. Once the peer reports
// a problem the stream stops releasing folders; at the end, the folders the
// messages name are dropped and the rest is released.
type rsyncListing struct {
	peerMsgs
	scope string
	g     *emitGrouper
	dirs  map[string]bool // every folder the list holds, and the scope
	// toRel maps a path quoted in a peer message to a relPath, false when it
	// is not under the backend's base.
	toRel func(string) (string, bool)
}

func newRsyncListing(scope string, emit func(string, []model.FileEntry), complete func(string), toRel func(string) (string, bool)) *rsyncListing {
	l := &rsyncListing{scope: scope, dirs: map[string]bool{scope: true}, toRel: toRel}
	l.g = &emitGrouper{emit: emit, complete: complete, lateChildren: true, hold: l.seen.Load}
	return l
}

func (l *rsyncListing) add(fi rsyncpkg.FileInfo) {
	e, ok := fileInfoEntry(l.scope, fi)
	if !ok {
		return
	}
	if e.IsDir {
		l.dirs[e.RelPath] = true
	}
	l.g.add(e)
}

// finish ends a run that returned no error. A run whose problems cannot all
// be pinned to folders fails as a whole: the cache drops everything. That
// includes a flag with no message at all, which is how gorsync's sender
// reports an unreadable folder.
func (l *rsyncListing) finish(label string, res *rsyncclient.Result) error {
	probs := l.problems()
	if l.missed(res) {
		return incompleteListing(label, probs)
	}
	if len(probs) == 0 {
		if res.IOErrors&ioErrGeneral != 0 {
			return incompleteListing(label, nil)
		}
		l.g.finish()
		return nil
	}
	unread := map[string]bool{}
	for _, m := range probs {
		dirs, ok := l.unreadBy(m)
		if !ok {
			return incompleteListing(label, probs)
		}
		for _, d := range dirs {
			unread[d] = true
		}
	}
	l.g.finishExcept(unread)
	return &unreadDirs{dirs: slices.Sorted(maps.Keys(unread)), why: incompleteListing(label, probs).Error()}
}

// unreadBy names the folders whose listings a peer message makes short.
// rsync quotes the path, as in `opendir "/photos" (in backups) failed`. A
// folder it could not open lists none of its children; an entry it could
// not stat is missing from its parent. Both are named for any message, as
// long as one of them is a folder the list holds: false otherwise, also
// for a message with no quoted path.
func (l *rsyncListing) unreadBy(msg string) ([]string, bool) {
	i, j := strings.IndexByte(msg, '"'), strings.LastIndexByte(msg, '"')
	if i < 0 || j <= i || l.toRel == nil {
		return nil, false
	}
	rel, ok := l.toRel(msg[i+1 : j])
	if !ok || rel != l.scope && !strings.HasPrefix(rel, l.scope+"/") && l.scope != "" {
		return nil, false
	}
	var dirs []string
	if l.dirs[rel] {
		dirs = append(dirs, rel)
	}
	if parent := parentDir(rel); rel != l.scope && l.dirs[parent] {
		dirs = append(dirs, parent)
	}
	return dirs, len(dirs) > 0
}

// tailProgress credits growth of dir/base beyond skip to the context's
// progress counter until the returned stop runs. Without a counter it is a
// no-op.
func tailProgress(ctx context.Context, dir, base string, skip int64) func() {
	counter := progressFromContext(ctx)
	if counter == nil {
		return func() {}
	}
	size, ok := fileSizeFromContext(ctx)
	if !ok || size <= 0 {
		size = 1 << 62
	}
	size = max(size-skip, 0)
	var baseAdder *CappedAdder
	if bp := baseProgressFromContext(ctx); bp != nil {
		baseAdder = NewCappedAdder(bp, size)
	}
	stop := make(chan struct{})
	go tailDirSize(stop, dir, base, NewCappedAdder(counter, size), baseAdder, skip)
	return func() { close(stop) }
}

// rsyncOpenViaTemp receives relPath into a fresh temp dir through recv and
// returns the file from offset on, removing the dir on Close. A non-zero
// offset plants a sparse placeholder of that size first, so an --append
// receive fetches only the tail; a source that turned out shorter is an
// error, the caller then copies in full. With a progress counter the reader
// is marked pre-counted since the tail already credited the body.
func rsyncOpenViaTemp(ctx context.Context, relPath string, offset int64, recv func(dstDir string) error) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-dl-*")
	if err != nil {
		return nil, err
	}
	target := filepath.Join(tmpDir, filepath.Base(relPath))
	if offset > 0 {
		err = os.Truncate(target, offset)
		if errors.Is(err, os.ErrNotExist) {
			err = os.WriteFile(target, nil, 0o600)
			if err == nil {
				err = os.Truncate(target, offset)
			}
		}
	}
	if err == nil {
		stop := tailProgress(ctx, tmpDir, filepath.Base(relPath), offset)
		err = recv(tmpDir + "/")
		stop()
	}
	var f *os.File
	if err == nil {
		f, err = os.Open(target)
	}
	if err == nil && offset > 0 {
		var fi os.FileInfo
		if fi, err = f.Stat(); err == nil && fi.Size() < offset {
			err = fmt.Errorf("%s: source is %d bytes, shorter than offset %d", relPath, fi.Size(), offset)
		}
		if err == nil {
			_, err = f.Seek(offset, io.SeekStart)
		}
		if err != nil {
			f.Close()
		}
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
	stop := tailProgress(ctx, parent, filepath.Base(dstPath), 0)
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
	if err == nil {
		err = stampModTime(ctx, tmpFile)
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
	if err == nil {
		err = stampModTime(ctx, path)
	}
	return err
}

// stampModTime gives a staged upload the source's mtime, so -t sends that
// instead of the moment it was staged.
func stampModTime(ctx context.Context, file string) error {
	mtime, ok := modTimeFromContext(ctx)
	if !ok {
		return nil
	}
	return os.Chtimes(file, mtime, mtime)
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
