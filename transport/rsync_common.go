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
	"time"

	rsyncpkg "github.com/gokrazy/rsync"
	"github.com/gokrazy/rsync/rsyncclient"

	"sc/model"
)

// rsyncTransferFlags are passed to every rsync invocation that moves file
// data. -t preserves mtime. With neither --inplace nor --partial the
// receiver writes a temp file next to the destination, renames it over the
// destination once complete and removes it when the transfer fails (R12):
// an overwrite cut off half way leaves the old version, not the new start
// on the old end, and a file with other hard links (a snapshot backup)
// gets a new inode instead of changing theirs. --partial would rename the
// cut-off temp file over the destination. The resume path adds
// --append-verify, which writes in place by itself. -W disables the
// delta-sync algorithm so the sender streams the whole file. -W is intentional: gokrazy/rsync's hashSearch path
// (internal/sender/fileio.go) misclassifies read errors as "file has changed
// mid-transfer", and on this workload delta-sync saves no bandwidth anyway
// (mostly immutable archives). --ignore-times bypasses rsync's quick-check
// (skip when size+mtime match): SC only invokes these calls on user-initiated
// copies, so we must transfer unconditionally, otherwise a same-size,
// same-mtime, different-content file is silently skipped. -O leaves the
// times of folders alone: sc stages uploads in a temp tree whose folder
// times mean nothing, and a server may not let it set the time of a folder
// it does not own, which rsync reports as an error.
var rsyncTransferFlags = []string{"-t", "-O", "-W", "--ignore-times"}

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
// silenced; the library would otherwise write into the TUI. It never
// restricts the process: on Linux gorsync otherwise puts the whole process
// in a Landlock sandbox allowing only the folders of that one run, which
// cannot be lifted, so after the first download sc could not make a temp
// folder or read any other folder.
func newRsyncClient(flags []string, opts ...rsyncclient.Option) (*rsyncclient.Client, error) {
	base := []rsyncclient.Option{rsyncclient.WithStdout(io.Discard), rsyncclient.WithStderr(nopWriteCloser{io.Discard}), rsyncclient.DontRestrict()}
	return rsyncclient.New(flags, append(base, opts...)...)
}

// nopWriteCloser lets a plain writer stand in where gorsync wants a closer it
// never closes.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// countingRW wraps a session stream so bytes written credit adder and tell
// the stall guard over ctx the transfer moves, also when nothing is
// credited (a source that counted itself, D5); nil when neither listens.
func countingRW(ctx context.Context, adder *CappedAdder) func(io.ReadWriteCloser) io.ReadWriteCloser {
	if adder == nil && watchFromContext(ctx) == nil {
		return nil
	}
	return func(rw io.ReadWriteCloser) io.ReadWriteCloser {
		return &CountingReadWriter{RW: rw, Adder: adder, ctx: ctx}
	}
}

// transferErr fails a transfer run that rsync itself ends with exit code
// 23 or 24 and that the library returns without an error: the server
// reported errors for some files (it could not write, delete or read
// them, or a failed --append-verify), or the sending side left something
// out because it could not read it or it vanished.
func transferErr(xferErrors, ioErrors int) error {
	switch {
	case xferErrors > 0:
		return fmt.Errorf("rsync: the server reported %d error(s); not everything was done", xferErrors)
	case ioErrors != 0:
		return errors.New("rsync: the sending side could not read everything; not everything was sent")
	}
	return nil
}

// md4Of serves Checksum for md4: one fetch scoped to the file. No sum is
// kept from one ask to the next. A cache of the sums bulk fetches gave,
// with no size or time to tell a file someone else changed since, served
// the old sum as current, "equal" to the other side (R11); the scanner
// keeps each sum with the size and time it is for.
func md4Of(ctx context.Context, relPath string, fetch func(context.Context, string, bool) (map[string]string, error)) (string, error) {
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

// md4Sums are the non-zero checksums of a dry-run file list, keyed by path
// relative to the backend base.
func md4Sums(scope string, recursive bool, list []rsyncpkg.FileInfo) map[string]string {
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
	return got
}

func rsyncModeToFileMode(m int32) os.FileMode {
	mode := unixFileMode(uint32(m))
	switch m & rsyncpkg.S_IFMT {
	case rsyncpkg.S_IFREG:
	case rsyncpkg.S_IFDIR:
		mode |= os.ModeDir
	case rsyncpkg.S_IFLNK:
		mode |= os.ModeSymlink
	case rsyncpkg.S_IFIFO:
		mode |= os.ModeNamedPipe
	case rsyncpkg.S_IFSOCK:
		mode |= os.ModeSocket
	case rsyncpkg.S_IFCHR:
		mode |= os.ModeDevice | os.ModeCharDevice
	case rsyncpkg.S_IFBLK:
		mode |= os.ModeDevice
	default:
		mode |= os.ModeIrregular
	}
	return mode
}

// fileInfoEntry converts one dry-run file list item to an entry under scope;
// false for the "." self entry. Stock rsync lists no links, pipes or devices
// without -l and -D; gorsyncd lists them, and they keep their type here: a
// pipe taken for a file would be opened by a download (L2, L9).
func fileInfoEntry(scope string, fi rsyncpkg.FileInfo) (model.FileEntry, bool) {
	if fi.Name == "" || fi.Name == "." {
		return model.FileEntry{}, false
	}
	mode := rsyncModeToFileMode(fi.Mode)
	return model.FileEntry{
		RelPath: path.Join(scope, fi.Name),
		Name:    path.Base(fi.Name),
		Size:    fi.Length,
		ModTime: fi.ModTime,
		TimeRes: model.TimeSecond, // the file list has whole seconds
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

// all returns every message the peer sent, in order.
func (p *peerMsgs) all() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.msgs)
}

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

// tailProgress credits what rsync receives as dir/base to the context's
// progress counter until the returned stop runs: the growth beyond skip of
// the file itself for a receive in place (--append), else of rsync's temp
// file (see tailDirSize). stop(true), after a receive that worked, credits
// what the last look missed: the file is all there. Without a counter it
// is a no-op.
func tailProgress(ctx context.Context, dir, base string, inplace bool, skip int64) func(ok bool) {
	counter := progressFromContext(ctx)
	if counter == nil {
		return func(bool) {}
	}
	size, known := progressBudgetFromContext(ctx)
	if known = known && size > 0; !known {
		size = 1 << 62
	}
	size = max(size-skip, 0)
	var baseAdder *CappedAdder
	if bp := baseProgressFromContext(ctx); bp != nil {
		baseAdder = NewCappedAdder(bp, size)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	adder := NewCappedAdder(counter, size)
	go func() {
		defer close(done)
		tailDirSize(stop, dir, base, inplace, adder, baseAdder, skip)
	}()
	return func(ok bool) {
		close(stop)
		<-done
		if ok && known {
			adder.Add(size)
		}
	}
}

// rsyncOpenViaTemp receives relPath into a fresh temp dir through recv and
// returns the file from offset on, removing the dir on Close. A non-zero
// offset plants a sparse placeholder of that size first, so an --append
// receive fetches only the tail; a source that turned out shorter is an
// error, the caller then copies in full. With a progress counter the reader
// is marked pre-counted since the tail already credited the body.
func rsyncOpenViaTemp(ctx context.Context, relPath string, offset int64, recv func(dstDir string) error) (io.ReadCloser, error) {
	tmpDir, err := tempDir("rsync-dl-*")
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
		// Below protocol 30 the sender hashes the prefix before it sends
		// the rest (an --append is verified there): nothing moves meanwhile.
		ExpectSilence(ctx, HashTime(offset))
		stop := tailProgress(ctx, tmpDir, filepath.Base(relPath), offset > 0, offset)
		err = recv(tmpDir + "/")
		stop(err == nil)
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
		// Counted as it came in; reading it on, as fast as the
		// destination takes it, still shows the copy alive (D5).
		return WrapPreCounted(activeReadCloser(ctx, rc)), nil
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
	stop := tailProgress(ctx, parent, filepath.Base(dstPath), false, 0)
	err := recv(parent + "/")
	stop(err == nil)
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

// appendVerifySilence is how long an --append-verify of a file whose first
// offset bytes are already there sends nothing (R7): the sender reads and
// hashes that prefix before the first byte of the rest, and the receiver
// hashes its copy of it before it takes the rest.
func appendVerifySilence(offset int64) time.Duration { return 2 * HashTime(offset) }

// stageUpload spools src into tmpFile so rsync can send from disk, crediting
// half the progress budget for the read (the push credits the rest). Returns
// the push-phase adder and its budget, nil without progress. A file the
// temp folder has no room for fails before a byte is read (roomFor).
func stageUpload(ctx context.Context, src io.Reader, tmpFile string, p model.PutParams) (*CappedAdder, int64, error) {
	if err := roomFor(filepath.Dir(tmpFile), p.Size); err != nil {
		return nil, 0, err
	}
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, p.Mode)
	if err != nil {
		return nil, 0, err
	}
	counter := progressFromContext(ctx)
	fileSize := max(p.Size, 0)
	var readAdder, pushAdder *CappedAdder
	var pushBudget int64
	if counter != nil && fileSize > 0 && !IsPreCounted(src) {
		readBudget := fileSize / 2
		pushBudget = fileSize - readBudget
		readAdder = NewCappedAdder(counter, readBudget)
		pushAdder = NewCappedAdder(counter, pushBudget)
	}
	spool := &spoolWriter{w: f}
	var dst io.Writer = spool
	if readAdder != nil {
		dst = &CountingWriter{W: spool, Adder: readAdder}
	}
	_, err = io.Copy(dst, ActiveReader(ctx, src))
	if spool.err != nil {
		err = tempFull(err)
	}
	if cerr := f.Close(); err == nil {
		err = tempFull(cerr)
	}
	if err == nil {
		err = stampModTime(tmpFile, p.ModTime)
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
// A source that gives another number of bytes than its size fails: rsync
// would send and verify the short copy as if it were the file. No progress
// shows it (the engine credited the prefix, the push credits the rest), so
// each read tells the stall guard it works (R7).
func spoolTo(ctx context.Context, src model.RangeOpener, path string, p model.PutParams) error {
	if err := roomFor(filepath.Dir(path), src.Size()); err != nil {
		return err
	}
	rd, err := src.Open(ctx)
	if err != nil {
		return err
	}
	defer rd.Close()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, p.Mode)
	if err != nil {
		return err
	}
	spool := &spoolWriter{w: f}
	n, err := io.Copy(spool, ActiveReader(ctx, rd))
	if spool.err != nil {
		err = tempFull(err)
	}
	if cerr := f.Close(); err == nil {
		err = tempFull(cerr)
	}
	if cerr := rd.Close(); err == nil && cerr != nil {
		err = cerr
	}
	if err == nil && n != src.Size() {
		err = SizeMismatch(n, src.Size())
	}
	if err == nil {
		err = stampModTime(path, p.ModTime)
	}
	return err
}

// stampModTime gives a staged upload the source's mtime, so -t sends that
// instead of the moment it was staged. A zero mtime is not known: the file
// keeps the time it was staged, and SetTimes follows.
func stampModTime(file string, mtime time.Time) error {
	if mtime.IsZero() {
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
