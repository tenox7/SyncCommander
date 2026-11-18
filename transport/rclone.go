package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/atexit"

	"sc/model"
)

const rcloneScheme = "rclone://"

// rcloneObjCacheLimit bounds the per-backend fs.Object cache. Objects carry
// the listing metadata (etag, storage class), so a hit turns Checksum and
// Open into zero extra round trips on S3/B2/Drive. Two generations are live
// at once, so peak is 2x this.
const rcloneObjCacheLimit = 100000

// RcloneBackend adapts an rclone fs.Fs to model.Backend, bringing every
// rclone remote (S3, GCS, Azure, Drive, Dropbox, B2, Swift, SMB, crypt, ...)
// into sc. Optional capabilities are discovered from fs.Features at runtime
// and degrade to the required Backend methods when a remote lacks them.
//
//	rclone://gdrive/Photos/2024                    named remote from rclone.conf
//	rclone://gdrive:Photos/2024                    same, rclone's own syntax
//	rclone://:s3,provider=AWS,access_key_id=K:bkt  ad-hoc, no config file
//	rclone://gdrive,shared_with_me:file            per-remote option override
//
// Everything after rclone:// is handed to rclone verbatim, except the sugar
// form with no colon where the first path segment becomes the remote name.
type RcloneBackend struct {
	url       string
	f         fs.Fs
	feat      *fs.Features
	avail     []string
	listCache *listCache
	objs      *rcObjCache
	dirs      sync.Map // directories known to exist
	putMtime  sync.Map // mtime stamped at Put, so SetTimes can skip a redundant round trip
	closeOnce sync.Once
	timeRes   model.TimeRes // how exact the listed mtimes are (rcloneTimeRes)
}

var rcloneInit sync.Once

// rcloneConf is rclone's config file storage, installed by rcloneSetup.
var rcloneConf *rcloneConfig

// rcloneSetup makes rclone a library that leaves the process and the
// terminal to sc, the first time it runs, and reads rclone's config file
// (L13). Every remote reads that file, so its error fails every open.
//   - rclone logs into sc's remote log: its default handler writes to
//     stderr, which would corrupt the TUI.
//   - An encrypted config file never asks for its password on the terminal
//     the TUI holds: RCLONE_CONFIG_PASS or RCLONE_PASSWORD_COMMAND gives it,
//     or the open fails and says so.
//   - rclone installs no SIGINT/SIGTERM handler: its handler ends the
//     process with the terminal still raw, where bubbletea's restores it.
//   - A config file that cannot be read is an error, never rclone's
//     os.Exit, and the next open reads it again.
func rcloneSetup() error {
	rcloneInit.Do(func() {
		fs.SetLogger(rcloneLogHandler{})
		fs.GetConfig(nil).AskPassword = false
		atexit.IgnoreSignals()
		configfile.Install()
		rcloneConf = &rcloneConfig{Storage: config.Data()}
		config.SetData(rcloneConf) // no-op when there is no config file path
		config.LoadedData()        // reads the file through rcloneConf.Load
	})
	return rcloneConf.check()
}

// rcloneConfig is rclone's config file storage with a Load that cannot end
// the process: rclone loads the file on first use (config.LoadedData) and
// calls os.Exit on any error but "not found". Load keeps the error for
// check and tells rclone the file loaded, holding no remotes.
type rcloneConfig struct {
	config.Storage
	mu  sync.Mutex
	err error // the last load's failure
}

func (c *rcloneConfig) Load() error {
	err := c.Storage.Load()
	failed := err
	if err == config.ErrorConfigFileNotFound { // rclone compares it so
		failed = nil
	}
	c.mu.Lock()
	c.err = failed
	c.mu.Unlock()
	if failed != nil {
		return nil
	}
	return err
}

// check returns the config file's error, reading the file again when the
// last read failed: a file fixed while sc runs needs no restart.
func (c *rcloneConfig) check() error {
	c.mu.Lock()
	failed := c.err != nil
	c.mu.Unlock()
	if failed {
		_ = c.Load()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return fmt.Errorf("rclone: config file %s: %w", config.GetConfigPath(), c.err)
	}
	return nil
}

// rcloneFatalHook runs before rclone ends the process; see OnRcloneFatal.
var rcloneFatalHook atomic.Pointer[func()]

// OnRcloneFatal sets release to run before rclone ends the process, which
// it still does on a few errors it calls fatal, such as a CA certificate
// file that cannot be read; nil unsets it. main gives back the terminal the
// TUI holds.
func OnRcloneFatal(release func()) {
	if release == nil {
		rcloneFatalHook.Store(nil)
		return
	}
	rcloneFatalHook.Store(&release)
}

type rcloneLogHandler struct{}

func (rcloneLogHandler) Enabled(_ context.Context, l slog.Level) bool { return l > slog.LevelDebug }

func (rcloneLogHandler) Handle(_ context.Context, r slog.Record) error {
	dir := DirIn
	if r.Level >= slog.LevelError {
		dir = DirErr
	}
	Log.Add("rclone", dir, r.Message)
	if r.Level == fs.SlogLevelCritical {
		rcloneFatal(r.Message)
	}
	return nil
}

// rcloneFatal runs right before rclone's os.Exit(1), which sc cannot stop:
// rclone logs at its critical level only in fs.Fatal and fs.Fatalf. The
// hook gives the terminal back, and the reason goes to stderr, where it
// stays after the exit; sc's own log goes with the process.
func rcloneFatal(msg string) {
	if release := rcloneFatalHook.Load(); release != nil {
		(*release)()
	}
	fmt.Fprintln(os.Stderr, "sc: rclone ended the program:", msg)
}

func (h rcloneLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h rcloneLogHandler) WithGroup(string) slog.Handler      { return h }

// parseRcloneSpec turns an sc URL into an rclone remote spec.
func parseRcloneSpec(rawURL string) (string, error) {
	spec := strings.TrimPrefix(rawURL, rcloneScheme)
	if spec == "" {
		return "", errors.New("rclone: empty remote, expected rclone://remote/path")
	}
	// An unquoted colon means the caller already wrote rclone syntax (named
	// remote, connection string, or option override) — pass it through. A
	// leading slash is a bare local path, which rclone also takes verbatim.
	if unquotedIndex(spec, ':') >= 0 || strings.HasPrefix(spec, "/") {
		return spec, nil
	}
	name, rest, _ := strings.Cut(spec, "/")
	return name + ":" + rest, nil
}

// NewRcloneBackend makes the remote rawURL names; a cancel of ctx stops it
// while it is being made.
func NewRcloneBackend(ctx context.Context, rawURL string, insecure bool) (*RcloneBackend, error) {
	if err := rcloneSetup(); err != nil {
		return nil, err
	}
	spec, err := parseRcloneSpec(rawURL)
	if err != nil {
		return nil, err
	}
	// A remote may keep the context it is made with for later work (s3
	// makes new connections with it, smb keeps it): it gets one that a
	// cancel of ctx ends only while the remote is being made.
	makeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopWatch := context.AfterFunc(ctx, cancel)
	makeCtx, ci := fs.AddConfig(makeCtx)
	ci.InsecureSkipVerify = insecure
	// Object stores keep the real mtime in per-object metadata, so an exact
	// mtime costs one HEAD per file — unusable at a million objects. Default
	// to the timestamp that comes free in the listing (the upload time) and
	// let RCLONE_USE_SERVER_MODTIME=false buy exactness at that price.
	ci.UseServerModTime = !isFalse(os.Getenv("RCLONE_USE_SERVER_MODTIME"))

	f, err := fs.NewFs(makeCtx, spec)
	if !stopWatch() {
		// ctx ended while the remote was made: it holds a cancelled
		// context now, so it is not used.
		if s, ok := f.(fs.Shutdowner); ok {
			_ = s.Shutdown(context.Background())
		}
		return nil, fmt.Errorf("rclone: %w", ctx.Err())
	}
	if errors.Is(err, fs.ErrorIsFile) {
		return nil, fmt.Errorf("rclone: %s is a file, point at a directory", MaskURLPassword(rawURL))
	}
	if err != nil {
		return nil, fmt.Errorf("rclone: %v", err)
	}
	b := &RcloneBackend{
		url:       MaskURLPassword(rawURL),
		f:         f,
		feat:      f.Features(),
		objs:      newRcObjCache(rcloneObjCacheLimit),
		listCache: newListCache(),
	}
	b.avail = rcloneHashNames(f.Hashes())
	b.timeRes = rcloneTimeRes(b.feat.SlowModTime && ci.UseServerModTime, f.Precision())
	Log.Add("rclone", DirIn, fmt.Sprintf("%s: %s hashes=%v listR=%v move=%v dirmove=%v",
		b.url, f.String(), b.avail, b.canListRecursive(), b.feat.Move != nil, b.feat.DirMove != nil))
	if b.feat.SlowModTime && ci.UseServerModTime {
		Log.Add("rclone", DirIn, b.url+": mtime is the server upload time; "+
			"RCLONE_USE_SERVER_MODTIME=false reads the exact one at 1 request per file")
	}
	return b, nil
}

func (b *RcloneBackend) BasePath() string { return b.url }

// Close shuts the remote down when rclone gives it a Shutdown: smb closes
// its sessions, dropbox commits the uploads it batched, crypt passes it on
// to what it wraps. rclone's own cache of remotes calls it when it lets one
// go; sc makes its remotes without that cache, so nothing else does. Only
// the first Close shuts it down.
func (b *RcloneBackend) Close() error {
	var err error
	b.closeOnce.Do(func() {
		if s, ok := b.f.(fs.Shutdowner); ok {
			err = s.Shutdown(context.Background())
		}
	})
	return err
}

// rcloneHashOrder is sc's preference order intersected with what rclone can
// compute. Cheap-and-wide first: xxh3 is fastest locally, md5 is what most
// object stores hand back for free in a listing.
var rcloneHashOrder = []struct {
	name string
	t    hash.Type
}{
	{"xxh3", hash.XXH3},
	{"md5", hash.MD5},
	{"sha256", hash.SHA256},
	{"sha1", hash.SHA1},
}

func rcloneHashNames(set hash.Set) []string {
	var out []string
	for _, h := range rcloneHashOrder {
		if set.Contains(h.t) {
			out = append(out, h.name)
		}
	}
	return out
}

func (b *RcloneBackend) ChecksumAlgos(context.Context) ([]string, error) { return b.avail, nil }

// hashType is rclone's type for algo, when the remote has it.
func (b *RcloneBackend) hashType(algo string) (hash.Type, error) {
	for _, h := range rcloneHashOrder {
		if h.name == algo && b.f.Hashes().Contains(h.t) {
			return h.t, nil
		}
	}
	return hash.None, fmt.Errorf("rclone: %s has no %q checksum", b.f.Name(), algo)
}

// object resolves relPath to an fs.Object, preferring the listing cache so
// backends that carry the checksum in their listing never pay a second round
// trip for it.
func (b *RcloneBackend) object(ctx context.Context, relPath string) (fs.Object, error) {
	if o, ok := b.objs.get(relPath); ok {
		return o, nil
	}
	o, err := b.f.NewObject(ctx, relPath)
	if err != nil {
		return nil, err
	}
	b.objs.put(relPath, o)
	return o, nil
}

// Every method cleans its path first (cleanRel): rclone joins a path onto
// the remote's root as it is given, so a ".." would climb above it (L7).
func (b *RcloneBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, cleanRel(relDir), b.liveList)
}

func (b *RcloneBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	Log.Add("rclone", DirOut, "list "+b.f.Name()+":"+path.Join(b.f.Root(), relDir))
	dirEntries, err := b.f.List(ctx, relDir)
	if err != nil {
		return nil, err
	}
	entries := b.convert(ctx, dirEntries)
	Log.Add("rclone", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// Stat asks for relPath alone: NewObject for a file, one call (a HEAD on an
// object store), never from the listing cache. rclone has no call for one
// folder, so when no file is there the parent folder is listed, as rclone's
// own stat does.
func (b *RcloneBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	rel := cleanRel(relPath)
	if rel == "" {
		return model.FileEntry{IsDir: true, Mode: os.ModeDir | 0755}, nil
	}
	Log.Add("rclone", DirOut, "stat "+b.f.Name()+":"+path.Join(b.f.Root(), rel))
	o, err := b.f.NewObject(ctx, rel)
	if err == nil {
		return b.convert(ctx, fs.DirEntries{o})[0], nil
	}
	if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorIsDir) && !errors.Is(err, fs.ErrorNotAFile) {
		return model.FileEntry{}, err
	}
	dirEntries, err := b.f.List(ctx, model.DirOf(rel))
	if errors.Is(err, fs.ErrorDirNotFound) {
		return model.FileEntry{}, fmt.Errorf("rclone: %s: %w", rel, os.ErrNotExist)
	}
	if err != nil {
		return model.FileEntry{}, err
	}
	for _, d := range dirEntries {
		if strings.TrimPrefix(d.Remote(), "/") == rel {
			return b.convert(ctx, fs.DirEntries{d})[0], nil
		}
	}
	return model.FileEntry{}, fmt.Errorf("rclone: %s: %w", rel, os.ErrNotExist)
}

// convert maps rclone entries to sc entries, caching every object seen so a
// later Checksum or Open costs no extra round trip.
func (b *RcloneBackend) convert(ctx context.Context, dirEntries fs.DirEntries) []model.FileEntry {
	entries := make([]model.FileEntry, 0, len(dirEntries))
	for _, d := range dirEntries {
		rel := strings.TrimPrefix(d.Remote(), "/")
		e := model.FileEntry{
			RelPath: rel,
			Name:    path.Base(rel),
			Size:    d.Size(),
			Mode:    0644,
		}
		if o, ok := d.(fs.Object); ok {
			b.objs.put(rel, o)
		} else {
			e.IsDir = true
			e.Size = 0
			e.Mode = os.ModeDir | 0755
		}
		e.ModTime, e.TimeRes = d.ModTime(ctx), b.timeRes
		entries = append(entries, e)
	}
	return entries
}

func (b *RcloneBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	relPath = cleanRel(relPath)
	ht, err := b.hashType(algo)
	if err != nil {
		return "", err
	}
	o, err := b.object(ctx, relPath)
	if err != nil {
		return "", err
	}
	sum, err := o.Hash(ctx, ht)
	if err != nil {
		return "", err
	}
	if sum == "" {
		return "", fmt.Errorf("rclone: no %s for %s", ht, relPath)
	}
	return sum, nil
}

func (b *RcloneBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	relPath = cleanRel(relPath)
	if put, ok := b.putMtime.LoadAndDelete(relPath); ok && put.(time.Time).Equal(mtime) {
		return nil
	}
	o, err := b.object(ctx, relPath)
	if err != nil {
		return err
	}
	// Some remotes implement SetModTime as a server-side copy onto itself,
	// which changes the etag, so the cached object must not outlive this.
	defer b.objs.drop(relPath)
	if err := o.SetModTime(ctx, mtime); err != nil {
		return err
	}
	// atime/btime only survive on remotes with a metadata layer (local, smb,
	// and the object stores that keep user metadata).
	sm, ok := o.(fs.SetMetadataer)
	if !ok || !b.feat.WriteMetadata {
		return nil
	}
	md := fs.Metadata{"mtime": mtime.Format(time.RFC3339Nano)}
	if !atime.IsZero() {
		md["atime"] = atime.Format(time.RFC3339Nano)
	}
	if !btime.IsZero() {
		md["btime"] = btime.Format(time.RFC3339Nano)
	}
	if err := sm.SetMetadata(ctx, md); err != nil {
		Log.Add("rclone", DirErr, "set times metadata "+relPath+": "+err.Error())
	}
	return nil
}

func (b *RcloneBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	relPath = cleanRel(relPath)
	if err := b.ensureDir(ctx, parentDir(relPath)); err != nil {
		return err
	}
	size := p.Size
	if !p.SizeKnown() {
		size = -1
	}
	put := b.f.Put
	if size < 0 {
		// rclone needs the length up front unless the remote can stream.
		if b.feat.PutStream == nil {
			var cleanup func()
			var err error
			src, size, cleanup, err = spoolToTemp(src)
			if err != nil {
				return err
			}
			defer cleanup()
		} else {
			put = b.feat.PutStream
		}
	}
	// Stamping the source mtime here makes the later SetTimes a no-op on
	// stores that would otherwise implement it as a copy of the object.
	mtime := p.ModTime
	if mtime.IsZero() {
		mtime = time.Now()
	}
	info := &rcObjectInfo{fs: b.f, remote: relPath, size: size, modTime: mtime}
	o, err := put(ctx, LimitOutReader(ctx, src), info)
	if err != nil {
		return err
	}
	b.putMtime.Store(relPath, mtime)
	b.objs.put(relPath, o)
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// spoolToTemp buffers an unknown-length stream so its size can be declared to
// a remote that cannot stream. Only reached on remotes without PutStream.
func spoolToTemp(src io.Reader) (io.Reader, int64, func(), error) {
	tmp, err := os.CreateTemp("", "sc-rclone-*")
	if err != nil {
		return nil, 0, nil, err
	}
	cleanup := func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}
	n, err := io.Copy(tmp, src)
	if err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	return tmp, n, cleanup, nil
}

func (b *RcloneBackend) Mkdir(ctx context.Context, relPath string, _ os.FileMode) error {
	relPath = cleanRel(relPath)
	if err := b.ensureDir(ctx, relPath); err != nil {
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// ensureDir walks the components so remotes whose Mkdir is not recursive
// still get the full path, remembering what exists so a copy of many files
// into one directory is not a Mkdir per file. Bucket backends no-op anyway.
func (b *RcloneBackend) ensureDir(ctx context.Context, relPath string) error {
	relPath = strings.Trim(relPath, "/")
	if relPath == "" {
		return nil
	}
	if _, ok := b.dirs.Load(relPath); ok {
		return nil
	}
	if err := b.ensureDir(ctx, parentDir(relPath)); err != nil {
		return err
	}
	if err := b.f.Mkdir(ctx, relPath); err != nil {
		return err
	}
	b.dirs.Store(relPath, struct{}{})
	return nil
}

func (b *RcloneBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	oldRelPath, newRelPath = cleanRel(oldRelPath), cleanRel(newRelPath)
	if err := renameBase("rclone", oldRelPath, newRelPath); err != nil {
		return err
	}
	if taken, err := nameTaken(ctx, b.liveList, newRelPath); err != nil {
		return err
	} else if taken {
		return renameTargetTaken("rclone", newRelPath)
	}
	b.dirs.Clear()
	defer func() {
		b.objs.drop(oldRelPath)
		b.listCache.invalidateAncestors(oldRelPath)
		b.listCache.invalidateAncestors(newRelPath)
		b.listCache.invalidateTree(oldRelPath)
	}()
	// operations.Move/DirMove use the remote's server-side move when it has
	// one and fall back to copy-then-delete when it doesn't, so this works on
	// stores with no rename primitive at all.
	o, err := b.f.NewObject(ctx, oldRelPath)
	if errors.Is(err, fs.ErrorIsDir) || errors.Is(err, fs.ErrorObjectNotFound) {
		return operations.DirMove(ctx, b.f, oldRelPath, newRelPath)
	}
	if err != nil {
		return err
	}
	if err := b.Mkdir(ctx, parentDir(newRelPath), 0755); err != nil {
		return err
	}
	_, err = operations.Move(ctx, b.f, nil, newRelPath, o)
	return err
}

func (b *RcloneBackend) Remove(ctx context.Context, relPath string) error {
	relPath = cleanRel(relPath)
	defer func() {
		b.objs.drop(relPath)
		b.listCache.invalidateAncestors(relPath)
	}()
	o, err := b.f.NewObject(ctx, relPath)
	if errors.Is(err, fs.ErrorIsDir) || errors.Is(err, fs.ErrorObjectNotFound) {
		return b.f.Rmdir(ctx, relPath)
	}
	if err != nil {
		return err
	}
	return o.Remove(ctx)
}

func (b *RcloneBackend) RemoveAll(ctx context.Context, relPath string) error {
	relPath = cleanRel(relPath)
	if relPath == "" {
		return errors.New("rclone: refuse to purge the remote root")
	}
	b.dirs.Clear()
	defer func() {
		b.objs.drop(relPath)
		b.listCache.invalidateAncestors(relPath)
		b.listCache.invalidateTree(relPath)
	}()
	if err := operations.Purge(ctx, b.f, relPath); err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func (b *RcloneBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	relPath = cleanRel(relPath)
	o, err := b.object(ctx, relPath)
	if err != nil {
		return nil, err
	}
	rc, err := o.Open(ctx)
	if err != nil {
		return nil, err
	}
	return LimitReadCloser(ctx, rc), nil
}

func (b *RcloneBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	relPath = cleanRel(relPath)
	o, err := b.object(ctx, relPath)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		rc, err := o.Open(ctx)
		if err != nil {
			return nil, err
		}
		return LimitReadCloser(ctx, rc), nil
	}
	rc, err := o.Open(ctx, &fs.RangeOption{Start: offset, End: -1})
	if err != nil {
		return nil, err
	}
	return LimitReadCloser(ctx, rc), nil
}

// rclone has no resume-into-a-partial-file primitive: OpenWriterAt opens with
// O_TRUNC (it exists for multi-threaded downloads writing a whole new file),
// so RcloneBackend deliberately does not implement model.Resumer and sc
// recopies interrupted uploads in full. Resuming a download FROM an rclone
// remote works — that is OpenAt above.

// canListRecursive reports whether one call can walk the whole subtree.
// Only ListR does that — ListP is the streaming variant of List and covers a
// single directory, so it is no help here.
func (b *RcloneBackend) canListRecursive() bool {
	return b.feat.ListR != nil
}

// PreloadRecursive streams one recursive listing into the shared list cache.
// ListP is preferred where a remote has it — it delivers batches as they
// arrive instead of buffering the whole tree. Remotes with neither are a
// no-op and fall through to per-directory List.
func (b *RcloneBackend) PreloadRecursive(ctx context.Context, scope string) error {
	scope = cleanRel(scope)
	if !b.canListRecursive() {
		return nil
	}
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

func (b *RcloneBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry), _ func(string)) error {
	scope = strings.Trim(scope, "/")
	Log.Add("rclone", DirOut, "recursive list "+b.f.Name()+":"+path.Join(b.f.Root(), scope))
	var count int
	// Bucket backends return a flat object list from ListR with no directory
	// entries at all, so every ancestor is synthesized from the object paths.
	// seen tracks which ones are already placed; it is only touched from the
	// single preload goroutine. scope is pre-seeded so a scoped preload never
	// emits into directories above it.
	seen := map[string]bool{scope: true}
	cb := func(dirEntries fs.DirEntries) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		byParent := make(map[string][]model.FileEntry)
		// register makes dir resolve as a cache hit even when it has no
		// children, instead of falling through to a live list.
		register := func(dir string) {
			if _, ok := byParent[dir]; !ok {
				byParent[dir] = nil
			}
		}
		for _, e := range b.convert(ctx, dirEntries) {
			if e.IsDir {
				seen[e.RelPath] = true
				register(e.RelPath)
			}
			for d := parentDir(e.RelPath); d != "" && !seen[d]; d = parentDir(d) {
				seen[d] = true
				byParent[parentDir(d)] = append(byParent[parentDir(d)], synthDirEntry(d))
				register(d)
			}
			byParent[parentDir(e.RelPath)] = append(byParent[parentDir(e.RelPath)], e)
		}
		for parent, entries := range byParent {
			emit(parent, entries)
			count += len(entries)
		}
		return nil
	}
	err := b.feat.ListR(ctx, scope, cb)
	if err != nil {
		Log.Add("rclone", DirErr, "recursive list: "+err.Error())
		return err
	}
	Log.Add("rclone", DirIn, fmt.Sprintf("recursive list: %d entries", count))
	return nil
}

// rcObjectInfo is the fs.ObjectInfo sc hands to Put — enough for rclone to
// size and time the upload; the checksum is left to the remote.
type rcObjectInfo struct {
	fs      fs.Fs
	remote  string
	size    int64
	modTime time.Time
}

func (o *rcObjectInfo) Fs() fs.Info                       { return o.fs }
func (o *rcObjectInfo) String() string                    { return o.remote }
func (o *rcObjectInfo) Remote() string                    { return o.remote }
func (o *rcObjectInfo) ModTime(context.Context) time.Time { return o.modTime }
func (o *rcObjectInfo) Size() int64                       { return o.size }
func (o *rcObjectInfo) Storable() bool                    { return true }
func (o *rcObjectInfo) Hash(context.Context, hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// rcObjCache is a two-generation bounded cache. When the live generation
// fills it becomes the spare and a fresh one starts, so lookups stay O(1)
// and memory stays bounded without per-entry bookkeeping.
type rcObjCache struct {
	mu    sync.Mutex
	cur   map[string]fs.Object
	prev  map[string]fs.Object
	limit int
}

func newRcObjCache(limit int) *rcObjCache {
	return &rcObjCache{cur: make(map[string]fs.Object), limit: limit}
}

func (c *rcObjCache) put(rel string, o fs.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cur) >= c.limit {
		c.prev, c.cur = c.cur, make(map[string]fs.Object, c.limit/4)
	}
	c.cur[rel] = o
}

func (c *rcObjCache) get(rel string) (fs.Object, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o, ok := c.cur[rel]; ok {
		return o, true
	}
	o, ok := c.prev[rel]
	if ok {
		c.cur[rel] = o
	}
	return o, ok
}

func (c *rcObjCache) drop(rel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cur, rel)
	delete(c.prev, rel)
}

// rcloneSecretKeys are the rclone option names whose values must never reach
// the UI or the log. Matched as substrings so provider-specific spellings
// (secret_access_key, client_secret, sa_credentials) are all covered.
var rcloneSecretKeys = []string{"pass", "secret", "key", "token", "credential", "auth"}

// unquotedIndex returns the offset of the first ch outside ' or " quotes,
// or -1. rclone allows option values to be quoted precisely so they can
// contain the , and : that otherwise delimit a connection string.
func unquotedIndex(s string, ch byte) int {
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == ch:
			return i
		}
	}
	return -1
}

// splitRcloneOpts separates the option region of a connection string from the
// trailing :path. The leading colon of the :backend,... form is kept with the
// options so the caller can rejoin without special-casing it.
func splitRcloneOpts(spec string) (opts, tail string) {
	lead := ""
	if strings.HasPrefix(spec, ":") {
		lead, spec = ":", spec[1:]
	}
	i := unquotedIndex(spec, ':')
	if i < 0 {
		return lead + spec, ""
	}
	return lead + spec[:i], spec[i:]
}

// maskRcloneSecrets blanks the values of credential options in an rclone
// connection string, leaving the backend, path and other options readable.
func maskRcloneSecrets(spec string) string {
	opts, tail := splitRcloneOpts(spec)
	var out []string
	for len(opts) > 0 {
		part := opts
		if i := unquotedIndex(opts, ','); i >= 0 {
			part, opts = opts[:i], opts[i+1:]
		} else {
			opts = ""
		}
		if k, _, ok := strings.Cut(part, "="); ok && isRcloneSecret(k) {
			part = k + "=xxxxx"
		}
		out = append(out, part)
	}
	return strings.Join(out, ",") + tail
}

func isRcloneSecret(key string) bool {
	k := strings.ToLower(key)
	for _, s := range rcloneSecretKeys {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// rcloneTimeRes is how exact a remote's listed mtimes are. With
// UseServerModTime on a remote whose own mtime costs a request per file
// (S3, Swift: SlowModTime), the listed time is the upload's, which no copy
// sets: no time of the file's, so the compare leaves it out. It was
// compared, and every file copied there was "different" after the copy
// (L17).
func rcloneTimeRes(uploadTime bool, precision time.Duration) model.TimeRes {
	if uploadTime {
		return model.TimeNone
	}
	return model.TimeResOf(precision)
}

// synthDirEntry is a directory sc invents for a path that only ever appeared
// as a prefix of an object key. Bucket remotes have no directory objects, so
// there is no mtime to report.
func synthDirEntry(relPath string) model.FileEntry {
	return model.FileEntry{
		RelPath: relPath,
		Name:    path.Base(relPath),
		IsDir:   true,
		Mode:    os.ModeDir | 0755,
	}
}

// isFalse reports whether an env var was explicitly set to a falsey value.
// An unset or empty variable is not false — it leaves the default in place.
func isFalse(v string) bool {
	switch strings.ToLower(v) {
	case "0", "false", "no", "off":
		return true
	}
	return false
}
