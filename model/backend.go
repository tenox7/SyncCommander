package model

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

// A FileEntry is one listed file or folder; a tree keeps two for each node.
// ModTime, which every compare reads, is a time.Time; the access, change
// and birth times, rarely compared, take a third of the room (F13).
type FileEntry struct {
	RelPath   string
	Name      string
	Size      int64
	ModTime   time.Time
	ATime     UnixNano
	CTime     UnixNano
	BirthTime UnixNano
	IsDir     bool
	// TimeRes is how exact ModTime is as the backend keeps it (L17). It
	// takes no room: it fills the gap before Mode.
	TimeRes TimeRes
	Mode    os.FileMode
}

// IsFileName reports whether name can be the name of one entry in a
// folder: one path part, so that joined onto the folder it names that entry
// and nothing else. A server sends names in its listings as it likes, and
// "../../.ssh/authorized_keys" joined onto a folder points outside it; on a
// server that runs Windows, which takes a backslash for a slash too,
// "..\..\x" does (L7). A backslash is kept otherwise: on Unix it is a
// character of the name.
func IsFileName(name string) bool {
	if name == "" || name == "." || strings.ContainsAny(name, "/\x00") {
		return false
	}
	// "..", and ".." between backslashes.
	for part := range strings.SplitSeq(name, `\`) {
		if part == ".." {
			return false
		}
	}
	return true
}

// IsSpecial reports an entry that is neither a folder nor a regular file: a
// symbolic link, a named pipe, a socket or a device; or a part file of an
// unfinished copy (IsPartName). It is listed, so the tree shows it and a
// same-name file or folder on the other side is a type conflict, but it is
// never read, summed or copied: a link leads out of the tree, opening a pipe
// waits for a writer (L2, L9), and a part holds half a file (L14).
func (e *FileEntry) IsSpecial() bool {
	return !e.IsDir && (e.Mode&os.ModeType != 0 || IsPartName(e.Name))
}

// IsPart reports a part file sc writes a copy into (PartName).
func (e *FileEntry) IsPart() bool { return !e.IsDir && e.Mode&os.ModeType == 0 && IsPartName(e.Name) }

// Kind names what the entry is, for messages.
func (e *FileEntry) Kind() string {
	switch {
	case e.IsDir:
		return "folder"
	case e.IsPart():
		return "partial copy"
	}
	return KindName(e.Mode)
}

// KindName names what an entry of mode m is, for messages.
func KindName(m os.FileMode) string {
	switch {
	case m&os.ModeDir != 0:
		return "folder"
	case m&os.ModeSymlink != 0:
		return "symbolic link"
	case m&os.ModeNamedPipe != 0:
		return "named pipe"
	case m&os.ModeSocket != 0:
		return "socket"
	case m&os.ModeDevice != 0:
		return "device"
	case m&os.ModeType != 0:
		return "special file"
	}
	return "file"
}

// Backend is one side of a sync. List returns every entry of relDir, or an
// error when it cannot vouch for that: a listing that is short without an
// error makes mirror delete what it left out. Under a context from
// WithFreshListing, List asks the server and never answers from a cache.
//
// Stat returns the entry of relPath alone, as the server holds it now: one
// path, never from a listing or its cache, the same row a listing gives. A
// link is reported as a link, not followed (lstat). Nothing at relPath is
// fs.ErrNotExist. A part file is not the file: Stat looks at the final name
// only (D1, F18: one file's size used to cost a listing of its folder, and
// came from a cache that could be hours old).
//
// Close lets go of what the backend holds: its connections, sessions and
// pools, and work the server keeps for it until then (an rclone remote's
// Shutdown). Nothing else is called on the backend after it. A second
// Close does no harm: a pane closes the backends it replaces, and the
// program closes the ones it started with on exit. It was found by type
// assertion, which a wrapper that embeds Backend hid (D1).
//
// Capabilities says what the backend can do beyond these methods (Caps). A
// remote's says what the server it connects to can do, so it may connect,
// and it fails when it cannot: a caller must not guess, or a copy that
// should go through a part file would write the real file directly.
//
// Checksum sums relPath with algo, one of the names ChecksumAlgos gives;
// another name is an error. The caller says which algorithm on every call:
// it used to be set inside the backend (SetChecksumAlgo), where a new
// connection did not have it and a wrapper had to replay it (D2, F6).
// ChecksumAlgos may ask the server (an ssh server's commands, a WebDAV
// server's properties), so it is asked only when a checksum is wanted, and
// a backend keeps the answer for its connection.
type Backend interface {
	BasePath() string
	List(ctx context.Context, relDir string) ([]FileEntry, error)
	Stat(ctx context.Context, relPath string) (FileEntry, error)
	Checksum(ctx context.Context, relPath, algo string) (string, error)
	ChecksumAlgos(ctx context.Context) ([]string, error)
	SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error
	CopyFrom(ctx context.Context, relPath string, src io.Reader, p PutParams) error
	Mkdir(ctx context.Context, relPath string, mode os.FileMode) error
	Rename(ctx context.Context, oldRelPath, newRelPath string) error
	Remove(ctx context.Context, relPath string) error
	RemoveAll(ctx context.Context, relPath string) error
	Open(ctx context.Context, relPath string) (io.ReadCloser, error)
	Capabilities(ctx context.Context) (Caps, error)
	Close() error
}

// Caps is what a backend can do beyond Backend. Each flag names an optional
// interface below; a caller decides from the flag, then type-asserts to
// make the call. The reconnecting wrapper that holds every remote has the
// methods of every optional interface, so its method set says nothing: it
// says what the backend it connects to can do. Found by type assertion,
// each was a "yes" for every remote and an ErrUnsupported at run time,
// which every caller had to check again; a batch copy walked the whole
// local folder before it learnt that the destination takes no batch (D2,
// F20).
type Caps struct {
	Resume       bool // Resumer
	Parts        bool // PartWriter
	RegionSums   bool // RegionChecksummer
	OpenAt       bool // SeekableOpener
	SendLocal    bool // LocalSender
	RecvLocal    bool // LocalReceiver
	Batch        bool // BatchSender
	Preload      bool // RecursivePreloader
	PrefetchSums bool // ChecksumPrefetcher
	BatchSums    bool // ChecksumBatcher
	BatchRemove  bool // BatchRemover
	// ListedSums: a prefetch is a listing, which carries the sums (rclone
	// on S3, GCS, B2, Drive and the like), and reads no file. It pays for
	// the files right in a folder too, but not for a few files of many
	// (L20).
	ListedSums bool
	// OwnsProgress: CopyFrom counts the bytes it sends against the context's
	// progress counter itself (rsync), so the engine must not count them
	// again (F21).
	OwnsProgress bool
	// RedialsItself: the backend makes its own connections again, so a
	// lost one fails the call it met, never the backend. An HTTP client's
	// every call is a request, and rclone's backends redial for themselves;
	// the reconnecting wrapper keeps such a backend, and its caches (L19).
	RedialsItself bool
	// Names is how the file system under the root matches names. A local
	// root finds it out; a remote does not know it yet.
	Names NameRule
}

// PutParams is what a backend is told about a file it takes. Size and
// ModTime travelled in the context, where the compiler cannot see a caller
// or a wrapper leave one out (D1).
type PutParams struct {
	// Size is the number of bytes src gives. 0 or less is taken as not
	// known: a backend that needs the size up front (a Content-Length, an
	// object store's put) measures the data or streams it. A caller that
	// leaves the size out costs a spool, never a short file.
	Size int64
	// Mode is the new file's mode, for a backend that sets one; its
	// permission bits are used. A backend that writes the file but cannot
	// give it the mode returns an error that wraps ErrModeNotSet: the
	// bytes are in place.
	Mode os.FileMode
	// ModTime is the source's modification time, zero when not known. A
	// backend that stores it with the data does; SetTimes follows anyway.
	ModTime time.Time
}

// SizeKnown reports whether Size says how many bytes src gives.
func (p PutParams) SizeKnown() bool { return p.Size > 0 }

type freshListingKey struct{}

// WithFreshListing returns a context under which Backend.List lists the
// folder on the server, not from a cache a preload filled, and
// RecursivePreloader.PreloadRecursive starts a new recursive listing on an
// empty cache even while one runs. A delete decided from listings checks
// the folder this way first; a rescan lists this way, so it shows what the
// server holds now (W-H4).
func WithFreshListing(ctx context.Context) context.Context {
	return context.WithValue(ctx, freshListingKey{}, true)
}

// WantsFreshListing reports whether ctx came from WithFreshListing.
func WantsFreshListing(ctx context.Context) bool {
	fresh, _ := ctx.Value(freshListingKey{}).(bool)
	return fresh
}

// ChecksumPrefetcher is implemented by backends where a bulk fetch is cheaper
// than per-file Checksum calls (e.g. rsync MD4 returns checksums via filelist).
// scope is a relPath under the backend's base; "" means whole base.
// recursive is true when scope refers to a directory subtree. algo is the
// algorithm asked for; one the backend cannot fetch in bulk is no error, and
// nothing is fetched. The sums come back by relPath, for the caller to use
// now: the backend keeps none, so a later Checksum reads the file as it is
// then (R11).
type ChecksumPrefetcher interface {
	PrefetchChecksums(ctx context.Context, scope, algo string, recursive bool) (map[string]string, error)
}

// ChecksumBatcher is implemented by backends that sum many files with one
// request, where Checksum is a round trip for each file: hours for a
// million small files at 50 ms (S19). It sums the files rels names,
// relPaths under the backend's base, with algo, and gives the sums by
// relPath for the caller to use now; as with a prefetch, the backend keeps
// none. A file it gives no sum for, because it cannot be read, is not a
// file now or the request ended first, the caller sums by itself, where
// the error shows. How many files one call takes is the caller's choice.
type ChecksumBatcher interface {
	ChecksumFiles(ctx context.Context, rels []string, algo string) (map[string]string, error)
}

// RangeOpener is a handle to a source file the destination can open from byte
// 0 or from an arbitrary offset. Used by Resumer.AppendFrom so each backend
// can pick the access pattern that fits its protocol — cat-style backends
// open only the tail (OpenAt(offset)); rsync wants the full file (Open).
//
// LocalPath returns a path on the local filesystem if the source backend is
// LocalFS, or "" otherwise. Rsync-style destinations use this to send
// directly from disk without staging a tmpfile copy.
type RangeOpener interface {
	Open(ctx context.Context) (io.ReadCloser, error)
	OpenAt(ctx context.Context, offset int64) (io.ReadCloser, error)
	Size() int64
	LocalPath() string
}

// PartWriter is implemented by destinations that take a copy into a part
// file next to the real one, PartPath(relPath), and rename it over the real
// file only once it is complete and checked. The real file never holds half
// a copy, an overwrite cut off half way leaves the old version, and a link
// at the name is replaced, never written through (review L14, S4).
//
// The engine writes the part with CopyFrom and, to resume, AppendFrom: an
// AppendFrom must never create its file and must fail unless the file holds
// exactly offset bytes, so a resume never fills a gap with zeros.
type PartWriter interface {
	// PartSize returns the size of relPath's part now, from one stat of
	// that path (not a listing, which may be cached): 0 when there is
	// none, an error when something other than a file has its name.
	PartSize(ctx context.Context, relPath string) (int64, error)
	// CommitPart makes relPath's part durable, gives it mode, and renames
	// it over relPath. A file or a link at relPath is replaced, a folder
	// is not. A mode the destination will not take does not keep the part
	// from its name: the part is renamed, and the error wraps
	// ErrModeNotSet.
	CommitPart(ctx context.Context, relPath string, mode os.FileMode) error
}

// ErrPartInTheWay is PartSize's error for something other than a file at a
// part's name, such as a link. The name belongs to sc, so the engine removes
// it and copies from the start.
var ErrPartInTheWay = errors.New("not a part file")

// ErrModeNotSet is a write or a commit that put the file in place but could
// not give it its mode: an SFTP server that refuses SETSTAT, a disk with no
// modes. The copy is no failure, as one whose times could not be set is
// none: it is counted, and never sent again. A failed chmod failed the
// copy, which sent the file up to six times and could leave it under its
// part's name (S20).
var ErrModeNotSet = errors.New("mode not set")

// RegionChecksummer is implemented by backends that can checksum n bytes of a
// file from off with algo, as Checksum sums a whole file. Before resuming
// into a part the engine compares its bytes with the same bytes of the
// source: a part is never appended to on its size alone.
type RegionChecksummer interface {
	ChecksumRange(ctx context.Context, relPath, algo string, off, n int64) (string, error)
}

// Resumer is implemented by backends that can resume an interrupted upload by
// continuing from the offset that already exists at the destination.
//
// src exposes the source file for whichever access pattern fits the backend:
// streaming backends (cat-style append) call src.OpenAt(offset) to read only
// the tail; protocol-aware backends (rsync --append) call src.Open() and let
// the protocol negotiate the prefix.
type Resumer interface {
	AppendFrom(ctx context.Context, relPath string, src RangeOpener, p PutParams, offset int64) error
}

// SeekableOpener is implemented by backends that can open a file for reading
// from an arbitrary byte offset, avoiding wasted bandwidth when resuming.
type SeekableOpener interface {
	OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error)
}

// LocalFS is implemented by backends backed by a local filesystem path. Other
// backends can pair with these to perform direct path-to-path transfers (e.g.
// rsync directly from a local source or to a local destination), avoiding any
// intermediate tmp file. A network mount that the OS exposes as a normal path
// counts as local for this purpose.
type LocalFS interface {
	LocalPath(relPath string) string
}

// LocalSender is implemented by backends that can ingest a file directly from
// a local filesystem path, bypassing the io.Reader streaming flow and any
// intermediate tmp file (e.g. rsync invoked with the local path as source).
//
// Implementations MUST credit progress.Bytes during the push and top up to
// p.Size on success, so the stall guard sees movement on long transfers and
// the byte counter ends correctly even under delta-sync. On error the
// implementation MUST roll back any in-band credit. The caller does not
// top-up after the call. See RsyncSSHBackend.SendLocalFile for the pattern.
type LocalSender interface {
	SendLocalFile(ctx context.Context, srcPath, relPath string, p PutParams) error
}

// LocalReceiver is implemented by backends that can write directly to a local
// filesystem path, bypassing io.ReadCloser streaming and any intermediate tmp
// file (e.g. rsync invoked with the local path as destination).
type LocalReceiver interface {
	RecvToLocalFile(ctx context.Context, relPath, dstPath string) error
}

type listTimeoutKey struct{}

// listTimeout is what WithListTimeout put in a ctx.
type listTimeout struct {
	parent   context.Context
	d        time.Duration
	deadline time.Time
}

// WithListTimeout is context.WithTimeout(parent, d) for one folder
// listing. A backend that first waits for a recursive listing of its own
// to reach the folder does not count the wait as the listing's time: the
// wait ends when the caller stops (ListWait), and a listing it makes
// after the wait gets d from then on (W-M2).
func WithListTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, d)
	dl, _ := ctx.Deadline()
	return context.WithValue(ctx, listTimeoutKey{}, listTimeout{parent, d, dl}), cancel
}

// ListWait is the ctx of a wait before a listing made with ctx. It has
// ctx's values, and it ends when ctx ends, but not by the timeout
// WithListTimeout set, which is for the listing. d is that timeout, 0 for
// none; then the wait is ctx itself. stop releases the wait's ctx.
func ListWait(ctx context.Context) (wait context.Context, d time.Duration, stop func()) {
	lt, ok := ctx.Value(listTimeoutKey{}).(listTimeout)
	if !ok {
		return ctx, 0, func() {}
	}
	wait, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	stopParent := context.AfterFunc(lt.parent, func() { cancel(context.Cause(lt.parent)) })
	stopCtx := context.AfterFunc(ctx, func() {
		if dl, _ := ctx.Deadline(); errors.Is(ctx.Err(), context.DeadlineExceeded) && dl.Equal(lt.deadline) {
			return // the listing's own timeout
		}
		cancel(context.Cause(ctx))
	})
	return wait, lt.d, func() {
		stopParent()
		stopCtx()
		cancel(context.Canceled)
	}
}

type hashBytesKey struct{}

// WithHashBytes says that the far side hashes about n bytes for the call
// made with ctx before it answers in full: a file's checksum, or those of a
// folder's files. A backend that fails a connection gone silent allows the
// time that takes (R17). n <= 0 says nothing.
func WithHashBytes(ctx context.Context, n int64) context.Context {
	if n <= 0 {
		return ctx
	}
	return context.WithValue(ctx, hashBytesKey{}, n)
}

// HashBytes is what WithHashBytes said for ctx, 0 for nothing.
func HashBytes(ctx context.Context) int64 {
	n, _ := ctx.Value(hashBytesKey{}).(int64)
	return n
}

// BatchSender is implemented by destination backends that can transfer
// many local paths in one protocol session (e.g. rsync streaming all files
// over a single SSH channel) instead of N per-file calls. The receiver
// recreates them under its base + relPath.
//
// files are what to send: paths relative to the base, as relPath, and under
// relPath; files, and folders to create. The session sends those and the
// folders above them, and nothing else of relPath (F20). An empty list
// sends nothing.
//
// onFile fires once per file as the sender begins it, with the path relative
// to relPath. The implementation MUST credit progress.Bytes through the
// context counter as data flows; the caller seeds Total/TotalBytes before the
// call and does not top-up afterwards. A listed file is never skipped on an
// mtime+size match: the caller listed it because it differs.
type BatchSender interface {
	SendLocalTree(ctx context.Context, srcRoot, relPath string, files []string, onFile func(name string)) error
}

// BatchRemover is implemented by backends that remove several entries of
// one folder in one round trip. An rsync daemon has no delete: each is a
// --delete run, which lists the folder, so a mirror that deleted V of a
// folder's N entries one by one listed it V times (R16).
type BatchRemover interface {
	// RemoveIn removes the entries of dir that items name: a file as
	// Remove does, a folder with what it holds, as RemoveAll does. An
	// error says some may still be there: a listing of dir tells which.
	RemoveIn(ctx context.Context, dir string, items []RemoveItem) error
}

// RemoveItem is an entry RemoveIn removes: its name in the folder, and
// whether it is a folder, removed with what it holds.
type RemoveItem struct {
	Name  string
	IsDir bool
}

// RecursivePreloader is implemented by backends where one recursive listing
// call is dramatically cheaper than per-directory List calls (e.g. rsync over
// the network). When triggered, the backend kicks off a single recursive list
// in the background and progressively populates an internal cache. Subsequent
// List calls for any directory under scope are served from the cache as the
// stream arrives. scope is a relPath under the backend's base; "" means whole
// base. Returns immediately; the preload runs asynchronously.
type RecursivePreloader interface {
	PreloadRecursive(ctx context.Context, scope string) error
}

type LivenessManaged interface {
	ManagesLiveness() bool
}
