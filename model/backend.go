package model

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
)

type FileEntry struct {
	RelPath   string
	Name      string
	Size      int64
	ModTime   time.Time
	ATime     time.Time
	CTime     time.Time
	BirthTime time.Time
	IsDir     bool
	Mode      os.FileMode
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
type Backend interface {
	BasePath() string
	List(ctx context.Context, relDir string) ([]FileEntry, error)
	Stat(ctx context.Context, relPath string) (FileEntry, error)
	Checksum(ctx context.Context, relPath string) (string, error)
	SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error
	CopyFrom(ctx context.Context, relPath string, src io.Reader, p PutParams) error
	Mkdir(ctx context.Context, relPath string, mode os.FileMode) error
	Rename(ctx context.Context, oldRelPath, newRelPath string) error
	Remove(ctx context.Context, relPath string) error
	RemoveAll(ctx context.Context, relPath string) error
	Open(ctx context.Context, relPath string) (io.ReadCloser, error)
	Close() error
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
	// permission bits are used.
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

// ChecksumProber is implemented by backends that support content checksums.
type ChecksumProber interface {
	ProbeChecksums() []string
	SetChecksumAlgo(algo string)
}

// ChecksumPrefetcher is implemented by backends where a bulk fetch is cheaper
// than per-file Checksum calls (e.g. rsync MD4 returns checksums via filelist).
// scope is a relPath under the backend's base; "" means whole base.
// recursive is true when scope refers to a directory subtree.
type ChecksumPrefetcher interface {
	PrefetchChecksums(ctx context.Context, scope string, recursive bool) error
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
	// is not.
	CommitPart(ctx context.Context, relPath string, mode os.FileMode) error
}

// ErrPartInTheWay is PartSize's error for something other than a file at a
// part's name, such as a link. The name belongs to sc, so the engine removes
// it and copies from the start.
var ErrPartInTheWay = errors.New("not a part file")

// RegionChecksummer is implemented by backends that can checksum n bytes of a
// file from off, with the algorithm Checksum uses. Before resuming into a
// part the engine compares its bytes with the same bytes of the source: a
// part is never appended to on its size alone.
type RegionChecksummer interface {
	ChecksumRange(ctx context.Context, relPath string, off, n int64) (string, error)
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

// BatchSender is implemented by destination backends that can transfer an
// entire local subtree in one protocol session (e.g. rsync streaming all
// files over a single SSH channel) instead of N per-file calls. The receiver
// recreates the tree under its base + relPath.
//
// onFile fires once per file as the sender begins it, with the path relative
// to srcRoot. The implementation MUST credit progress.Bytes through the
// context counter as data flows; the caller seeds Total/TotalBytes before the
// call and does not top-up afterwards. --ignore-times semantics are assumed:
// every file in the source tree is transferred unconditionally so files the
// caller previously flagged as different can never be silently skipped by an
// mtime+size match.
type BatchSender interface {
	SendLocalTree(ctx context.Context, srcRoot, relPath string, onFile func(name string)) error
}

// BackendRangeOpener adapts a Backend + relPath into a RangeOpener. OpenAt
// uses the backend's SeekableOpener path when available, otherwise opens
// from byte 0 and discards the prefix.
type BackendRangeOpener struct {
	Backend  Backend
	RelPath  string
	FileSize int64
}

func (r *BackendRangeOpener) Size() int64 { return r.FileSize }

func (r *BackendRangeOpener) LocalPath() string {
	if lp, ok := r.Backend.(LocalFS); ok {
		return lp.LocalPath(r.RelPath)
	}
	return ""
}

func (r *BackendRangeOpener) Open(ctx context.Context) (io.ReadCloser, error) {
	return r.Backend.Open(ctx, r.RelPath)
}

func (r *BackendRangeOpener) OpenAt(ctx context.Context, offset int64) (io.ReadCloser, error) {
	if offset == 0 {
		return r.Backend.Open(ctx, r.RelPath)
	}
	if seeker, ok := r.Backend.(SeekableOpener); ok {
		rd, err := seeker.OpenAt(ctx, r.RelPath, offset)
		if err == nil {
			return rd, nil
		}
		// Fall back below on any error (notably ErrUnsupported).
	}
	rd, err := r.Backend.Open(ctx, r.RelPath)
	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(io.Discard, rd, offset); err != nil {
		rd.Close()
		return nil, err
	}
	return rd, nil
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
