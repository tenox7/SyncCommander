package model

import (
	"context"
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

type Backend interface {
	BasePath() string
	List(ctx context.Context, relDir string) ([]FileEntry, error)
	Checksum(ctx context.Context, relPath string) (string, error)
	SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error
	CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error
	Mkdir(ctx context.Context, relPath string, mode os.FileMode) error
	Rename(ctx context.Context, oldRelPath, newRelPath string) error
	Remove(ctx context.Context, relPath string) error
	RemoveAll(ctx context.Context, relPath string) error
	Open(ctx context.Context, relPath string) (io.ReadCloser, error)
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

// Resumer is implemented by backends that can resume an interrupted upload by
// continuing from the offset that already exists at the destination.
//
// src exposes the source file for whichever access pattern fits the backend:
// streaming backends (cat-style append) call src.OpenAt(offset) to read only
// the tail; protocol-aware backends (rsync --append) call src.Open() and let
// the protocol negotiate the prefix.
type Resumer interface {
	AppendFrom(ctx context.Context, relPath string, src RangeOpener, mode os.FileMode, offset int64) error
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
// fileSize on success, so the stall guard sees movement on long transfers and
// the byte counter ends correctly even under delta-sync. On error the
// implementation MUST roll back any in-band credit. The caller does not
// top-up after the call. See RsyncSSHBackend.SendLocalFile for the pattern.
type LocalSender interface {
	SendLocalFile(ctx context.Context, srcPath, relPath string, mode os.FileMode) error
}

// LocalReceiver is implemented by backends that can write directly to a local
// filesystem path, bypassing io.ReadCloser streaming and any intermediate tmp
// file (e.g. rsync invoked with the local path as destination).
type LocalReceiver interface {
	RecvToLocalFile(ctx context.Context, relPath, dstPath string) error
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
		// Fall back below on any error (notably ErrResumeUnsupported).
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
