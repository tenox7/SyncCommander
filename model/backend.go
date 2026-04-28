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

// Resumer is implemented by backends that can resume an interrupted upload by
// appending to an existing partial destination file. src must be positioned at
// the byte that follows the last byte already present at the destination.
type Resumer interface {
	AppendFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode, offset int64) error
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
type LocalSender interface {
	SendLocalFile(ctx context.Context, srcPath, relPath string, mode os.FileMode) error
}

// LocalReceiver is implemented by backends that can write directly to a local
// filesystem path, bypassing io.ReadCloser streaming and any intermediate tmp
// file (e.g. rsync invoked with the local path as destination).
type LocalReceiver interface {
	RecvToLocalFile(ctx context.Context, relPath, dstPath string) error
}
