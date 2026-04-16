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
