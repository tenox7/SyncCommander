package transfer

import (
	"context"
	"io/fs"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"sc/model"
	"sc/transport"
)

// statByListing answers Stat for a stub whose listing is the truth.
func statByListing(ctx context.Context, b model.Backend, relPath string) (model.FileEntry, error) {
	entries, err := b.List(ctx, model.DirOf(relPath))
	if err != nil {
		return model.FileEntry{}, err
	}
	for _, e := range entries {
		if e.Name == path.Base(relPath) {
			return e, nil
		}
	}
	return model.FileEntry{}, fs.ErrNotExist
}

// liveStub is a destination whose listing is stale, as a cached one is:
// it shows f.bin with the listed bytes the scan saw, while the file holds
// what size says now, and is a link when kind says so. It counts listings
// and keeps the offset of the last append.
type liveStub struct {
	sumStub
	listed int64
	kind   os.FileMode
	lists  atomic.Int64
	offset atomic.Int64
}

func (s *liveStub) List(context.Context, string) ([]model.FileEntry, error) {
	s.lists.Add(1)
	return []model.FileEntry{{Name: "f.bin", RelPath: "f.bin", Size: s.listed, Mode: 0o644}}, nil
}

func (s *liveStub) Stat(_ context.Context, relPath string) (model.FileEntry, error) {
	if relPath != "f.bin" {
		return model.FileEntry{}, fs.ErrNotExist
	}
	return model.FileEntry{Name: "f.bin", RelPath: "f.bin", Size: s.size.Load(), Mode: s.kind | 0o644}, nil
}

func (s *liveStub) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	s.offset.Store(offset)
	return s.sumStub.AppendFrom(ctx, relPath, src, mode, offset)
}

// livePair is a source of body and a destination that holds have bytes of
// it while its listing still says listed.
func livePair(t *testing.T, body string, listed, have int, kind os.FileMode) (*scenario, *liveStub) {
	t.Helper()
	src := &sumStub{body: body}
	src.size.Store(int64(len(body)))
	dst := &liveStub{listed: int64(listed), kind: kind}
	dst.body = body[:have]
	dst.size.Store(int64(have))
	lazy := transport.NewLazyBackend("sftp://host/dst", func(context.Context) (model.Backend, error) { return dst, nil })
	t.Cleanup(func() { transport.CloseBackend(lazy) })
	return scanned(t, "", "", src, lazy), dst
}

// F18, D1: the size a resume appends at was read from a listing of the
// file's folder: a folder of 500,000 files listed for one size, and, after
// a scan, a listing from the cache, as old as the scan. The append then
// started where the file used to end. It now comes from one Stat.
func TestResumeStartsAtTheLiveSize(t *testing.T) {
	s, dst := livePair(t, "0123456789abcdefghijklmnopqrstuvwxyz", 10, 20, 0)
	dst.lists.Store(0)
	p := s.copy(t, "f.bin", true, false)
	if dst.appends.Load() != 1 || dst.offset.Load() != 20 {
		t.Errorf("%d appends at %d; want one at 20, the size the file has now, not the 10 its old listing shows",
			dst.appends.Load(), dst.offset.Load())
	}
	if n := dst.lists.Load(); n != 0 {
		t.Errorf("the copy listed the destination folder %d times to learn one size, want 0", n)
	}
	if p.Failed.Load() != 0 {
		t.Errorf("%d failed", p.Failed.Load())
	}
}

// F18, D1: what is at the name now may be a link, though the scan saw a
// file. A size taken from the listing was the link's own size, and the
// append went through the link. Only a file is resumed.
func TestResumeDoesNotAppendThroughALink(t *testing.T) {
	s, dst := livePair(t, "0123456789abcdefghijklmnopqrstuvwxyz", 10, 10, os.ModeSymlink)
	s.copy(t, "f.bin", true, false)
	if dst.appends.Load() != 0 || dst.fullCopies.Load() != 1 {
		t.Errorf("%d appends, %d full copies onto a link; want no append, one full copy",
			dst.appends.Load(), dst.fullCopies.Load())
	}
}

// oneStub holds one entry, f.bin, as its listing and its Stat agree.
type oneStub struct {
	sumStub
	e model.FileEntry
}

func (s *oneStub) List(context.Context, string) ([]model.FileEntry, error) {
	return []model.FileEntry{s.e}, nil
}
func (s *oneStub) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	return statByListing(ctx, s, relPath)
}

// The live size is a regular file's size. Anything else, a folder, a link,
// a pipe, nothing, has no bytes to resume after.
func TestLiveSizeIsAFilesSize(t *testing.T) {
	for _, tc := range []struct {
		what string
		e    model.FileEntry
		want int64
	}{
		{"file", model.FileEntry{Name: "f.bin", Size: 7, Mode: 0o644}, 7},
		{"folder", model.FileEntry{Name: "f.bin", Size: 4096, IsDir: true, Mode: os.ModeDir | 0o755}, 0},
		{"link", model.FileEntry{Name: "f.bin", Size: 11, Mode: os.ModeSymlink | 0o777}, 0},
		{"pipe", model.FileEntry{Name: "f.bin", Size: 3, Mode: os.ModeNamedPipe | 0o644}, 0},
		{"nothing", model.FileEntry{Name: "other", Size: 5, Mode: 0o644}, 0},
	} {
		tc.e.RelPath = tc.e.Name
		if got := peekDstSize(context.Background(), &oneStub{e: tc.e}, "f.bin"); got != tc.want {
			t.Errorf("%s: live size %d, want %d", tc.what, got, tc.want)
		}
	}
}
