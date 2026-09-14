package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// resumeStub is a destination that can append, plus a source to append from.
// dstSize is what List reports; sum is what Checksum reports.
type resumeStub struct {
	body       string
	dstSize    int64
	sum        string
	appends    int
	appendFrom int64
}

func (s *resumeStub) BasePath() string { return "/stub" }

func (s *resumeStub) List(_ context.Context, _ string) ([]model.FileEntry, error) {
	return []model.FileEntry{{Name: "f.bin", RelPath: "f.bin", Size: s.dstSize}}, nil
}

func (s *resumeStub) Checksum(context.Context, string) (string, error) { return s.sum, nil }
func (s *resumeStub) ProbeChecksums() []string                         { return []string{"xxh3"} }
func (s *resumeStub) SetChecksumAlgo(string)                           {}
func (s *resumeStub) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}

func (s *resumeStub) CopyFrom(_ context.Context, _ string, src io.Reader, _ os.FileMode) error {
	_, err := io.Copy(io.Discard, src)
	return err
}

func (s *resumeStub) Mkdir(context.Context, string, os.FileMode) error { return nil }
func (s *resumeStub) Rename(context.Context, string, string) error     { return nil }
func (s *resumeStub) Remove(context.Context, string) error             { return nil }
func (s *resumeStub) RemoveAll(context.Context, string) error          { return nil }

func (s *resumeStub) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.body)), nil
}

func (s *resumeStub) AppendFrom(ctx context.Context, _ string, src model.RangeOpener, _ os.FileMode, offset int64) error {
	s.appends++
	s.appendFrom = offset
	rc, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rc.Close()
	n, err := io.Copy(io.Discard, rc)
	s.dstSize = offset + n
	return err
}

const srcBody = "0123456789abcdef"

func newResumePair(dstSize int64, srcSum, dstSum string) (*resumeStub, *resumeStub) {
	src := &resumeStub{body: srcBody, dstSize: int64(len(srcBody)), sum: srcSum}
	dst := &resumeStub{body: srcBody[:dstSize], dstSize: dstSize, sum: dstSum}
	return src, dst
}

func srcEntry() *model.FileEntry {
	return &model.FileEntry{Name: "f.bin", RelPath: "f.bin", Size: int64(len(srcBody))}
}

// Resume must start from the destination's live size, not the size the scan
// recorded — an external partial write between scan and copy would otherwise
// append at the wrong offset and corrupt the file.
func TestTryResumeCopyUsesLiveDestinationSize(t *testing.T) {
	src, dst := newResumePair(10, "same", "same")
	// Tree still shows the stale 4-byte partial from scan time.
	stale := &model.FileEntry{Name: "f.bin", RelPath: "f.bin", Size: 4}

	var bytes, base atomic.Int64
	if !tryResumeCopy(context.Background(), src, dst, "f.bin", srcEntry(), stale, &bytes, &base, nil) {
		t.Fatal("tryResumeCopy = false, want a successful resume")
	}
	if dst.appendFrom != 10 {
		t.Errorf("appended from offset %d, want 10 (the live size)", dst.appendFrom)
	}
}

// A destination that has since grown to full size is not resumable: fall back
// to a full copy rather than appending past the end.
func TestTryResumeCopySkipsWhenDestinationAlreadyFull(t *testing.T) {
	src, dst := newResumePair(int64(len(srcBody)), "same", "same")
	stale := &model.FileEntry{Name: "f.bin", RelPath: "f.bin", Size: 4}

	var bytes, base atomic.Int64
	if tryResumeCopy(context.Background(), src, dst, "f.bin", srcEntry(), stale, &bytes, &base, nil) {
		t.Fatal("tryResumeCopy = true, want a fall-back to full copy")
	}
	if dst.appends != 0 {
		t.Errorf("appends = %d, want 0", dst.appends)
	}
}

// Resume trusts the prefix already at the destination. When verification says
// the result diverges, the resume must be rejected so the caller recopies in
// full, and the progress credited during the append must be rolled back.
func TestResumeVerifyMismatchRejectsAndRollsBack(t *testing.T) {
	src, dst := newResumePair(10, "aaa", "bbb")

	var bytes, base atomic.Int64
	scanner := model.NewScanner(src, dst, 1, 1, true)
	verify := resumeVerifier(true, scanner, src, dst, "f.bin", int64(len(srcBody)))
	if verify == nil {
		t.Fatal("resumeVerifier = nil with verification enabled")
	}

	err := resumeAttempt(context.Background(), src, dst, "f.bin", srcEntry(), 10, &bytes, &base, verify)
	if !errors.Is(err, errResumeMismatch) {
		t.Fatalf("resumeAttempt = %v, want errResumeMismatch", err)
	}
	if bytes.Load() != 0 || base.Load() != 0 {
		t.Errorf("progress not rolled back: bytes=%d base=%d", bytes.Load(), base.Load())
	}
}

func TestResumeVerifyMatchAccepts(t *testing.T) {
	src, dst := newResumePair(10, "same", "same")

	var bytes, base atomic.Int64
	scanner := model.NewScanner(src, dst, 1, 1, true)
	verify := resumeVerifier(true, scanner, src, dst, "f.bin", int64(len(srcBody)))

	if err := resumeAttempt(context.Background(), src, dst, "f.bin", srcEntry(), 10, &bytes, &base, verify); err != nil {
		t.Fatalf("resumeAttempt = %v, want nil", err)
	}
	if got := bytes.Load(); got != int64(len(srcBody)) {
		t.Errorf("bytes = %d, want %d", got, len(srcBody))
	}
	if got := base.Load(); got != 10 {
		t.Errorf("base = %d, want 10 (the pre-existing prefix)", got)
	}
}

func TestResumeVerifierDisabled(t *testing.T) {
	src, dst := newResumePair(10, "aaa", "bbb")
	scanner := model.NewScanner(src, dst, 1, 1, true)
	if resumeVerifier(false, scanner, src, dst, "f.bin", int64(len(srcBody))) != nil {
		t.Fatal("resumeVerifier returned a check while verification is off")
	}
}

func TestResumeMismatchIsTreatedLikeUnsupported(t *testing.T) {
	// The copy loop falls back to a full copy for both sentinels; assert they
	// stay distinguishable from a generic transfer error.
	if errors.Is(errResumeMismatch, transport.ErrUnsupported) {
		t.Error("errResumeMismatch must not alias ErrUnsupported")
	}
	wrapped := errors.Join(errResumeMismatch, errors.New("ctx"))
	if !errors.Is(wrapped, errResumeMismatch) {
		t.Error("wrapped mismatch no longer matches errResumeMismatch")
	}
}
