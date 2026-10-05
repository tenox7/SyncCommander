package transfer

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"testing"

	"sc/model"
)

type failingAppender struct{ *resumeStub }

func (f *failingAppender) AppendFrom(context.Context, string, model.RangeOpener, os.FileMode, int64) error {
	return io.ErrUnexpectedEOF
}

// A destination that rejects the append (rsync --append-verify finding a
// different prefix, say) is not a resume: the credited offset is rolled back
// and the caller falls through to a full copy.
func TestTryResumeCopyFallsBackWhenAppendFails(t *testing.T) {
	src, dst := newResumePair(10, "same", "same")
	var bytes, base atomic.Int64
	if tryResumeCopy(context.Background(), src, &failingAppender{dst}, "f.bin", srcEntry(), &model.FileEntry{Name: "f.bin", RelPath: "f.bin", Size: 10}, &bytes, &base, nil) {
		t.Fatal("tryResumeCopy = true although the append failed")
	}
	if bytes.Load() != 0 || base.Load() != 0 {
		t.Fatalf("progress not rolled back: bytes=%d base=%d", bytes.Load(), base.Load())
	}
}
