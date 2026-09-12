package transport

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"sc/model"
)

type copyStub struct {
	calls   int
	failFor int   // fail the first N CopyFrom calls
	consume int64 // bytes read from src before failing
	got     []string
}

func (s *copyStub) BasePath() string                                        { return "/stub" }
func (s *copyStub) List(context.Context, string) ([]model.FileEntry, error) { return nil, nil }
func (s *copyStub) Checksum(context.Context, string) (string, error)        { return "", nil }
func (s *copyStub) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}
func (s *copyStub) Mkdir(context.Context, string, os.FileMode) error    { return nil }
func (s *copyStub) Rename(context.Context, string, string) error        { return nil }
func (s *copyStub) Remove(context.Context, string) error                { return nil }
func (s *copyStub) RemoveAll(context.Context, string) error             { return nil }
func (s *copyStub) Open(context.Context, string) (io.ReadCloser, error) { return nil, nil }

func (s *copyStub) CopyFrom(_ context.Context, _ string, src io.Reader, _ os.FileMode) error {
	s.calls++
	if s.calls <= s.failFor {
		if s.consume > 0 {
			_, _ = io.CopyN(io.Discard, src, s.consume)
		}
		return errors.New("broken pipe")
	}
	b, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	s.got = append(s.got, string(b))
	return nil
}

func newLazyFor(inner model.Backend) model.Backend {
	return NewLazyBackend("stub://host/path", func() (model.Backend, error) { return inner, nil })
}

// A failure before anything was read off src is safe to retry: the connection
// died, markBrokenIf dropped it, and the next attempt redials with the stream
// still at byte 0.
func TestLazyCopyFromRetriesUntouchedStream(t *testing.T) {
	SetMaxRetries(2)
	defer SetMaxRetries(5)

	inner := &copyStub{failFor: 1}
	b := newLazyFor(inner)
	if err := b.CopyFrom(context.Background(), "f", strings.NewReader("hello"), 0o644); err != nil {
		t.Fatalf("CopyFrom = %v, want nil after retry", err)
	}
	if inner.calls != 2 {
		t.Errorf("calls = %d, want 2", inner.calls)
	}
	if len(inner.got) != 1 || inner.got[0] != "hello" {
		t.Errorf("destination got %q, want [hello]", inner.got)
	}
}

// Once bytes have been consumed the reader can't be rewound, so retrying would
// write a truncated file. The error must surface to the caller, which re-opens
// the source and retries at a higher level.
func TestLazyCopyFromDoesNotRetryConsumedStream(t *testing.T) {
	SetMaxRetries(3)
	defer SetMaxRetries(5)

	inner := &copyStub{failFor: 1, consume: 3}
	b := newLazyFor(inner)
	err := b.CopyFrom(context.Background(), "f", strings.NewReader("hello"), 0o644)
	if err == nil {
		t.Fatal("CopyFrom = nil, want the error to surface")
	}
	if inner.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on a partially read stream)", inner.calls)
	}
	if len(inner.got) != 0 {
		t.Errorf("destination wrote %q, want nothing", inner.got)
	}
}

func TestLazyCopyFromGivesUpAfterMaxRetries(t *testing.T) {
	SetMaxRetries(1)
	defer SetMaxRetries(5)

	inner := &copyStub{failFor: 99}
	b := newLazyFor(inner)
	if err := b.CopyFrom(context.Background(), "f", strings.NewReader("hello"), 0o644); err == nil {
		t.Fatal("CopyFrom = nil, want an error")
	}
	if inner.calls != 2 {
		t.Errorf("calls = %d, want 2 (initial + 1 retry)", inner.calls)
	}
}
