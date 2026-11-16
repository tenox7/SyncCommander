package transfer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// sumStub is one file, f.bin, whose checksum takes sumDelay, or does not
// come at all with hang set. It counts the appends and full copies it takes.
type sumStub struct {
	body       string
	size       atomic.Int64 // what List gives for f.bin; 0 = not there
	sumDelay   time.Duration
	hang       bool
	appends    atomic.Int64
	fullCopies atomic.Int64
}

func (s *sumStub) BasePath() string { return "/stub" }
func (s *sumStub) List(context.Context, string) ([]model.FileEntry, error) {
	if n := s.size.Load(); n > 0 {
		return []model.FileEntry{{Name: "f.bin", RelPath: "f.bin", Size: n, Mode: 0o644}}, nil
	}
	return nil, nil
}
func (s *sumStub) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	return statByListing(ctx, s, relPath)
}
func (s *sumStub) Checksum(ctx context.Context, _ string) (string, error) {
	wait := s.sumDelay
	if s.hang {
		wait = time.Hour
	}
	select {
	case <-time.After(wait):
		return "same", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
func (s *sumStub) ProbeChecksums() []string { return []string{"sha256"} }
func (s *sumStub) SetChecksumAlgo(string)   {}
func (s *sumStub) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}
func (s *sumStub) CopyFrom(_ context.Context, _ string, src io.Reader, _ os.FileMode) error {
	s.fullCopies.Add(1)
	n, err := io.Copy(io.Discard, src)
	if err == nil {
		s.size.Store(n)
	}
	return err
}
func (s *sumStub) Mkdir(context.Context, string, os.FileMode) error { return nil }
func (s *sumStub) Rename(context.Context, string, string) error     { return nil }
func (s *sumStub) Remove(context.Context, string) error             { return nil }
func (s *sumStub) RemoveAll(context.Context, string) error          { return nil }
func (s *sumStub) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.body)), nil
}
func (s *sumStub) AppendFrom(ctx context.Context, _ string, src model.RangeOpener, _ os.FileMode, offset int64) error {
	s.appends.Add(1)
	rc, err := src.OpenAt(ctx, offset)
	if err != nil {
		return err
	}
	defer rc.Close()
	n, err := io.Copy(io.Discard, rc)
	s.size.Store(offset + n)
	return err
}

// resumePair is a source holding body and a lazy destination holding its
// first have bytes, scanned, with checksums that take sumDelay or hang.
func resumePair(t *testing.T, body string, have int, sumDelay time.Duration, hang bool) (*scenario, *sumStub) {
	t.Helper()
	src := &sumStub{body: body, sumDelay: sumDelay, hang: hang}
	src.size.Store(int64(len(body)))
	dst := &sumStub{body: body[:have], sumDelay: sumDelay, hang: hang}
	dst.size.Store(int64(have))
	lazy := transport.NewLazyBackend("sftp://host/dst", func(context.Context) (model.Backend, error) { return dst, nil })
	t.Cleanup(func() { transport.CloseBackend(lazy) })
	s := scanned(t, "", "", src, lazy)
	s.verifyResume = true
	return s, dst
}

func shortStallTimeout(t *testing.T) {
	old := transport.StallTimeout()
	transport.SetStallTimeout(time.Second) // stands in for the 60 s default
	t.Cleanup(func() { transport.SetStallTimeout(old) })
}

// F5: a resume appends the missing tail, then checks the file by checksum
// on both sides. The checksums moved no byte the stall guard watched, so a
// check longer than the stall timeout was cancelled, and the file copied
// again from byte 0: resume only worked for files small enough to hash in
// a minute. The guard now waits as long as a slow side needs to hash the
// file (here 4 s for 40 MiB).
func TestResumeVerifySlowerThanTheStallTimeout(t *testing.T) {
	shortStallTimeout(t)
	body := strings.Repeat("x", 40<<20)
	s, dst := resumePair(t, body, 30<<20, 2*time.Second, false)
	mark := len(transport.Log.Lines())
	p := s.copy(t, "f.bin", true, false)
	if dst.appends.Load() != 1 || dst.fullCopies.Load() != 0 {
		t.Errorf("a check of 2 s per side under a 1 s stall timeout: %d appends, %d full copies; want the resume kept",
			dst.appends.Load(), dst.fullCopies.Load())
	}
	if p.Failed.Load() != 0 || !logged(mark, "COPY f.bin OK (resumed)") {
		t.Errorf("the copy did not end resumed (%d failed)", p.Failed.Load())
	}
}

// F5: a check that does stall is given up, and the log says so. The line
// read "context canceled", which the log drops: the file was copied again
// in full with no word why.
func TestResumeVerifyStallIsLogged(t *testing.T) {
	shortStallTimeout(t)
	s, dst := resumePair(t, "0123456789abcdef", 10, 0, true)
	mark := len(transport.Log.Lines())
	p := s.copy(t, "f.bin", true, false)
	if !logged(mark, "transfer stalled, recopying in full") {
		t.Errorf("the log does not say the check stalled:\n%s", strings.Join(transport.Log.Lines()[mark:], "\n"))
	}
	if logged(mark, "comparing the resumed prefix byte for byte") {
		t.Error("after the stall the check went on to compare bytes, which cannot run")
	}
	if dst.fullCopies.Load() != 1 || p.Failed.Load() != 0 {
		t.Errorf("%d full copies, %d failed; want the file copied in full once", dst.fullCopies.Load(), p.Failed.Load())
	}
}

// F21: every remote backend runs inside the reconnecting wrapper, and the
// wrapper did not pass on the rsync backend's "I count my own progress".
// The copy engine counted each byte, and the rsync push counted it again:
// a file showed twice its size.
func TestRsyncDestinationProgressCountedOnce(t *testing.T) {
	url, mod := startRsyncd(t)
	body := strings.Repeat("y", 300_000)
	src := &sumStub{body: body}
	src.size.Store(int64(len(body)))
	dst, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseBackend(dst)
	s := scanned(t, "", "", src, dst)
	p := s.copy(t, "f.bin", true, false)
	if p.Failed.Load() != 0 {
		t.Fatalf("the copy failed:\n%s", strings.Join(transport.Log.Lines(), "\n"))
	}
	if got, err := os.ReadFile(filepath.Join(mod, "f.bin")); err != nil || string(got) != body {
		t.Fatalf("the daemon holds %d bytes (%v), want the %d sent", len(got), err, len(body))
	}
	if got := p.CompletedBytes.Load(); got != int64(len(body)) {
		t.Errorf("the progress counted %d bytes for a %d-byte file, want it once", got, len(body))
	}
}
