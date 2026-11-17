package transfer

// Scenario tests on the fake backend standing in for a server that cuts a
// copy off, stops answering, or cannot list a folder (L22): the real
// scanner and copy engine, and faults the fake injects. On the fake before
// L22 no call could fail and a copy was all or nothing, so none of these
// paths could be reached.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// fakeBody is n bytes that do not repeat, the same each run.
func fakeBody(n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{22}).Read(b)
	return b
}

// emptyFake is a fake server that holds nothing yet.
func emptyFake(t *testing.T) *transport.FakeBackend {
	t.Helper()
	b, err := transport.NewFakeBackend("fake://empty?dirs=0&files=0")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fakeContent(t *testing.T, b model.Backend, rel string) []byte {
	t.Helper()
	rd, err := b.Open(context.Background(), rel)
	if err != nil {
		t.Fatalf("open %s: %v", rel, err)
	}
	defer rd.Close()
	data, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return data
}

// localBig writes a file of 1 MiB to a new local folder.
func localBig(t *testing.T, name string) (dir string, body []byte) {
	t.Helper()
	dir, body = t.TempDir(), fakeBody(1<<20)
	if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, body
}

// An upload cut off half way is tried again and goes on from its part: the
// part's bytes are checked by checksum, the rest is appended, and the file
// takes its name only when whole.
func TestScenarioFakeCutOffUploadResumes(t *testing.T) {
	oneRetry(t)
	left, body := localBig(t, "big.bin")
	fake := emptyFake(t)
	cut := fake.Inject(transport.FakeFault{Op: transport.FakeCopyFrom, Path: "big.bin", Cut: 300_000, Times: 1})
	appends := fake.Inject(transport.FakeFault{Op: transport.FakeAppendFrom, Path: "big.bin"})

	s := scanned(t, left, "", transport.NewLocalBackend(left), fake)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "big.bin", true, false); p.Failed.Load() != 0 {
		t.Fatal("the upload failed")
	}
	if cut.Hits() != 1 || appends.Hits() != 1 {
		t.Errorf("%d uploads cut off and %d appends, want 1 and 1", cut.Hits(), appends.Hits())
	}
	if !logged(mark, "resuming after") {
		t.Error("the second try did not go on from the part")
	}
	if got := fakeContent(t, fake, "big.bin"); !bytes.Equal(got, body) {
		t.Errorf("the server holds %d bytes that are not the source's", len(got))
	}
	if _, err := fake.Stat(context.Background(), model.PartPath("big.bin")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the part after the upload: %v, want not there", err)
	}
}

// A download cut off every time fails, and the local file stays as it was:
// the bytes so far are in its part. Scanned again, the next copy goes on
// from the part.
func TestScenarioFakeCutOffDownloadKeepsTheOldFile(t *testing.T) {
	oneRetry(t)
	ctx := context.Background()
	fake := emptyFake(t)
	body := fakeBody(1 << 20)
	if err := fake.CopyFrom(ctx, "doc.bin", bytes.NewReader(body), model.PutParams{Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	local := t.TempDir()
	old := filepath.Join(local, "doc.bin")
	writeFile(t, old, "the old version")
	hourAgo := time.Now().Add(-time.Hour)
	if err := os.Chtimes(old, hourAgo, hourAgo); err != nil {
		t.Fatal(err)
	}

	always := fake.Inject(transport.FakeFault{Op: transport.FakeOpen, Path: "doc.bin", Cut: 100_000})
	s := scanned(t, "", local, fake, transport.NewLocalBackend(local))
	if p := s.copy(t, "doc.bin", true, false); p.Failed.Load() == 0 {
		t.Error("a download cut off every time did not fail")
	}
	if got := contentOf(old); got != "the old version" {
		t.Errorf("the local file after a failed download holds %d bytes, want its old content", len(got))
	}
	part := contentOf(filepath.Join(local, model.PartName("doc.bin")))
	if len(part) == 0 || !bytes.HasPrefix(body, []byte(part)) {
		t.Errorf("the part holds %d bytes, want the source's first bytes", len(part))
	}

	always.Remove()
	s = scanned(t, "", local, fake, transport.NewLocalBackend(local))
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "doc.bin", true, false); p.Failed.Load() != 0 {
		t.Fatal("the download failed")
	}
	if !logged(mark, "resuming after") {
		t.Error("the download did not go on from its part")
	}
	if got := contentOf(old); got != string(body) {
		t.Errorf("the local file holds %d bytes that are not the source's", len(got))
	}
	wantTree(t, "the local side", local, "doc.bin")
}

// An upload that stops moving bytes is ended by the stall guard, tried
// again, and goes on from its part.
func TestScenarioFakeStalledUploadIsRetried(t *testing.T) {
	oneRetry(t)
	stall := transport.StallTimeout()
	transport.SetStallTimeout(300 * time.Millisecond)
	t.Cleanup(func() { transport.SetStallTimeout(stall) })
	left, body := localBig(t, "big.bin")
	fake := emptyFake(t)
	stalled := fake.Inject(transport.FakeFault{Op: transport.FakeCopyFrom, Path: "big.bin", Cut: 200_000, Stall: true, Times: 1})

	s := scanned(t, left, "", transport.NewLocalBackend(left), fake)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "big.bin", true, false); p.Failed.Load() != 0 {
		t.Fatal("the upload failed")
	}
	if stalled.Hits() != 1 {
		t.Errorf("%d uploads stalled, want 1", stalled.Hits())
	}
	if !logged(mark, "stalled") || !logged(mark, "resuming after") {
		t.Error("the log does not say the upload stalled and went on from its part")
	}
	if got := fakeContent(t, fake, "big.bin"); !bytes.Equal(got, body) {
		t.Errorf("the server holds %d bytes that are not the source's", len(got))
	}
}

// A folder the server cannot list is flagged, and a mirror from it fails and
// deletes nothing in it.
func TestScenarioFakeUnlistableFolderMirror(t *testing.T) {
	fake, err := transport.NewFakeBackend("fake://x?dirs=2&files=2&depth=1&size=100")
	if err != nil {
		t.Fatal(err)
	}
	local := t.TempDir()
	s := scanned(t, "", local, fake, transport.NewLocalBackend(local))
	if p := s.copy(t, "", true, false); p.Failed.Load() != 0 {
		t.Fatalf("the download failed %d files", p.Failed.Load())
	}
	want := diskTree(t, local)
	if len(want) != 8 {
		t.Fatalf("the download wrote %q, want 2 folders of 2 files and 2 files", want)
	}

	fake.Inject(transport.FakeFault{Op: transport.FakeList, Path: "d00001", Err: errors.New("connection reset by peer")})
	s = scanned(t, "", local, fake, transport.NewLocalBackend(local))
	if !s.node(t, "d00001").ListErr {
		t.Error("d00001 is not flagged as a failed listing")
	}
	for _, rel := range []string{"d00001", ""} {
		if p := s.copy(t, rel, true, true); p.Failed.Load() == 0 {
			t.Errorf("a mirror of %q from a folder the server cannot list did not fail", rel)
		}
	}
	wantTree(t, "the local side after the mirrors", local, want...)
}
