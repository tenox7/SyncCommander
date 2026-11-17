package transport

// The fake resumes a copy as a local disk does, and Inject makes its calls
// fail, stop half way or wait (L22). Before, its writes were all or
// nothing and no call could fail: tests on it could not reach the retry,
// resume and cut-off paths.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// bytesSource is a model.RangeOpener over a byte slice.
type bytesSource []byte

func (s bytesSource) Open(ctx context.Context) (io.ReadCloser, error) { return s.OpenAt(ctx, 0) }
func (s bytesSource) OpenAt(_ context.Context, off int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s[off:])), nil
}
func (s bytesSource) Size() int64       { return int64(len(s)) }
func (s bytesSource) LocalPath() string { return "" }

var errFake = errors.New("connection reset by peer")

func TestFakeResumesAsALocalDiskDoes(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")
	body := []byte(strings.Repeat("0123456789", 30))

	fakeWrite(t, b, "p.bin", string(body[:100]))
	if err := b.AppendFrom(ctx, "p.bin", bytesSource(body), model.PutParams{}, 100); err != nil {
		t.Fatal(err)
	}
	if got := fakeRead(t, b, "p.bin"); got != string(body) {
		t.Errorf("p.bin after the append holds %q", got)
	}
	for _, off := range []int64{100, 400} {
		if err := b.AppendFrom(ctx, "p.bin", bytesSource(body), model.PutParams{}, off); !errors.Is(err, ErrAppendOffset) {
			t.Errorf("an append at %d to a file of 300 bytes: %v, want ErrAppendOffset", off, err)
		}
	}
	if err := b.AppendFrom(ctx, "none.bin", bytesSource(body), model.PutParams{}, 10); !errors.Is(err, ErrAppendOffset) {
		t.Errorf("an append to a file not there: %v, want ErrAppendOffset", err)
	}
	if got := fakeRead(t, b, "p.bin"); got != string(body) {
		t.Errorf("p.bin after the refused appends holds %q", got)
	}

	// A generated file takes its bytes along.
	gen := fakeRead(t, b, "f00000.dat")
	more := append([]byte(gen), "tail"...)
	if err := b.AppendFrom(ctx, "f00000.dat", bytesSource(more), model.PutParams{}, int64(len(gen))); err != nil {
		t.Fatal(err)
	}
	if got := fakeRead(t, b, "f00000.dat"); got != string(more) {
		t.Errorf("f00000.dat after the append: %d bytes, want the %d it had and 4 more", len(got), len(gen))
	}

	// A region sum is the sum of those bytes; one past the end fails.
	want := sha256.Sum256(body[100:200])
	if got, err := b.ChecksumRange(ctx, "p.bin", "sha256", 100, 100); err != nil || got != hex.EncodeToString(want[:]) {
		t.Errorf("ChecksumRange of bytes 100-199: %s, %v; want %x", got, err, want)
	}
	if _, err := b.ChecksumRange(ctx, "p.bin", "sha256", 200, 101); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("ChecksumRange past the end: %v, want io.ErrUnexpectedEOF", err)
	}

	// A copy goes into a part, which CommitPart puts in place: over a file,
	// never over a folder.
	if n, err := b.PartSize(ctx, "q.bin"); n != 0 || err != nil {
		t.Errorf("PartSize with no part: %d, %v", n, err)
	}
	fakeWrite(t, b, model.PartPath("q.bin"), "half")
	if n, err := b.PartSize(ctx, "q.bin"); n != 4 || err != nil {
		t.Errorf("PartSize: %d, %v; want 4", n, err)
	}
	if err := b.CommitPart(ctx, "q.bin", 0o640); err != nil {
		t.Fatal(err)
	}
	if e, err := b.Stat(ctx, "q.bin"); err != nil || e.Size != 4 || e.Mode.Perm() != 0o640 {
		t.Errorf("q.bin after CommitPart: %+v, %v; want 4 bytes, mode 0640", e, err)
	}
	if _, err := b.Stat(ctx, model.PartPath("q.bin")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the part after CommitPart: %v, want not there", err)
	}
	fakeWrite(t, b, model.PartPath("f00001.dat"), "new")
	if err := b.CommitPart(ctx, "f00001.dat", 0o644); err != nil {
		t.Fatal(err)
	}
	if got := fakeRead(t, b, "f00001.dat"); got != "new" {
		t.Errorf("f00001.dat after CommitPart holds %q", got)
	}
	fakeWrite(t, b, model.PartPath("d00000"), "new")
	if err := b.CommitPart(ctx, "d00000", 0o644); !errors.Is(err, fs.ErrExist) {
		t.Errorf("CommitPart over the folder d00000: %v, want refused", err)
	}
	if err := b.Mkdir(ctx, model.PartPath("r.bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PartSize(ctx, "r.bin"); !errors.Is(err, model.ErrPartInTheWay) {
		t.Errorf("PartSize with a folder at the part's name: %v, want ErrPartInTheWay", err)
	}
}

func TestFakeInjectFailsCallsByCallAndPath(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")

	once := b.Inject(FakeFault{Op: FakeList, Path: "d00000", Err: errFake, Times: 1})
	if _, err := b.List(ctx, "d00000"); !errors.Is(err, errFake) {
		t.Errorf("List d00000: %v, want the fault's error", err)
	}
	if _, err := b.List(ctx, "d00000"); err != nil {
		t.Errorf("the second List d00000: %v; the fault was for one", err)
	}
	if _, err := b.List(ctx, "d00001"); err != nil {
		t.Errorf("List d00001: %v; the fault was for d00000", err)
	}
	if once.Hits() != 1 {
		t.Errorf("the fault was applied %d times, want 1", once.Hits())
	}

	sub := b.Inject(FakeFault{Op: FakeOpen, Path: "d00001/", Err: fs.ErrPermission})
	for rel, fails := range map[string]bool{"d00001/f00000.dat": true, "d00001/d00000/f00001.dat": true, "d00002/f00000.dat": false, "f00000.dat": false} {
		_, err := b.Open(ctx, rel)
		if got := errors.Is(err, fs.ErrPermission); got != fails {
			t.Errorf("Open %s: %v; a fault on d00001/ fails it: %v", rel, err, fails)
		}
	}
	if _, err := b.Stat(ctx, "d00001/f00000.dat"); err != nil {
		t.Errorf("Stat d00001/f00000.dat: %v; the fault was for Open", err)
	}
	sub.Remove()
	if _, err := b.Open(ctx, "d00001/f00000.dat"); err != nil {
		t.Errorf("Open d00001/f00000.dat after the fault's removal: %v", err)
	}

	// A cut means nothing to a call that moves no bytes: it fails as told.
	b.Inject(FakeFault{Op: FakeStat, Path: "f00004.dat", Cut: 5, Err: errFake, Times: 1})
	if _, err := b.Stat(ctx, "f00004.dat"); !errors.Is(err, errFake) {
		t.Errorf("Stat with a cut and an error: %v, want the fault's error", err)
	}

	// After: the rename happened, and its answer was lost. A retry finds
	// the file gone.
	b.Inject(FakeFault{Op: FakeRename, Path: "f00003.dat", Err: errFake, After: true, Times: 1})
	if err := b.Rename(ctx, "f00003.dat", "r.dat"); !errors.Is(err, errFake) {
		t.Errorf("Rename: %v, want the fault's error", err)
	}
	if _, err := b.Stat(ctx, "r.dat"); err != nil {
		t.Errorf("the rename whose answer was lost did not happen: %v", err)
	}
	if err := b.Rename(ctx, "f00003.dat", "r.dat"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Rename again: %v, want not there", err)
	}

	// The part a copy goes into matches its file's path.
	b.Inject(FakeFault{Op: FakeCopyFrom, Path: "big.bin", Err: errFake, Times: 1})
	if err := b.CopyFrom(ctx, model.PartPath("big.bin"), strings.NewReader("x"), model.PutParams{}); !errors.Is(err, errFake) {
		t.Errorf("CopyFrom into big.bin's part: %v, want the fault's error", err)
	}
	if _, err := b.Stat(ctx, model.PartPath("big.bin")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a copy that failed before it began left %v", err)
	}

	// With nothing to do, a fault counts the calls it matches.
	probe := b.Inject(FakeFault{Path: "d00002/f00000.dat"})
	b.Stat(ctx, "d00002/f00000.dat")
	b.Checksum(ctx, "d00002/f00000.dat", "xxh3")
	b.Stat(ctx, "d00002/f00001.dat")
	if probe.Hits() != 2 {
		t.Errorf("the probe counted %d calls, want 2", probe.Hits())
	}
}

// bigFakeFile is a generated file of at least min bytes, and its bytes.
func bigFakeFile(t *testing.T, b *FakeBackend, min int) (string, []byte) {
	t.Helper()
	entries, err := b.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir && e.Size >= int64(min) {
			return e.RelPath, []byte(fakeRead(t, b, e.RelPath))
		}
	}
	t.Fatalf("no file of %d bytes", min)
	return "", nil
}

func TestFakeCutsCallsThatMoveBytes(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")
	rel, full := bigFakeFile(t, b, 10000)

	b.Inject(FakeFault{Op: FakeOpen, Path: rel, Cut: 1000, Times: 1})
	rd, err := b.Open(ctx, rel)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rd)
	rd.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) || !bytes.Equal(got, full[:1000]) {
		t.Errorf("a read cut after 1000 bytes: %d bytes, %v; want the first 1000, then io.ErrUnexpectedEOF", len(got), err)
	}
	b.Inject(FakeFault{Op: FakeOpen, Path: rel, Cut: 500, Err: errFake, Times: 1})
	rd, err = b.OpenAt(ctx, rel, 2000)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(rd)
	rd.Close()
	if !errors.Is(err, errFake) || !bytes.Equal(got, full[2000:2500]) {
		t.Errorf("a read from 2000 cut after 500 bytes: %d bytes, %v; want bytes 2000-2499, then the fault's error", len(got), err)
	}
	if got := fakeRead(t, b, rel); got != string(full) {
		t.Error("the read after the cuts is not the whole file")
	}

	// A copy cut off leaves the bytes it wrote; one shorter than the cut is
	// not cut.
	body := strings.Repeat("x", 10000)
	b.Inject(FakeFault{Op: FakeCopyFrom, Path: "up.bin", Cut: 4096, Times: 1})
	if err := b.CopyFrom(ctx, "up.bin", strings.NewReader(body), model.PutParams{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a copy cut after 4096 bytes: %v, want io.ErrUnexpectedEOF", err)
	}
	if got := fakeRead(t, b, "up.bin"); got != body[:4096] {
		t.Errorf("a copy cut after 4096 bytes left %d", len(got))
	}
	b.Inject(FakeFault{Op: FakeCopyFrom, Path: "small.bin", Cut: 4096, Times: 1})
	if err := b.CopyFrom(ctx, "small.bin", strings.NewReader("short"), model.PutParams{}); err != nil {
		t.Errorf("a copy shorter than the cut: %v", err)
	}

	b.Inject(FakeFault{Op: FakeAppendFrom, Path: "up.bin", Cut: 1000, Times: 1})
	if err := b.AppendFrom(ctx, "up.bin", bytesSource(body), model.PutParams{}, 4096); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("an append cut after 1000 bytes: %v, want io.ErrUnexpectedEOF", err)
	}
	if got := fakeRead(t, b, "up.bin"); got != body[:5096] {
		t.Errorf("an append cut after 1000 bytes left %d bytes, want 5096", len(got))
	}

	b.Inject(FakeFault{Op: FakeChecksum, Path: rel, Cut: 10, Times: 1})
	if _, err := b.Checksum(ctx, rel, "xxh3"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a checksum cut after 10 bytes: %v, want io.ErrUnexpectedEOF", err)
	}
	if _, err := b.Checksum(ctx, rel, "xxh3"); err != nil {
		t.Errorf("the checksum after the cut: %v", err)
	}
}

func TestFakeDelaysAndStallsCalls(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")

	b.Inject(FakeFault{Op: FakeStat, Path: "f00000.dat", Delay: 100 * time.Millisecond})
	start := time.Now()
	if _, err := b.Stat(ctx, "f00000.dat"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Errorf("Stat with a delay of 100ms took %v", d)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := b.Stat(short, "f00000.dat"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stat whose context ends during the delay: %v, want the context's error", err)
	}

	b.Inject(FakeFault{Op: FakeList, Path: "d00000", Stall: true, Times: 1})
	short2, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel2()
	start = time.Now()
	if _, err := b.List(short2, "d00000"); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) < 50*time.Millisecond {
		t.Errorf("a stalled List: %v after %v, want the context's error when it ends", err, time.Since(start))
	}

	// A copy that stalls after 4096 bytes leaves them written while it
	// waits, and ends when its context does.
	b.Inject(FakeFault{Op: FakeCopyFrom, Path: "s.bin", Cut: 4096, Stall: true, Times: 1})
	stallCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- b.CopyFrom(stallCtx, "s.bin", strings.NewReader(strings.Repeat("y", 10000)), model.PutParams{})
	}()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if e, err := b.Stat(ctx, "s.bin"); err == nil && e.Size == 4096 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the stalled copy did not leave its 4096 bytes")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("the stalled copy returned before its context ended: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("the stalled copy: %v, want the context's error", err)
	}
}
