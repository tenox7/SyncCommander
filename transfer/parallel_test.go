package transfer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// openStub is a source whose files read as one byte each.
type openStub struct{ listStub }

func (b *openStub) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("x")), nil
}

// gateDst holds every CopyFrom until the test lets one through on release,
// so the test sees exactly how many files are in flight.
type gateDst struct {
	listStub
	release chan struct{}
	running atomic.Int32
	started atomic.Int32
	peak    atomic.Int32
}

func (b *gateDst) CopyFrom(ctx context.Context, _ string, src io.Reader, _ model.PutParams) error {
	n := b.running.Add(1)
	defer b.running.Add(-1)
	b.started.Add(1)
	for p := b.peak.Load(); n > p && !b.peak.CompareAndSwap(p, n); p = b.peak.Load() {
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	_, err := io.Copy(io.Discard, src)
	return err
}

// startGatedCopy copies a directory of n one-byte files into a gateDst in the
// background, as the UI does after p.Reset. done closes when Copy returns.
func startGatedCopy(t *testing.T, n, parallel int, p *Progress) (dst *gateDst, done chan struct{}) {
	t.Helper()
	files := make([]model.FileEntry, n)
	for i := range files {
		name := fmt.Sprintf("f%02d", i)
		files[i] = model.FileEntry{RelPath: "d/" + name, Name: name, Size: 1}
	}
	src := &openStub{listStub{dirs: map[string][]model.FileEntry{
		"":  {{RelPath: "d", Name: "d", IsDir: true}},
		"d": files,
	}}}
	dst = &gateDst{listStub: listStub{dirs: map[string][]model.FileEntry{}}, release: make(chan struct{})}
	scanner := model.NewScanner(src, dst, 1, 1, false)
	scanner.Scan(context.Background(), model.CompareOpts{})
	var node *model.TreeNode
	for _, c := range scanner.Tree().Children {
		if c.Name == "d" {
			node = c
		}
	}
	if node == nil {
		t.Fatal("d not in tree")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() {
		defer close(done)
		Copy(ctx, Request{Src: src, Dst: dst, Scanner: scanner, Node: node, RelPath: "d", LeftToRight: true, Parallel: parallel, Progress: p})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return dst, done
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func finishAll(t *testing.T, dst *gateDst, done chan struct{}, p *Progress, files int) {
	t.Helper()
	close(dst.release)
	<-done
	if f := p.Failed.Load(); f != 0 {
		t.Fatalf("%d file(s) failed", f)
	}
	if got := dst.started.Load(); got != int32(files) {
		t.Fatalf("%d files copied, want %d", got, files)
	}
}

// + past the start value starts more files at once, and the popup gets a row
// for each even though Reset only made rows for the start value.
func TestCopyRaisedMidRunStartsMoreFiles(t *testing.T) {
	p := &Progress{}
	p.Reset(2, true)
	dst, done := startGatedCopy(t, 8, 2, p)
	waitFor(t, "2 files in flight", func() bool { return dst.running.Load() == 2 })

	p.SetParallel(4)
	waitFor(t, "4 files in flight", func() bool { return dst.running.Load() == 4 })
	active := 0
	for _, s := range p.SnapshotSlots() {
		if s.Active {
			active++
		}
	}
	if active != 4 {
		t.Fatalf("popup has %d rows in flight for 4 files in flight", active)
	}
	finishAll(t, dst, done, p, 8)
}

// The popup gave a row to the slots in flight only: a file that ended a
// moment before the next one took its slot took its row with it for a tick,
// and the popup jumped by a row. A free slot now stays in its place with the
// file that ended in it, and a slot in flight always has its file.
func TestAFreeSlotKeepsItsPlaceAndItsFile(t *testing.T) {
	p := &Progress{}
	p.Reset(2, true)
	dst, done := startGatedCopy(t, 2, 2, p)
	waitFor(t, "2 files in flight", func() bool { return dst.running.Load() == 2 })
	for _, s := range p.SnapshotSlots() {
		if !s.Active || s.File == "" || s.Start == 0 || s.End != 0 {
			t.Fatalf("a slot in flight is %+v, want its file, its start and no end", s)
		}
	}
	dst.release <- struct{}{}
	waitFor(t, "a file done", func() bool { return p.Done.Load() == 1 })
	slots := p.SnapshotSlots()
	if len(slots) != 2 {
		t.Fatalf("%d slots after a file ended, want 2: %+v", len(slots), slots)
	}
	free := 0
	for _, s := range slots {
		if !s.Active {
			free++
			if !strings.HasPrefix(s.File, "d/f") || s.Bytes != 1 || s.End == 0 {
				t.Errorf("the free slot is %+v, want the file that ended in it, its byte and its end", s)
			}
		}
	}
	if free != 1 {
		t.Errorf("%d free slots, want 1: %+v", free, slots)
	}
	finishAll(t, dst, done, p, 2)
}

// + pressed while the subtree is still being listed, before the workers
// exist, must not be lost when they start.
func TestCopyKeepsParallelSetBeforeWorkersStart(t *testing.T) {
	p := &Progress{}
	p.Reset(2, true)
	p.SetParallel(3) // no semaphore yet
	dst, done := startGatedCopy(t, 6, 2, p)
	waitFor(t, "3 files in flight", func() bool { return dst.running.Load() == 3 })
	finishAll(t, dst, done, p, 6)
	if got := dst.peak.Load(); got != 3 {
		t.Fatalf("peak %d files in flight, want 3", got)
	}
}

// - never stops a running file: the ones in flight finish, the first to
// finish are not replaced, and only then does the lower limit hold.
func TestCopyLoweredMidRunLetsRunningFilesFinish(t *testing.T) {
	p := &Progress{}
	p.Reset(3, true)
	dst, done := startGatedCopy(t, 6, 3, p)
	waitFor(t, "3 files in flight", func() bool { return dst.running.Load() == 3 })

	p.SetParallel(1)
	for want := int32(2); want >= 1; want-- {
		dst.release <- struct{}{}
		waitFor(t, fmt.Sprintf("%d files in flight", want), func() bool { return dst.running.Load() == want })
		time.Sleep(20 * time.Millisecond)
		if got := dst.started.Load(); got != 3 {
			t.Fatalf("a finished file was replaced while above the lowered limit: %d started", got)
		}
	}
	dst.release <- struct{}{}
	waitFor(t, "the next file to start", func() bool { return dst.started.Load() == 4 })

	dst.peak.Store(dst.running.Load())
	finishAll(t, dst, done, p, 6)
	if got := dst.peak.Load(); got != 1 {
		t.Fatalf("after settling, %d files ran at once, want 1", got)
	}
}
