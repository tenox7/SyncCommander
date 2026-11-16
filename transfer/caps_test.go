package transfer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"sc/model"
	"sc/transport"
)

// plainRemote has the methods of model.Backend and no other, as an sftp://
// destination has no batch and a webdav:// one cannot append, on a local
// folder. It counts the Stat calls it gets.
type plainRemote struct {
	model.Backend
	stats atomic.Int64
}

func (p *plainRemote) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	p.stats.Add(1)
	return p.Backend.Stat(ctx, relPath)
}

func (p *plainRemote) Capabilities(context.Context) (model.Caps, error) { return model.Caps{}, nil }

// behindWrapper puts b behind the reconnecting wrapper that holds every
// remote, which has the methods of every optional interface.
func behindWrapper(t *testing.T, b model.Backend) model.Backend {
	t.Helper()
	l := transport.NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) { return b, nil })
	t.Cleanup(func() { transport.CloseBackend(l) })
	return l
}

// F20, D2: a batch copy walked the whole local folder to count its files,
// then learnt whether the destination takes a batch at all. The wrapper
// that holds every remote has every optional method, so the walk ran for
// sftp, ftp and webdav destinations too, a BATCH line went to the log, and
// the batch failed as unsupported. At a million files the walk alone takes
// minutes.
func TestBatchAsksBeforeItWalks(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "d", "a.txt"), "aaa")
	writeFile(t, filepath.Join(left, "d", "b.txt"), "bbb")
	if err := os.MkdirAll(right, 0o755); err != nil {
		t.Fatal(err)
	}
	s := scanned(t, left, right, transport.NewLocalBackend(left), behindWrapper(t, &plainRemote{Backend: transport.NewLocalBackend(right)}))
	mark := len(transport.Log.Lines())
	p := &Progress{}
	Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: s.node(t, "d"), RelPath: "d",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Batch: true})
	if logged(mark, "BATCH") {
		t.Error("a copy to a destination that takes no batch counted the folder for one and logged a BATCH line")
	}
	if p.Failed.Load() != 0 {
		t.Errorf("%d files failed", p.Failed.Load())
	}
	wantTree(t, "the copy", right, "d/", "d/a.txt", "d/b.txt")
}

// D2: before it appended to a shorter file at the destination, a copy asked
// for the file's live size, a round trip, and then learnt from a failed
// append that the destination cannot append: the wrapper that holds every
// remote said it could. The copy asks the destination's Caps first.
func TestResumeAsksBeforeTheSize(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	writeFile(t, filepath.Join(left, "f.txt"), "0123456789")
	writeFile(t, filepath.Join(right, "f.txt"), "01234")
	remote := &plainRemote{Backend: transport.NewLocalBackend(right)}
	s := scanned(t, left, right, transport.NewLocalBackend(left), behindWrapper(t, remote))
	remote.stats.Store(0)
	if p := s.copy(t, "f.txt", true, false); p.Failed.Load() != 0 {
		t.Fatalf("%d files failed", p.Failed.Load())
	}
	if n := remote.stats.Load(); n != 0 {
		t.Errorf("the copy asked a destination that cannot append for the file's size %d times; want none", n)
	}
	if got := readFile(t, filepath.Join(right, "f.txt")); got != "0123456789" {
		t.Errorf("the destination holds %q", got)
	}
}

// An rsync daemon destination appends to a shorter file (--append-verify).
// With Caps that left Resume out for it, a copy over a cut-off one sent the
// whole file again, and nothing else in the suite noticed.
func TestRsyncDestinationResumes(t *testing.T) {
	url, mod := startRsyncd(t)
	local := t.TempDir()
	body := strings.Repeat("0123456789", 100_000)
	writeFile(t, filepath.Join(local, "big.bin"), body)
	writeFile(t, filepath.Join(mod, "big.bin"), body[:400_000])
	dst, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(dst) })
	s := scanned(t, local, mod, transport.NewLocalBackend(local), dst)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "big.bin", true, false); p.Failed.Load() != 0 {
		t.Fatalf("%d files failed", p.Failed.Load())
	}
	if !logged(mark, "COPY big.bin OK (resumed)") {
		t.Error("the copy over a cut-off file on an rsync daemon did not resume it")
	}
	if got := readFile(t, filepath.Join(mod, "big.bin")); got != body {
		t.Errorf("the destination holds %d bytes, not the source's", len(got))
	}
}
