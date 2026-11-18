package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// pausedSource gives a full buffer of zeros on each read, each after a
// pause, and counts its reads and the ones still running.
type pausedSource struct {
	pause   time.Duration
	reads   atomic.Int32
	reading atomic.Int32
}

func (s *pausedSource) Read(p []byte) (int, error) {
	s.reads.Add(1)
	s.reading.Add(1)
	defer s.reading.Add(-1)
	time.Sleep(s.pause)
	clear(p)
	return len(p), nil
}

func (s *pausedSource) Close() error { return nil }

// pausedOpener gives the pausedSource to AppendFrom.
type pausedOpener struct{ src *pausedSource }

func (o pausedOpener) Open(context.Context) (io.ReadCloser, error)          { return o.src, nil }
func (o pausedOpener) OpenAt(context.Context, int64) (io.ReadCloser, error) { return o.src, nil }
func (o pausedOpener) Size() int64                                          { return 1 << 20 }
func (o pausedOpener) LocalPath() string                                    { return "" }

// S24: x/crypto copies a session's Stdin in a goroutine that Run does not
// wait for. A command that failed before it read its input left that
// goroutine reading the source after the call had returned; a retry on the
// same source lost what it took. Once a call returns, nothing reads its
// source.
func TestSCPLeavesTheSourceAloneOnceACallReturns(t *testing.T) {
	root := t.TempDir()
	// A file stands where the folder would go: mkdir fails before cat
	// reads anything.
	if err := os.WriteFile(filepath.Join(root, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 3 bytes, not 10: the append's check fails before cat reads.
	if err := os.WriteFile(filepath.Join(root, "short.txt"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	s := b.(*SCPBackend)
	ctx := context.Background()
	params := model.PutParams{Size: 1 << 20, Mode: 0o644}
	for _, tc := range []struct {
		name string
		run  func(*pausedSource) error
		is   func(error) bool // the failure is the command's, not the input's
	}{
		{"CopyFrom", func(src *pausedSource) error { return b.CopyFrom(ctx, "blocker/x.txt", src, params) },
			func(err error) bool { return exitCode(err) == 1 }},
		{"AppendFrom", func(src *pausedSource) error { return s.AppendFrom(ctx, "short.txt", pausedOpener{src}, params, 10) },
			func(err error) bool { return errors.Is(err, ErrAppendOffset) }},
		{"a command", func(src *pausedSource) error { return s.exec(ctx, s.client, "exit 3", src, nil) },
			func(err error) bool { return exitCode(err) == 3 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &pausedSource{pause: 400 * time.Millisecond}
			if err := tc.run(src); !tc.is(err) {
				t.Errorf("got %v, want the command's own failure", err)
			}
			if n := src.reading.Load(); n != 0 {
				t.Errorf("%d reads of the source still running after the call returned", n)
			}
			reads := src.reads.Load()
			time.Sleep(500 * time.Millisecond)
			if n := src.reads.Load(); n != reads {
				t.Errorf("the source was read %d more times after the call returned", n-reads)
			}
		})
	}
}

// S24: what the command reads still all reaches it, its exit status
// comes back, and a source that fails fails the command, as with
// x/crypto's own copy.
func TestSCPCommandsStillGetTheirInput(t *testing.T) {
	root := t.TempDir()
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	ctx := context.Background()
	body := strings.Repeat("0123456789abcdef", 1<<16) // 1 MiB
	if err := b.CopyFrom(ctx, "d/f.txt", strings.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "d", "f.txt")); err != nil || string(got) != body {
		t.Fatalf("the copy holds %d bytes, %v; want the %d sent", len(got), err, len(body))
	}
	s := b.(*SCPBackend)
	var out strings.Builder
	if err := s.exec(ctx, s.client, "wc -c", strings.NewReader(body), &out); err != nil || strings.TrimSpace(out.String()) != "1048576" {
		t.Errorf("wc -c read %q, %v; want 1048576", out.String(), err)
	}
	if err := s.exec(ctx, s.client, "cat > /dev/null; exit 3", strings.NewReader(body), nil); exitCode(err) != 3 {
		t.Errorf("a command that read its input and exited 3: %v", err)
	}
	broken := io.MultiReader(strings.NewReader("abc"), failingReader{errSourceBroke})
	if err := s.exec(ctx, s.client, "cat > /dev/null", broken, nil); !errors.Is(err, errSourceBroke) {
		t.Errorf("a command fed by a source that failed: %v, want the source's error", err)
	}
}

var errSourceBroke = errors.New("the source broke")

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

// chunkSource gives n bytes, 32 KiB a read at most, the first read after a
// pause.
type chunkSource struct {
	left  int
	pause time.Duration
	reads atomic.Int32
}

func (s *chunkSource) Read(p []byte) (int, error) {
	if s.reads.Add(1) == 1 {
		time.Sleep(s.pause)
	}
	if s.left == 0 {
		return 0, io.EOF
	}
	n := min(len(p), s.left, 32<<10)
	clear(p[:n])
	s.left -= n
	return n, nil
}

// S24, what it cost: the lazy wrapper tries a copy again on the same source
// while none of it has been read. The first try's mkdir failed before cat
// read anything, and x/crypto's copy of the source, still in its first
// read, took 32 KiB that no try sent. The second try wrote the rest, and
// the copy was done: a file 32 KiB short, every byte of the source read.
func TestSCPRetriedCopyIsNotShort(t *testing.T) {
	real, err := exec.LookPath("mkdir")
	if err != nil {
		t.Skip("no mkdir")
	}
	mark := filepath.Join(t.TempDir(), "failed-once")
	fakeCommand(t, "mkdir", fmt.Sprintf(`[ -e %[1]s ] || { : > %[1]s; echo "mkdir: Resource temporarily unavailable" >&2; exit 1; }
exec %[2]s "$@"`, shellQuote(mark), shellQuote(real)))
	root := t.TempDir()
	b, err := TryOpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	const size = 256 << 10
	src := &chunkSource{left: size, pause: 300 * time.Millisecond}
	err = b.CopyFrom(context.Background(), "d/f.bin", src, model.PutParams{Size: size, Mode: 0o644})
	if _, statErr := os.Stat(mark); statErr != nil {
		t.Fatal("the fake mkdir never ran")
	}
	fi, statErr := os.Stat(filepath.Join(root, "d", "f.bin"))
	if err == nil && (statErr != nil || fi.Size() != size) {
		got := int64(-1)
		if statErr == nil {
			got = fi.Size()
		}
		t.Errorf("the copy was done, and the file holds %d bytes of %d", got, size)
	}
}

// S24: rm -rf reads its input only to see its session end, and the input
// gives nothing while the command runs. Read by the call that runs the
// command, an input that gave nothing until it was closed after the call
// held the call for good.
func TestSCPRemoveAllReturns(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "d", "e"), 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	done := make(chan error, 1)
	go func() { done <- b.RemoveAll(context.Background(), "d") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("RemoveAll did not return in 30s")
	}
	if _, err := os.Lstat(filepath.Join(root, "d")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("d after its remove: %v", err)
	}
}
