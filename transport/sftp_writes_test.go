package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"

	"sc/model"
)

const testPacket = 32 << 10

// heldWrites is an in-memory sftp server's file writes, each held for
// hold, counting how many run at once. The write at failAt fails, when
// failAt is above 0. It notes a file's close and a SETSTAT that cuts a
// file, and refuses the cut when refuseCut is set.
type heldWrites struct {
	sftp.FileWriter
	hold      time.Duration
	failAt    atomic.Int64
	refuseCut bool
	now, max  atomic.Int32

	mu     sync.Mutex
	events []string
}

func (h *heldWrites) note(e string) {
	h.mu.Lock()
	h.events = append(h.events, e)
	h.mu.Unlock()
}

// last is the last n events.
func (h *heldWrites) last(n int) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.events[max(len(h.events)-n, 0):], " ")
}

func (h *heldWrites) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	w, err := h.FileWriter.Filewrite(r)
	return heldWriterAt{w, h}, err
}

type heldWriterAt struct {
	io.WriterAt
	h *heldWrites
}

func (w heldWriterAt) WriteAt(b []byte, off int64) (int, error) {
	n := w.h.now.Add(1)
	defer w.h.now.Add(-1)
	for m := w.h.max.Load(); n > m && !w.h.max.CompareAndSwap(m, n); m = w.h.max.Load() {
	}
	time.Sleep(w.h.hold)
	if at := w.h.failAt.Load(); at > 0 && off == at {
		return 0, errors.New("no space left on device")
	}
	return w.WriterAt.WriteAt(b, off)
}

// Close is the server's close of the file, once its writes are done.
func (w heldWriterAt) Close() error {
	w.h.note("close")
	if c, ok := w.WriterAt.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type heldCmds struct {
	sftp.FileCmder
	h *heldWrites
}

func (c heldCmds) Filecmd(r *sftp.Request) error {
	if r.Method == "Setstat" && r.AttrFlags().Size {
		c.h.note("cut")
		if c.h.refuseCut {
			return sftp.ErrSSHFxOpUnsupported
		}
	}
	return c.FileCmder.Filecmd(r)
}

// opaqueReader tells nothing but its bytes, as the copy's readers.
type opaqueReader struct{ r io.Reader }

func (o *opaqueReader) Read(p []byte) (int, error) { return o.r.Read(p) }

// opaqueOpener is memRangeOpener whose readers hide their size.
type opaqueOpener struct{ memRangeOpener }

func (o opaqueOpener) OpenAt(_ context.Context, off int64) (io.ReadCloser, error) {
	return io.NopCloser(&opaqueReader{bytes.NewReader(o.memRangeOpener[off:])}), nil
}

// heldSFTP is an SFTP backend on an in-memory server that w watches, and
// a read of a file there.
func heldSFTP(t *testing.T, w *heldWrites) (*SFTPBackend, func(p string) []byte) {
	t.Helper()
	h := sftp.InMemHandler()
	w.FileWriter = h.FilePut
	h.FilePut = w
	h.FileCmd = heldCmds{h.FileCmd, w}
	b, err := NewSFTPBackend(context.Background(), "sftp://x:pw@"+startSFTPRequestServer(t, h)+"/", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, func(p string) []byte {
		r := sftp.NewRequest("Get", p)
		r.Flags = 1 // read
		ra, err := h.FileGet.Fileread(r)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		got, err := io.ReadAll(io.NewSectionReader(ra, 0, 1<<30))
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		return got
	}
}

func testBody() []byte { return bytes.Repeat([]byte("0123456789abcdef"), 1<<16) } // 1 MiB, 32 packets

// S21: pkg/sftp keeps many writes in flight only for a reader that tells
// its size, and the copy's readers tell none: an upload sent one 32 KiB
// WRITE and waited for its answer, a round trip for each packet.
func TestSFTPUploadKeepsWritesInFlight(t *testing.T) {
	body := testBody()
	ctx := context.Background()
	p := model.PutParams{Size: int64(len(body)), Mode: 0o644}
	t.Run("a write", func(t *testing.T) {
		w := &heldWrites{hold: 2 * time.Millisecond}
		b, read := heldSFTP(t, w)
		if err := b.CopyFrom(ctx, "f.bin", &opaqueReader{bytes.NewReader(body)}, p); err != nil {
			t.Fatal(err)
		}
		if m := w.max.Load(); m < 4 {
			t.Errorf("at most %d writes in flight, want many", m)
		}
		if got := read("/f.bin"); !bytes.Equal(got, body) {
			t.Errorf("the file holds %d bytes, not the %d sent", len(got), len(body))
		}
	})
	t.Run("an append", func(t *testing.T) {
		w := &heldWrites{hold: 2 * time.Millisecond}
		b, read := heldSFTP(t, w)
		if err := b.CopyFrom(ctx, "f.bin", bytes.NewReader(body[:testPacket]), model.PutParams{Size: testPacket, Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		w.max.Store(0)
		if err := b.AppendFrom(ctx, "f.bin", opaqueOpener{body}, p, testPacket); err != nil {
			t.Fatal(err)
		}
		if m := w.max.Load(); m < 4 {
			t.Errorf("at most %d writes in flight, want many", m)
		}
		if got := read("/f.bin"); !bytes.Equal(got, body) {
			t.Errorf("the file holds %d bytes, not the %d sent", len(got), len(body))
		}
	})
}

// S21: with many writes in flight, one that fails can have later ones land
// past it, which left a hole of zeros below the file's size: the next try
// resumed from that size. The file is cut back to the first byte that may
// be missing, and holds the source's first bytes. The cut comes after the
// close: a server that runs writes side by side finishes them before it
// closes the file, and one still in flight would land past the cut.
func TestSFTPFailedWriteLeavesNoHole(t *testing.T) {
	body := testBody()
	ctx := context.Background()
	p := model.PutParams{Size: int64(len(body)), Mode: 0o644}
	for _, c := range []struct {
		name string
		put  func(b *SFTPBackend, w *heldWrites) (failAt int64, err error)
	}{
		{"a write", func(b *SFTPBackend, w *heldWrites) (int64, error) {
			w.failAt.Store(2 * testPacket)
			return 2 * testPacket, b.CopyFrom(ctx, "f.bin", bytes.NewReader(body), p)
		}},
		{"a write from a reader that hides its size", func(b *SFTPBackend, w *heldWrites) (int64, error) {
			w.failAt.Store(2 * testPacket)
			return 2 * testPacket, b.CopyFrom(ctx, "f.bin", &opaqueReader{bytes.NewReader(body)}, p)
		}},
		{"an append", func(b *SFTPBackend, w *heldWrites) (int64, error) {
			if err := b.CopyFrom(ctx, "f.bin", bytes.NewReader(body[:testPacket]), model.PutParams{Size: testPacket, Mode: 0o644}); err != nil {
				t.Fatal(err)
			}
			w.failAt.Store(3 * testPacket)
			return 3 * testPacket, b.AppendFrom(ctx, "f.bin", memRangeOpener(body), p, testPacket)
		}},
		{"an append from a reader that hides its size", func(b *SFTPBackend, w *heldWrites) (int64, error) {
			if err := b.CopyFrom(ctx, "f.bin", bytes.NewReader(body[:testPacket]), model.PutParams{Size: testPacket, Mode: 0o644}); err != nil {
				t.Fatal(err)
			}
			w.failAt.Store(3 * testPacket)
			return 3 * testPacket, b.AppendFrom(ctx, "f.bin", opaqueOpener{body}, p, testPacket)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := &heldWrites{hold: time.Millisecond}
			b, read := heldSFTP(t, w)
			failAt, err := c.put(b, w)
			if err == nil {
				t.Fatal("the copy went through a failed write")
			}
			if got := read("/f.bin"); !bytes.Equal(got, body[:failAt]) {
				t.Errorf("the file holds %d bytes after the write at %d failed; want the source's first %d", len(got), failAt, failAt)
			}
			if e := w.last(2); e != "close cut" {
				t.Errorf("the server saw %q last, want the close and then the cut", e)
			}
		})
	}
}

// S21: a server that will not cut the file: the copy's error says so, and
// still says why the write failed.
func TestSFTPFailedCutIsReported(t *testing.T) {
	body := testBody()
	w := &heldWrites{hold: time.Millisecond, refuseCut: true}
	w.failAt.Store(2 * testPacket)
	b, _ := heldSFTP(t, w)
	err := b.CopyFrom(context.Background(), "f.bin", bytes.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644})
	if err == nil || !strings.Contains(err.Error(), "cutting it at 65536") || !strings.Contains(err.Error(), "no space left on device") {
		t.Errorf("err = %v, want the write's error and that the file was not cut", err)
	}
}
