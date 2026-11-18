package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
)

// inFlight wraps a server's file reads: it counts the READs it serves at
// once, each taking a little while, as over a link with a round trip.
type inFlight struct {
	sftp.FileReader
	mu        sync.Mutex
	now, peak int
}

func (f *inFlight) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	ra, err := f.FileReader.Fileread(r)
	if err != nil {
		return nil, err
	}
	return readAtFunc(func(p []byte, off int64) (int, error) {
		f.mu.Lock()
		f.now++
		f.peak = max(f.peak, f.now)
		f.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		defer func() {
			f.mu.Lock()
			f.now--
			f.mu.Unlock()
		}()
		return ra.ReadAt(p, off)
	}), nil
}

type readAtFunc func([]byte, int64) (int, error)

func (f readAtFunc) ReadAt(p []byte, off int64) (int, error) { return f(p, off) }

// readBy32K reads rc to its end 32 KiB at a time, as io.Copy does.
func readBy32K(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer rc.Close()
	var out bytes.Buffer
	buf := make([]byte, 32<<10)
	for {
		n, err := rc.Read(buf)
		out.Write(buf[:n])
		if err == io.EOF {
			return out.Bytes()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// S17: pkg/sftp sends a read of more than one packet as many READs at
// once, and callers read 32 KiB, one packet, at a time: a download waited
// a round trip for each 32 KiB, 756 KiB/s over a 40 ms link. Now it reads
// ahead, so many READs are on their way at once; what it gives is the
// file, whole, from the start or from an offset, empty or of one byte.
func TestSFTPDownloadReadsAhead(t *testing.T) {
	h := sftp.InMemHandler()
	put := func(p string, body []byte) {
		r := sftp.NewRequest("Put", p)
		r.Flags = 2 | 8 | 16 // write, create, truncate
		w, err := h.FilePut.Filewrite(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteAt(body, 0); err != nil {
			t.Fatal(err)
		}
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
	if err := h.FileCmd.Filecmd(sftp.NewRequest("Mkdir", "/srv")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1<<20+7)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	put("/srv/big", data)
	put("/srv/empty", nil)
	put("/srv/one", []byte("x"))
	reads := &inFlight{FileReader: h.FileGet}
	h.FileGet = reads

	ctx := context.Background()
	b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+startSFTPRequestServer(t, h)+"/srv", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	rc, err := b.Open(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	if got := readBy32K(t, rc); !bytes.Equal(got, data) {
		t.Fatalf("the download gave %d bytes, not the file of %d", len(got), len(data))
	}
	reads.mu.Lock()
	peak := reads.peak
	reads.mu.Unlock()
	if peak < 4 {
		t.Errorf("at most %d READs were on their way at once; want them read ahead, 4 or more", peak)
	}
	rc, err = b.OpenAt(ctx, "big", 300_000)
	if err != nil {
		t.Fatal(err)
	}
	if got := readBy32K(t, rc); !bytes.Equal(got, data[300_000:]) {
		t.Errorf("the download from 300000 gave %d bytes, not the %d after it", len(got), len(data)-300_000)
	}
	for name, want := range map[string]string{"empty": "", "one": "x"} {
		rc, err := b.Open(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(readBy32K(t, rc)); got != want {
			t.Errorf("%s gave %q, want %q", name, got, want)
		}
	}
}

// sizedReads is a file that keeps the size of each read asked of it.
type sizedReads struct {
	left  int
	sizes []int
}

func (s *sizedReads) Read(p []byte) (int, error) {
	s.sizes = append(s.sizes, len(p))
	if s.left == 0 {
		return 0, io.EOF
	}
	n := min(len(p), s.left)
	s.left -= n
	return n, nil
}

func (s *sizedReads) Close() error { return nil }

// S17: the reads ahead start at one packet, as reads were before, so a
// slow link is no slower, and double while they come back quickly.
func TestSFTPReadAheadStartsAtOnePacket(t *testing.T) {
	f := &sizedReads{left: 1 << 20}
	r := &sftpReader{ReadCloser: f, ctx: context.Background(), done: func() {}}
	if got := len(readBy32K(t, r)); got != 1<<20 {
		t.Fatalf("read %d bytes, want %d", got, 1<<20)
	}
	want := []int{32 << 10, 64 << 10, 128 << 10, 256 << 10}
	if len(f.sizes) < len(want) || !slices.Equal(f.sizes[:len(want)], want) {
		t.Errorf("reads asked for %v; want them to start %v", f.sizes, want)
	}
}

// S17: a read ahead doubles after a full one that came back within a
// quarter of a second, up to 4 MiB, and halves after one that took more
// than a second, down to one packet: on a slow link a read ahead of 4 MiB
// shows no progress for longer than the stall guard waits.
func TestSFTPReadAheadSizes(t *testing.T) {
	const k = 1 << 10
	for _, c := range []struct {
		size, n int
		took    time.Duration
		want    int
	}{
		{32 * k, 32 * k, 10 * time.Millisecond, 64 * k},
		{2048 * k, 2048 * k, 10 * time.Millisecond, 4096 * k},
		{4096 * k, 4096 * k, 10 * time.Millisecond, 4096 * k},
		{256 * k, 1000, 10 * time.Millisecond, 256 * k},
		{256 * k, 256 * k, 500 * time.Millisecond, 256 * k},
		{256 * k, 256 * k, 2 * time.Second, 128 * k},
		{64 * k, 1000, 2 * time.Second, 32 * k},
		{32 * k, 32 * k, 5 * time.Second, 32 * k},
	} {
		if got := nextReadAhead(c.size, c.n, c.took); got != c.want {
			t.Errorf("after %d of %d in %v: %d; want %d", c.n, c.size, c.took, got, c.want)
		}
	}
}
