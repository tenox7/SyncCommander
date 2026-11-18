package transfer

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// timeless is a source whose listing gives its files no time, as an FTP
// or HTTP server may.
type timeless struct{ model.Backend }

func (b timeless) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	ents, err := b.Backend.List(ctx, relDir)
	for i := range ents {
		ents[i].ModTime = time.Time{}
	}
	return ents, err
}

func (b timeless) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	e, err := b.Backend.Stat(ctx, relPath)
	e.ModTime = time.Time{}
	return e, err
}

// timesGiven keeps the mtime each SetTimes was given.
type timesGiven struct {
	mu     sync.Mutex
	mtimes []time.Time
}

func (g *timesGiven) give(mtime time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mtimes = append(g.mtimes, mtime)
}

func (g *timesGiven) zero() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, m := range g.mtimes {
		if m.IsZero() {
			n++
		}
	}
	return n
}

// partsGiven is a fake destination, which writes part files, keeping the
// times it is given.
type partsGiven struct {
	*transport.FakeBackend
	*timesGiven
}

func (d partsGiven) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	d.give(mtime)
	return d.FakeBackend.SetTimes(ctx, relPath, mtime, atime, btime)
}

// plainGiven is partsGiven without part files.
type plainGiven struct {
	model.Backend
	*timesGiven
}

func (d plainGiven) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	d.give(mtime)
	return d.Backend.SetTimes(ctx, relPath, mtime, atime, btime)
}

// L25: a source file with no time, as some FTP and HTTP listings give,
// was copied and then given its time: a zero one, which FTP wrote as year
// 1 (MFMT 00010101000000). Such a file now gets no time set; its copy
// keeps the time it was written. A copy into a part file, and one without.
func TestCopyOfAFileWithNoTimeSetsNone(t *testing.T) {
	for _, parts := range []bool{true, false} {
		name := "part files"
		if !parts {
			name = "no part files"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fake, err := transport.NewFakeBackend("fake://src?dirs=0&files=0")
			if err != nil {
				t.Fatal(err)
			}
			if err := fake.Mkdir(ctx, "d", 0o755); err != nil {
				t.Fatal(err)
			}
			files := []string{"d/a.txt", "d/b.txt"}
			for _, f := range files {
				if err := fake.CopyFrom(ctx, f, strings.NewReader("x"), model.PutParams{Size: 1, Mode: 0o644}); err != nil {
					t.Fatal(err)
				}
			}
			out, err := transport.NewFakeBackend("fake://dst?dirs=0&files=0")
			if err != nil {
				t.Fatal(err)
			}
			given := &timesGiven{}
			var dst model.Backend = partsGiven{out, given}
			if !parts {
				dst = plainGiven{out, given}
			}
			s := scanned(t, "", "", timeless{fake}, dst)
			p := s.copy(t, "d", true, false)
			if f, n := p.Failed.Load(), p.TimesNotSet.Load(); f != 0 || n != 0 {
				t.Errorf("failed=%d, times not set=%d; want 0 and 0: the source has no time to set", f, n)
			}
			for _, f := range files {
				if got, ok := holds(t, out, f); got != "x" {
					t.Errorf("%s holds %q (there: %v), want x", f, got, ok)
				}
			}
			if n := given.zero(); n != 0 {
				t.Errorf("SetTimes was given a zero time %d times: FTP writes it as year 1", n)
			}
		})
	}
}
