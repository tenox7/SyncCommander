package transfer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// told keeps the PutParams of each put a destination takes.
type told struct {
	mu  sync.Mutex
	got []model.PutParams
}

func (t *told) add(p model.PutParams) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.got = append(t.got, p)
}

func (t *told) all() []model.PutParams {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.got)
}

// paramsStub is a destination without part files that keeps what each put
// was told.
type paramsStub struct {
	sumStub
	told
}

func (s *paramsStub) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	s.add(p)
	return s.sumStub.CopyFrom(ctx, relPath, src, p)
}

func (s *paramsStub) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	s.add(p)
	return s.sumStub.AppendFrom(ctx, relPath, src, p, offset)
}

// senderStub also takes a local file by its path, as rsync does.
type senderStub struct{ paramsStub }

func (s *senderStub) SendLocalFile(_ context.Context, srcPath, _ string, p model.PutParams) error {
	s.add(p)
	fi, err := os.Stat(srcPath)
	if err == nil {
		s.size.Store(fi.Size())
	}
	return err
}

// partsDst is a local destination, which writes part files, that keeps what
// each put was told.
type partsDst struct {
	*transport.LocalBackend
	told
}

func (d *partsDst) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	d.add(p)
	return d.LocalBackend.CopyFrom(ctx, relPath, src, p)
}

func (d *partsDst) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, p model.PutParams, offset int64) error {
	d.add(p)
	return d.LocalBackend.AppendFrom(ctx, relPath, src, p, offset)
}

// D1: every put the copy makes is told the size and mtime the listing gave
// the source, and its mode, or a part file's: a whole copy, a resume, a
// copy into a part file and a local file sent by path.
func TestPutsAreToldTheListedFile(t *testing.T) {
	const body = "0123456789abcdefghij"
	mtime := time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)
	srcDir := t.TempDir()
	f := filepath.Join(srcDir, "f.bin")
	if err := os.WriteFile(f, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(f, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	want := model.PutParams{Size: int64(len(body)), Mode: 0o640, ModTime: mtime}
	partWant := want
	partWant.Mode = model.PartMode(want.Mode)

	resumeDst := &paramsStub{}
	resumeDst.body = body[:8]
	resumeDst.size.Store(8)
	partDst := &partsDst{LocalBackend: transport.NewLocalBackend(t.TempDir())}
	sendDst := &senderStub{}
	for _, tc := range []struct {
		name string
		dst  model.Backend
		told *told
		want model.PutParams
	}{
		{"copy", &paramsStub{}, nil, want},
		{"resume", resumeDst, &resumeDst.told, want},
		{"part", partDst, &partDst.told, partWant},
		{"send", sendDst, &sendDst.told, want},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.told == nil {
				tc.told = &tc.dst.(*paramsStub).told
			}
			s := scanned(t, "", "", transport.NewLocalBackend(srcDir), tc.dst)
			p := s.copy(t, "f.bin", true, false)
			if p.Failed.Load() != 0 {
				t.Fatalf("%d failed", p.Failed.Load())
			}
			got := tc.told.all()
			if len(got) != 1 || got[0].Size != tc.want.Size || got[0].Mode != tc.want.Mode || !got[0].ModTime.Equal(tc.want.ModTime) {
				t.Errorf("the puts were told %+v; want one told %+v", got, tc.want)
			}
		})
	}
}
