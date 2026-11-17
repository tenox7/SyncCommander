package model_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// prefetchingBackend sums a folder in one call, as the rsync backends do,
// and counts the sums it is asked for one file at a time. Its bulk sums have
// none for the file missing names.
type prefetchingBackend struct {
	model.Backend
	missing string
	perFile atomic.Int64
	bulk    atomic.Int64
}

func (b *prefetchingBackend) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.Backend.Capabilities(ctx)
	caps.PrefetchSums = true
	return caps, err
}

func (b *prefetchingBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	b.perFile.Add(1)
	return b.Backend.Checksum(ctx, relPath, algo)
}

func (b *prefetchingBackend) PrefetchChecksums(ctx context.Context, scope, algo string, recursive bool) (map[string]string, error) {
	b.bulk.Add(1)
	sums := map[string]string{}
	var walk func(dir string) error
	walk = func(dir string) error {
		entries, err := b.Backend.List(ctx, dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			switch {
			case e.IsDir:
				if recursive {
					if err := walk(e.RelPath); err != nil {
						return err
					}
				}
			case e.RelPath != b.missing:
				sum, err := b.Backend.Checksum(ctx, e.RelPath, algo)
				if err != nil {
					return err
				}
				sums[e.RelPath] = sum
			}
		}
		return nil
	}
	return sums, walk(scope)
}

// R11: a bulk fetch gives its sums to the scan that asked, and the backend
// keeps none. The scan stores each one for its file, with no ask per file,
// and counts the file done. A file the fetch has no sum for is summed by
// itself, as a file outside any folder is. The next scan fetches again.
func TestScanUsesTheSumsAFolderFetchGives(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	write := func(root, rel, body string, mtime time.Time) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	files := []string{"d0/a.txt", "d0/b.txt", "d0/c.txt", "d0/sub/z.txt", "d1/x.txt", "d1/y.txt", "top.txt"}
	for _, rel := range files {
		write(left, rel, "body of "+rel, at)
		write(right, rel, "body of "+rel, at)
	}
	pb := &prefetchingBackend{Backend: transport.NewLocalBackend(right), missing: "d0/b.txt"}
	sc := model.NewScanner(transport.NewLocalBackend(left), pb, 1, 1, true)
	opts := model.CompareOpts{Size: true, ModTime: true, Checksum: true}
	ctx := context.Background()
	sc.Scan(ctx, opts)

	st := readChecksumState(sc)
	for _, rel := range files {
		if st.status[rel] != model.AttrEqual {
			t.Errorf("%s is %s by checksum, want equal", rel, statusName(st.status[rel]))
		}
	}
	if n := pb.bulk.Load(); n != 2 {
		t.Errorf("%d folder fetches, want 2: d0 with d0/sub, and d1", n)
	}
	if n := pb.perFile.Load(); n != 2 {
		t.Errorf("%d sums asked one file at a time, want 2: d0/b.txt, which the fetch had none for, and top.txt", n)
	}
	if p := sc.Progress(); p.ChecksumDone != int64(len(files)) || p.ChecksumFiles != int64(len(files)) {
		t.Errorf("the scan counts %d of %d files summed, want %d of %d", p.ChecksumDone, p.ChecksumFiles, len(files), len(files))
	}

	// The next scan fetches again: a file changed since, same size, is
	// different.
	write(right, "d1/x.txt", "BODY OF d1/x.txt", at.Add(time.Hour))
	sc.Scan(ctx, opts)
	if s := readChecksumState(sc).status["d1/x.txt"]; s != model.AttrDifferent {
		t.Errorf("d1/x.txt, changed since the first scan, is %s by checksum, want different", statusName(s))
	}
	if n := pb.bulk.Load(); n != 4 {
		t.Errorf("%d folder fetches after two scans, want 4", n)
	}
}
