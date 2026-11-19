package model_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// listingSumsBackend is a prefetchingBackend whose fetch is a listing
// (Caps.ListedSums), as rclone's on S3 is. It notes each fetch.
type listingSumsBackend struct {
	*prefetchingBackend
	mu      sync.Mutex
	fetches []string
}

func (b *listingSumsBackend) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.prefetchingBackend.Capabilities(ctx)
	caps.ListedSums = true
	return caps, err
}

func (b *listingSumsBackend) PrefetchChecksums(ctx context.Context, scope, algo string, recursive bool) (map[string]string, error) {
	b.mu.Lock()
	b.fetches = append(b.fetches, fmt.Sprintf("%q recursive=%v", scope, recursive))
	b.mu.Unlock()
	return b.prefetchingBackend.PrefetchChecksums(ctx, scope, algo, recursive)
}

func (b *listingSumsBackend) fetched() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Sorted(slices.Values(b.fetches))
}

// writeNamed writes each of files under root, holding its own name, with
// the mtime at.
func writeNamed(t *testing.T, root string, at time.Time, files ...string) {
	t.Helper()
	for _, rel := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(rel), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
}

// L20: a backend whose sums come with a listing, as rclone's on S3 do, is
// asked for the files right in the root too, in one listing: those were
// summed one at a time, a HEAD each, and on a flat bucket they are all the
// files. The files in big, none of them on the left, are no files of the
// root's own.
func TestListedSumsFetchTheRootsFilesToo(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	files := []string{"top1.txt", "top2.txt", "d0/a.txt", "d0/sub/z.txt", "d1/x.txt"}
	writeNamed(t, left, at, files...)
	writeNamed(t, right, at, files...)
	for i := range 40 {
		writeNamed(t, right, at, fmt.Sprintf("big/only%02d.txt", i))
	}
	lb := &listingSumsBackend{prefetchingBackend: &prefetchingBackend{Backend: transport.NewLocalBackend(right)}}
	sc := model.NewScanner(transport.NewLocalBackend(left), lb, 1, 1, true)
	sc.Scan(context.Background(), model.CompareOpts{Size: true, ModTime: true, Checksum: true})
	st := readChecksumState(sc)
	for _, rel := range files {
		if st.status[rel] != model.AttrEqual {
			t.Errorf("%s is %s by checksum, want equal", rel, statusName(st.status[rel]))
		}
	}
	want := []string{`"" recursive=false`, `"d0" recursive=true`, `"d1" recursive=true`}
	if got := lb.fetched(); !slices.Equal(got, want) {
		t.Errorf("fetches %q; want %q", got, want)
	}
	if n := lb.perFile.Load(); n != 0 {
		t.Errorf("%d sums asked one file at a time; want none", n)
	}
}

// A listing pays only for enough of the files there: d2's one file to sum
// among 41, d4's one among 41 under it, and the root's one among 21 are
// summed by themselves; d3's two among 12 come from a listing. A backend
// that hashes every file under a folder keeps its fetch of each folder,
// and of none for the root.
func TestListedSumsSkipAFewFilesOfMany(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	both := []string{"both.txt", "d2/both.txt", "d3/a.txt", "d3/b.txt", "d4/both.txt"}
	writeNamed(t, left, at, both...)
	writeNamed(t, right, at, both...)
	var only []string
	for i := range 40 {
		only = append(only, fmt.Sprintf("d2/only%02d.txt", i))
	}
	for i := range 20 {
		only = append(only, fmt.Sprintf("only%02d.txt", i))
	}
	for i := range 10 {
		only = append(only, fmt.Sprintf("d3/only%02d.txt", i))
	}
	for i := range 40 {
		only = append(only, fmt.Sprintf("d4/deep/only%02d.txt", i))
	}
	writeNamed(t, right, at, only...)
	opts := model.CompareOpts{Size: true, ModTime: true, Checksum: true}

	lb := &listingSumsBackend{prefetchingBackend: &prefetchingBackend{Backend: transport.NewLocalBackend(right)}}
	sc := model.NewScanner(transport.NewLocalBackend(left), lb, 1, 1, true)
	sc.Scan(context.Background(), opts)
	st := readChecksumState(sc)
	for _, rel := range both {
		if st.status[rel] != model.AttrEqual {
			t.Errorf("%s is %s by checksum, want equal", rel, statusName(st.status[rel]))
		}
	}
	if got, want := lb.fetched(), []string{`"d3" recursive=true`}; !slices.Equal(got, want) {
		t.Errorf("sums that come with a listing: fetches %q; want %q", got, want)
	}
	if n := lb.perFile.Load(); n != 3 {
		t.Errorf("sums that come with a listing: %d asked one file at a time; want 3, both.txt, d2/both.txt and d4/both.txt", n)
	}

	pb := &prefetchingBackend{Backend: transport.NewLocalBackend(right)}
	sc = model.NewScanner(transport.NewLocalBackend(left), pb, 1, 1, true)
	sc.Scan(context.Background(), opts)
	if n := pb.bulk.Load(); n != 3 {
		t.Errorf("a backend that hashes every file: %d folder fetches; want 3, d2, d3 and d4", n)
	}
	if n := pb.perFile.Load(); n != 1 {
		t.Errorf("a backend that hashes every file: %d sums asked one at a time; want 1, both.txt", n)
	}
}

// A rescan without checksums still sums what a changed file lacks on one
// side, and the root's own files come from a listing there too.
func TestListedSumsFetchTheRootsFilesOnARescan(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	files := []string{"top1.txt", "top2.txt"}
	writeNamed(t, left, at, files...)
	writeNamed(t, right, at, files...)
	lb := &listingSumsBackend{prefetchingBackend: &prefetchingBackend{Backend: transport.NewLocalBackend(right)}}
	sc := model.NewScanner(transport.NewLocalBackend(left), lb, 1, 1, true)
	ctx := context.Background()
	sc.Scan(ctx, model.CompareOpts{Size: true, ModTime: true, Checksum: true})
	var root *model.TreeNode
	sc.ReadTree(func(r *model.TreeNode) { root = r })
	changed := &model.ChangedPaths{Paths: [2]map[string]bool{model.SideRight: {"top1.txt": true, "top2.txt": true}}}
	sc.RescanNode(ctx, root, model.CompareOpts{Size: true, ModTime: true}, changed)
	st := readChecksumState(sc)
	for _, rel := range files {
		if st.status[rel] != model.AttrEqual {
			t.Errorf("%s is %s by checksum after the rescan, want equal", rel, statusName(st.status[rel]))
		}
	}
	want := []string{`"" recursive=false`, `"" recursive=false`}
	if got := lb.fetched(); !slices.Equal(got, want) {
		t.Errorf("fetches %q; want %q: one for the scan, one for the rescan", got, want)
	}
	if n := lb.perFile.Load(); n != 0 {
		t.Errorf("%d sums asked one file at a time; want none", n)
	}
}
