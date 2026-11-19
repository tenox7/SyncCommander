package transport

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rclone/rclone/fs"

	"sc/model"
)

// lookupCountingFs counts the objects rclone is asked for one at a time:
// each is a HEAD on S3.
type lookupCountingFs struct {
	fs.Fs
	lookups atomic.Int64
}

func (f *lookupCountingFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	f.lookups.Add(1)
	return f.Fs.NewObject(ctx, remote)
}

// rcloneWithFiles is an rclone memory remote with a bucket of its own, each
// of files holding its own name, and the count of its lookups.
func rcloneWithFiles(t *testing.T, bucket string, files []string) (*RcloneBackend, *lookupCountingFs) {
	t.Helper()
	ctx := context.Background()
	b, err := NewRcloneBackend(ctx, "rclone://:memory:"+bucket, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	for _, p := range files {
		if err := b.CopyFrom(ctx, p, strings.NewReader(p), model.PutParams{Size: int64(len(p)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	counting := &lookupCountingFs{Fs: b.f}
	b.f = counting
	return b, counting
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// L20: the listing carries the sums, but only the objects of the last
// 100,000 listed were kept: a checksum pass over a bigger tree asked for
// each file by itself, a HEAD per object on S3. A folder's sums now come
// from one listing of it.
func TestRclonePrefetchReadsTheSumsFromAListing(t *testing.T) {
	files := []string{"top.txt", "d/a.txt", "d/b.txt", "d/sub/c.txt", "e/x.txt"}
	b, counting := rcloneWithFiles(t, "l20prefetch", files)
	ctx := context.Background()
	if caps, err := b.Capabilities(ctx); err != nil || !caps.PrefetchSums || !caps.ListedSums {
		t.Fatalf("a memory remote, whose listing has md5: %+v, %v; want PrefetchSums and ListedSums", caps, err)
	}
	for _, c := range []struct {
		scope     string
		recursive bool
		want      []string
	}{
		{"d", true, []string{"d/a.txt", "d/b.txt", "d/sub/c.txt"}},
		{"d", false, []string{"d/a.txt", "d/b.txt"}},
		{"", false, []string{"top.txt"}},
		{"", true, files},
	} {
		got, err := b.PrefetchChecksums(ctx, c.scope, "md5", c.recursive)
		if err != nil {
			t.Fatalf("PrefetchChecksums(%q, recursive %v): %v", c.scope, c.recursive, err)
		}
		want := map[string]string{}
		for _, p := range c.want {
			want[p] = md5Hex(p)
		}
		if !maps.Equal(got, want) {
			t.Errorf("PrefetchChecksums(%q, recursive %v) = %v; want %v", c.scope, c.recursive, got, want)
		}
	}
	if n := counting.lookups.Load(); n != 0 {
		t.Errorf("%d objects looked up one at a time; want none: the listing has the sums", n)
	}
	if got, err := b.PrefetchChecksums(ctx, "d", "sha256", true); got != nil || err != nil {
		t.Errorf("a prefetch of a sum the remote has not = %v, %v; want nothing and no error", got, err)
	}
}

// A remote whose sums are read from the file, as local disk's are, gives
// no folder's: that would read every file under it, where the pass wants
// only some. Its Caps says the sums come with no listing.
func TestRcloneSlowSumsAreNotPrefetched(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b, err := NewRcloneBackend(ctx, "rclone://:local:"+dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if caps, err := b.Capabilities(ctx); err != nil || caps.ListedSums {
		t.Errorf("an rclone local remote, whose sums read the file: %+v, %v; want no ListedSums", caps, err)
	}
	if got, err := b.PrefetchChecksums(ctx, "", "md5", true); got != nil || err != nil {
		t.Errorf("PrefetchChecksums on it = %v, %v; want nothing and no error", got, err)
	}
}

// The L20 case end to end: a checksum pass over an rclone remote whose
// object cache has let the listed objects go, as a tree of more than
// 100,000 leaves it, looked up each file. Now it looks none up: each
// folder's sums come from a listing, the root's too.
func TestRcloneChecksumPassLooksNothingUp(t *testing.T) {
	files := []string{"top.txt", "top2.txt", "d/a.txt", "d/b.txt", "d/sub/c.txt", "e/x.txt"}
	b, counting := rcloneWithFiles(t, "l20scan", files)
	local := t.TempDir()
	for _, p := range files {
		full := filepath.Join(local, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	sc := model.NewScanner(NewLocalBackend(local), b, 1, 1, true)
	sc.Scan(ctx, model.CompareOpts{Size: true})
	b.objs = newRcObjCache(rcloneObjCacheLimit) // what a tree of more than the cache holds leaves
	counting.lookups.Store(0)
	var root *model.TreeNode
	sc.ReadTree(func(r *model.TreeNode) { root = r })
	sc.ChecksumNode(ctx, root)
	if n := counting.lookups.Load(); n != 0 {
		t.Errorf("a checksum pass over %d files looked up %d objects one at a time; want none", len(files), n)
	}
	sc.ReadTree(func(r *model.TreeNode) {
		var walk func(n *model.TreeNode)
		walk = func(n *model.TreeNode) {
			for _, c := range n.Children {
				if c.IsDir {
					walk(c)
				} else if c.Compare.Checksum != model.AttrEqual {
					t.Errorf("%s is %v by checksum; want equal", c.RelPath, c.Compare.Checksum)
				}
			}
		}
		walk(r)
	})
}
