package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"sc/model"
)

func startRcloneRestic(t *testing.T, root string) (port int, stop func()) {
	t.Helper()
	bin := rcloneBin(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port = l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(bin, "serve", "restic", root,
		"--addr", fmt.Sprintf("127.0.0.1:%d", port),
		"--user", "u", "--pass", "p")
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop = func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait() }) }
	t.Cleanup(stop)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if derr == nil {
			c.Close()
			time.Sleep(100 * time.Millisecond)
			return port, stop
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rclone restic did not come up")
	return 0, stop
}

func sha256hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// resticData lists data the way a scan does, its shard folders and then
// each of them, and returns each object's size by name; every object must
// sit at its one path.
func resticData(t *testing.T, b *ResticBackend) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	shards, err := b.List(ctx, "data")
	if err != nil {
		t.Fatalf("List data: %v", err)
	}
	sizes := map[string]int64{}
	for _, s := range shards {
		if !s.IsDir || s.RelPath != "data/"+s.Name || len(s.Name) != 2 {
			t.Fatalf("data holds %+v; want shard folders only", s)
		}
		objs, err := b.List(ctx, s.RelPath)
		if err != nil {
			t.Fatalf("List %s: %v", s.RelPath, err)
		}
		for _, o := range objs {
			if o.IsDir || o.RelPath != s.RelPath+"/"+o.Name || o.Name[:2] != s.Name {
				t.Fatalf("%s holds %+v", s.RelPath, o)
			}
			sizes[o.Name] = o.Size
		}
	}
	return sizes
}

func resticPut(t *testing.T, b *ResticBackend, relPath string, content []byte) {
	t.Helper()
	if err := b.CopyFrom(context.Background(), relPath, bytes.NewReader(content), model.PutParams{Size: int64(len(content)), Mode: 0o644}); err != nil {
		t.Fatalf("CopyFrom %s: %v", relPath, err)
	}
}

// TestResticRoundTrip drives a live `rclone serve restic` through the full
// backend surface: seed a repo by POSTing content-addressed objects, then
// verify the depth-0 root listing, per-type listing with sizes, the free
// name-is-sha256 checksum, whole + ranged (resume) reads, delete, and the
// recursive preload cache — the cache is proven by killing the server and
// re-listing every dir without any network.
func TestResticRoundTrip(t *testing.T) {
	SetResticDeletes(true)
	defer SetResticDeletes(false)
	root := t.TempDir()
	port, stop := startRcloneRestic(t, root)

	var b *ResticBackend
	var err error
	for i := 0; i < 20; i++ {
		b, err = NewResticBackend(context.Background(), fmt.Sprintf("restic://u:p@127.0.0.1:%d/", port), false, 8)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("NewResticBackend: %v", err)
	}
	ctx := context.Background()

	configContent := []byte(`{"version":2,"id":"abc","chunker_polynomial":"25b468838dcb75"}`)
	resticPut(t, b, "config", configContent)

	dataContents := [][]byte{
		bytes.Repeat([]byte("alpha-pack-"), 4096),
		bytes.Repeat([]byte("beta-pack-"), 8192),
		[]byte("gamma small pack"),
	}
	dataHashes := make([]string, len(dataContents))
	for i, c := range dataContents {
		dataHashes[i] = sha256hex(c)
		resticPut(t, b, resticPath("data", dataHashes[i]), c)
	}
	indexContent := []byte(`{"packs":[]}`)
	indexHash := sha256hex(indexContent)
	resticPut(t, b, "index/"+indexHash, indexContent)
	snapContent := []byte(`{"time":"2026-05-26T00:00:00Z","tree":"deadbeef"}`)
	snapHash := sha256hex(snapContent)
	resticPut(t, b, "snapshots/"+snapHash, snapContent)

	// Depth-0 root: five type dirs plus config, synthesized without a list call.
	rootEntries, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List root: %v", err)
	}
	wantRoot := []string{"config|false", "data|true", "index|true", "keys|true", "locks|true", "snapshots|true"}
	if got := listSignature(rootEntries); !eqStrings(got, wantRoot) {
		t.Fatalf("root listing:\n got=%v\nwant=%v", got, wantRoot)
	}
	for _, e := range rootEntries {
		if e.Name == "config" && e.Size != int64(len(configContent)) {
			t.Fatalf("config size = %d, want %d", e.Size, len(configContent))
		}
	}

	// data/ holds shard folders, as restic keeps it on disk, each object
	// with its real size.
	sizeByName := resticData(t, b)
	if len(sizeByName) != len(dataContents) {
		t.Fatalf("data has %d objects, want %d", len(sizeByName), len(dataContents))
	}
	for i, h := range dataHashes {
		if sizeByName[h] != int64(len(dataContents[i])) {
			t.Fatalf("data/%s size = %d, want %d", h, sizeByName[h], len(dataContents[i]))
		}
	}

	// Checksum is the object name (== sha256 of content), with no download.
	for i, h := range dataHashes {
		sum, err := b.Checksum(ctx, resticPath("data", h), "sha256")
		if err != nil {
			t.Fatalf("Checksum data/%s: %v", h, err)
		}
		if sum != h || sum != sha256hex(dataContents[i]) {
			t.Fatalf("Checksum data/%s = %s, want %s", h, sum, h)
		}
	}

	// Whole-object read, then a ranged read from an offset (the resume path).
	rc, err := b.Open(ctx, resticPath("data", dataHashes[0]))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, dataContents[0]) {
		t.Fatalf("Open data/%s content mismatch (%d bytes)", dataHashes[0], len(got))
	}
	offset := int64(len(dataContents[0]) / 3)
	rc, err = b.OpenAt(ctx, resticPath("data", dataHashes[0]), offset)
	if err != nil {
		t.Fatalf("OpenAt: %v", err)
	}
	gotTail, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(gotTail, dataContents[0][offset:]) {
		t.Fatalf("OpenAt @%d content mismatch (%d bytes)", offset, len(gotTail))
	}

	// Delete one object; it must disappear from a live listing.
	if err := b.Remove(ctx, resticPath("data", dataHashes[2])); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	sizeByName = resticData(t, b)
	if len(sizeByName) != len(dataContents)-1 {
		t.Fatalf("data has %d objects after delete, want %d", len(sizeByName), len(dataContents)-1)
	}
	if _, ok := sizeByName[dataHashes[2]]; ok {
		t.Fatalf("deleted object data/%s still listed", dataHashes[2])
	}

	// Recursive preload: one GET per type into the cache. Await it via List(""),
	// then kill the server — every dir must still list from cache, no network.
	if err := b.PreloadRecursive(ctx, ""); err != nil {
		t.Fatalf("PreloadRecursive: %v", err)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatalf("List root (await preload): %v", err)
	}
	stop()

	cases := map[string][]string{
		"":          wantRoot,
		"index":     {indexHash + "|false"},
		"snapshots": {snapHash + "|false"},
		"keys":      {},
		"locks":     {},
	}
	for dir, want := range cases {
		entries, err := b.List(ctx, dir)
		if err != nil {
			t.Fatalf("List %q after shutdown: %v", dir, err)
		}
		if got := listSignature(entries); !eqStrings(got, want) {
			t.Fatalf("cached dir %q:\n got=%v\nwant=%v", dir, got, want)
		}
	}
	if cached := resticData(t, b); len(cached) != len(dataContents)-1 {
		t.Fatalf("cached data has %d objects, want %d", len(cached), len(dataContents)-1)
	}
}
