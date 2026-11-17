package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"sc/model"
)

func TestFakeDeterministic(t *testing.T) {
	a, err := NewFakeBackend("fake://small")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewFakeBackend("fake://small")
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"", "d00000", "d00001/d00002"} {
		ea, _ := a.List(context.Background(), dir)
		eb, _ := b.List(context.Background(), dir)
		if len(ea) != len(eb) || len(ea) == 0 {
			t.Fatalf("%q: %d vs %d entries", dir, len(ea), len(eb))
		}
		for i := range ea {
			if ea[i] != eb[i] {
				t.Fatalf("%q entry %d: %+v vs %+v", dir, i, ea[i], eb[i])
			}
		}
	}
}

func TestFakeShape(t *testing.T) {
	b, err := NewFakeBackend("fake://x?dirs=3&files=4&depth=2")
	if err != nil {
		t.Fatal(err)
	}
	count := func(dir string) (dirs, files int) {
		entries, err := b.List(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir {
				dirs++
				continue
			}
			files++
		}
		return
	}
	if d, f := count(""); d != 3 || f != 4 {
		t.Errorf("root: got %dd %df, want 3d 4f", d, f)
	}
	if d, f := count("d00000"); d != 3 || f != 4 {
		t.Errorf("level 1: got %dd %df, want 3d 4f", d, f)
	}
	if d, f := count("d00000/d00001"); d != 0 || f != 4 {
		t.Errorf("leaf level: got %dd %df, want 0d 4f", d, f)
	}
}

// diff= changes a file in one of three ways, a third each: it grows (the same
// first bytes, more of them, a later time), it is rewritten (the same size,
// other bytes, a later time), or only a checksum tells (the same size and
// time, other bytes). Before L22 every changed file grew. drop= takes files
// away.
func TestFakeDiffAndDrop(t *testing.T) {
	ctx := context.Background()
	plain, _ := NewFakeBackend("fake://x?dirs=0&files=2000&depth=1&size=4096")
	skewed, _ := NewFakeBackend("fake://x?dirs=0&files=2000&depth=1&size=4096&diff=0.1&drop=0.05")
	base, _ := plain.List(ctx, "")
	other, _ := skewed.List(ctx, "")

	dropped := len(base) - len(other)
	if dropped < 60 || dropped > 140 {
		t.Errorf("drop=0.05 of 2000 removed %d entries, want ~100", dropped)
	}

	byName := make(map[string]model.FileEntry, len(base))
	for _, e := range base {
		byName[e.Name] = e
	}
	body := func(b *FakeBackend, rel string) []byte {
		t.Helper()
		rd, err := b.Open(ctx, rel)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		data, err := io.ReadAll(rd)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	var grown, rewritten, silent int
	for _, e := range other {
		was, ok := byName[e.Name]
		if !ok {
			continue
		}
		a, b := body(plain, e.RelPath), body(skewed, e.RelPath)
		later := e.ModTime.After(was.ModTime)
		switch {
		case e.Size > was.Size && later && bytes.HasPrefix(b, a):
			grown++
		case e.Size == was.Size && later && (e.Size == 0 || !bytes.Equal(a, b)):
			rewritten++
		case e.Size == was.Size && e.ModTime.Equal(was.ModTime) && !bytes.Equal(a, b):
			silent++
		case e != was || !bytes.Equal(a, b):
			t.Errorf("%s changed in a way diff= does not make: %+v, was %+v", e.Name, e, was)
		}
	}
	for kind, n := range map[string]int{"grew": grown, "were rewritten": rewritten, "changed so only a checksum tells": silent} {
		if n < 35 || n > 100 {
			t.Errorf("diff=0.1 of ~1900: %d files %s, want ~63", n, kind)
		}
	}
}

func TestFakeChecksumMatchesContent(t *testing.T) {
	b, _ := NewFakeBackend("fake://tiny")
	entries, _ := b.List(context.Background(), "")
	var rel string
	for _, e := range entries {
		if !e.IsDir && e.Size > 0 {
			rel = e.RelPath
			break
		}
	}
	if rel == "" {
		t.Fatal("no file generated")
	}
	rd, err := b.Open(context.Background(), rel)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	h := sha256.New()
	n, err := io.Copy(h, rd)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := b.Checksum(context.Background(), rel, "sha256")
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sum {
		t.Errorf("Open content hashes to %s, Checksum says %s (%d bytes)", got, sum, n)
	}
}

func TestFakeOverlay(t *testing.T) {
	ctx := context.Background()
	b, _ := NewFakeBackend("fake://tiny")

	if err := b.CopyFrom(ctx, "d00000/new.txt", strings.NewReader("hello"), model.PutParams{Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	entries, _ := b.List(ctx, "d00000")
	found := false
	for _, e := range entries {
		if e.Name == "new.txt" {
			found = true
			if e.Size != 5 {
				t.Errorf("written file size %d, want 5", e.Size)
			}
		}
	}
	if !found {
		t.Error("written file missing from listing")
	}

	before, _ := b.List(ctx, "")
	if err := b.Remove(ctx, before[len(before)-1].RelPath); err != nil {
		t.Fatal(err)
	}
	after, _ := b.List(ctx, "")
	if len(after) != len(before)-1 {
		t.Errorf("after Remove: %d entries, want %d", len(after), len(before)-1)
	}

	if err := b.RemoveAll(ctx, "d00000"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, "d00000/d00001"); err == nil {
		t.Error("listing under a removed dir should fail")
	}
}
