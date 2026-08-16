package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
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

func TestFakeDiffAndDrop(t *testing.T) {
	plain, _ := NewFakeBackend("fake://x?dirs=0&files=2000&depth=1")
	skewed, _ := NewFakeBackend("fake://x?dirs=0&files=2000&depth=1&diff=0.1&drop=0.05")
	base, _ := plain.List(context.Background(), "")
	other, _ := skewed.List(context.Background(), "")

	dropped := len(base) - len(other)
	if dropped < 60 || dropped > 140 {
		t.Errorf("drop=0.05 of 2000 removed %d entries, want ~100", dropped)
	}

	byName := make(map[string]int64, len(base))
	for _, e := range base {
		byName[e.Name] = e.Size
	}
	differing := 0
	for _, e := range other {
		if size, ok := byName[e.Name]; ok && size != e.Size {
			differing++
		}
	}
	if differing < 130 || differing > 270 {
		t.Errorf("diff=0.1 of ~1900 perturbed %d entries, want ~190", differing)
	}
}

func TestFakeChecksumMatchesContent(t *testing.T) {
	b, _ := NewFakeBackend("fake://tiny")
	b.SetChecksumAlgo("sha256")
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
	sum, err := b.Checksum(context.Background(), rel)
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

	if err := b.CopyFrom(ctx, "d00000/new.txt", strings.NewReader("hello"), 0644); err != nil {
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
