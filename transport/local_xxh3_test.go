package transport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/xxh3"

	"sc/model"
)

func TestLocalChecksumXXH3(t *testing.T) {
	dir := t.TempDir()
	data := []byte("the quick brown fox jumps over the lazy dog")
	if err := os.WriteFile(filepath.Join(dir, "f"), data, 0644); err != nil {
		t.Fatal(err)
	}

	b := NewLocalBackend(dir)
	b.SetChecksumAlgo("xxh3")
	got, err := b.Checksum(context.Background(), "f")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%016x", xxh3.Hash(data))
	if got != want {
		t.Fatalf("xxh3 checksum = %q, want %q", got, want)
	}
}

func TestLocalXXH3NegotiatedAndCompared(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	write := func(base, name, content string) {
		if err := os.WriteFile(filepath.Join(base, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write(left, "same", "identical")
	write(right, "same", "identical")
	write(left, "diff", "alpha")
	write(right, "diff", "beta")

	s := model.NewScanner(NewLocalBackend(left), NewLocalBackend(right), 4, 8, true)
	s.Scan(context.Background(), true, false, false, false)

	if algo := s.ChecksumAlgo(); algo != "xxh3" {
		t.Fatalf("negotiated algo = %q, want xxh3", algo)
	}

	byName := map[string]*model.TreeNode{}
	for _, c := range s.Tree().Children {
		byName[c.Name] = c
	}

	same := byName["same"]
	if same == nil || same.Compare.Checksum != model.AttrEqual {
		t.Fatalf("same: checksum status = %v, want AttrEqual", same)
	}
	if want := fmt.Sprintf("%016x", xxh3.Hash([]byte("identical"))); same.LeftChecksum != want {
		t.Fatalf("same: LeftChecksum = %q, want %q", same.LeftChecksum, want)
	}

	diff := byName["diff"]
	if diff == nil || diff.Compare.Checksum != model.AttrDifferent {
		t.Fatalf("diff: checksum status = %v, want AttrDifferent", diff)
	}
}
