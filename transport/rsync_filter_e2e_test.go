package transport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// R2: a delete names its victim in filter rules. rsync reads a rule as a
// wildcard pattern when it holds *, ? or [, and the "/name/***" rule always
// does; in a pattern a backslash makes the next byte stand for itself. A
// backslash left as is in that rule named another folder: RemoveAll("a\b")
// kept a\b and deleted what was in its sibling ab.
func TestRsyncRemoveBackslashNames(t *testing.T) {
	port, mod := startRsyncDaemon(t)
	checkRemoveBackslashNames(t, fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port), mod)
}

func TestGorsyncdRemoveBackslashNames(t *testing.T) {
	url, mod := startGorsyncd(t)
	checkRemoveBackslashNames(t, url, mod)
}

func checkRemoveBackslashNames(t *testing.T, url, mod string) {
	files := []string{
		`bs/a\b/f.txt`, `bs/a\b/sub/deep.txt`, // removed
		"bs/ab/f.txt", "bs/ab/sub/deep.txt", // what a\b/*** names unescaped
		`bs/z\/f.txt`,                                  // removed
		"bs/z/f.txt",                                   // what z\/*** names unescaped
		`bs/c\[d/f.txt`, "bs/c[d/f.txt", "bs/cd/f.txt", // the first removed
		`bs/t\*/f.txt`, "bs/t*/f.txt", "bs/tx/f.txt", // the first removed
		`bs/x\y.txt`, "bs/xy.txt", // the first removed
		`bs/m\[n.txt`, "bs/m[n.txt", "bs/mn.txt", // the first removed
	}
	for _, rel := range files {
		full := filepath.Join(mod, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(rel), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := NewRsyncBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	removed := []string{`bs/a\b`, `bs/z\`, `bs/c\[d`, `bs/t\*`}
	for _, rel := range removed {
		if err := b.RemoveAll(ctx, rel); err != nil {
			t.Errorf("RemoveAll(%q): %v", rel, err)
		}
	}
	for _, rel := range []string{`bs/x\y.txt`, `bs/m\[n.txt`} {
		if err := b.Remove(ctx, rel); err != nil {
			t.Errorf("Remove(%q): %v", rel, err)
		}
		removed = append(removed, rel)
	}
	for _, rel := range files {
		gone := false
		for _, r := range removed {
			gone = gone || rel == r || strings.HasPrefix(rel, r+"/")
		}
		_, err := os.Stat(filepath.Join(mod, filepath.FromSlash(rel)))
		switch {
		case gone && err == nil:
			t.Errorf("%s is still there after its remove", rel)
		case !gone && err != nil:
			t.Errorf("%s was deleted by the remove of another name: %v", rel, err)
		}
	}
}
