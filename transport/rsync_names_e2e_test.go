package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// R1: a stock rsync daemon reads each part of a path argument that holds *,
// ? or [ as a pattern, and splits the argument at " <module>/". Every
// operation must reach the named path, not a sibling the pattern matches.
// Each folder has a decoy that the name, read as a pattern, matches.
func TestRsyncDaemonNamesAreNotPatterns(t *testing.T) {
	port, mod := startRsyncDaemon(t)
	url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port)
	b, err := NewRsyncBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	body := bytes.Repeat([]byte("0123456789"), 1000)
	files := map[string]string{
		"s2/d[1]/victim.txt":          "bracket victim",
		"s2/d[1]/only-in-bracket.txt": "b",
		"s2/d[1]/sub/keep.txt":        "bracket sub",
		"s2/d1/victim.txt":            "d1 victim",
		"s2/d1/only-in-d1.txt":        "d",
		"s2/d1/sub/decoy.txt":         "d1 sub",
		"old testmod/x.txt":           "in old testmod",
		"old/x.txt":                   "in old",
		"x.txt":                       "top x",
		"a*b/f.txt":                   "star",
		"axb/f.txt":                   "axb",
		`q\[z/f.txt`:                  "backslash bracket",
		"q[z/f.txt":                   "bracket z",
		"plain/victim.txt":            "bracket victim",
		"s2/d[1]/grow.bin":            string(body[:4000]),
		"s2/d1/grow.bin":              string(body[:4000]),
	}
	for rel, data := range files {
		full := filepath.Join(mod, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	read := func(rel string) string {
		got, err := os.ReadFile(filepath.Join(mod, filepath.FromSlash(rel)))
		if err != nil {
			return "<" + err.Error() + ">"
		}
		return string(got)
	}

	for dir, want := range map[string][]string{
		"s2/d[1]":     {"grow.bin", "only-in-bracket.txt", "sub", "victim.txt"},
		"old testmod": {"x.txt"},
		"a*b":         {"f.txt"},
		`q\[z`:        {"f.txt"},
	} {
		if got, err := listNames(ctx, b, dir); err != nil || !slices.Equal(got, want) {
			t.Errorf("List(%q) = %q, %v; want %q", dir, got, err, want)
		}
	}
	deep, err := NewRsyncBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := deep.PreloadRecursive(ctx, "s2/d[1]"); err != nil {
		t.Fatal(err)
	}
	if got, err := listNames(ctx, deep, "s2/d[1]/sub"); err != nil || !slices.Equal(got, []string{"keep.txt"}) {
		t.Errorf("after a deep listing of s2/d[1]: List(s2/d[1]/sub) = %q, %v", got, err)
	}

	open := func(rel string, off int64) string {
		r, err := b.OpenAt(ctx, rel, off)
		if err != nil {
			return "<" + err.Error() + ">"
		}
		defer r.Close()
		got, _ := io.ReadAll(r)
		return string(got)
	}
	for rel, want := range map[string]string{
		"s2/d[1]/victim.txt": "bracket victim",
		"old testmod/x.txt":  "in old testmod",
		"a*b/f.txt":          "star",
		`q\[z/f.txt`:         "backslash bracket",
	} {
		if got := open(rel, 0); got != want {
			t.Errorf("Open(%q) = %q, want %q", rel, got, want)
		}
	}
	if got := open("s2/d[1]/victim.txt", 8); got != "victim" {
		t.Errorf("OpenAt(s2/d[1]/victim.txt, 8) = %q, want %q", got, "victim")
	}
	dl := filepath.Join(t.TempDir(), "victim.txt")
	if err := b.RecvToLocalFile(ctx, "s2/d[1]/victim.txt", dl); err != nil {
		t.Errorf("RecvToLocalFile: %v", err)
	} else if got, _ := os.ReadFile(dl); string(got) != "bracket victim" {
		t.Errorf("RecvToLocalFile(s2/d[1]/victim.txt) wrote %q", got)
	}

	b.SetChecksumAlgo("md4")
	sum, err1 := b.Checksum(ctx, "s2/d[1]/victim.txt")
	same, err2 := b.Checksum(ctx, "plain/victim.txt")
	if err1 != nil || err2 != nil || sum != same {
		t.Errorf("md4 of s2/d[1]/victim.txt = %q, %v; of the same bytes in plain/victim.txt %q, %v", sum, err1, same, err2)
	}

	if err := b.AppendFrom(ctx, "s2/d[1]/grow.bin", memRangeOpener(body), 0o644, 4000); err != nil {
		t.Errorf("AppendFrom: %v", err)
	}
	if got := read("s2/d[1]/grow.bin"); got != string(body) {
		t.Errorf("after AppendFrom, s2/d[1]/grow.bin has %d bytes, want %d", len(got), len(body))
	}
	if got := read("s2/d1/grow.bin"); len(got) != 4000 {
		t.Errorf("AppendFrom to s2/d[1]/grow.bin changed s2/d1/grow.bin to %d bytes", len(got))
	}

	if err := b.Remove(ctx, "s2/d[1]/victim.txt"); err != nil {
		t.Errorf("Remove: %v", err)
	}
	if err := b.RemoveAll(ctx, "s2/d[1]/sub"); err != nil {
		t.Errorf("RemoveAll: %v", err)
	}
	if err := b.Remove(ctx, "old testmod/x.txt"); err != nil {
		t.Errorf("Remove: %v", err)
	}
	for rel, want := range map[string]string{
		"s2/d1/victim.txt":    "d1 victim",
		"s2/d1/sub/decoy.txt": "d1 sub",
		"old/x.txt":           "in old",
		"x.txt":               "top x",
	} {
		if got := read(rel); got != want {
			t.Errorf("after the removes, %s = %q, want %q: a remove hit the wrong folder", rel, got, want)
		}
	}
	for _, rel := range []string{"s2/d[1]/victim.txt", "s2/d[1]/sub", "old testmod/x.txt"} {
		if _, err := os.Lstat(filepath.Join(mod, filepath.FromSlash(rel))); err == nil {
			t.Errorf("%s is still there after its remove", rel)
		}
	}

	// A pattern that matches nothing is used as a plain name: a push into a
	// missing gone[1] would create a folder named gone\[1\] and succeed.
	if err := os.MkdirAll(filepath.Join(mod, "gone1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove(ctx, "gone[1]/x.txt"); err == nil {
		t.Error("Remove(gone[1]/x.txt) with no gone[1] folder succeeded")
	}
	if err := b.AppendFrom(ctx, "gone[1]/grow.bin", memRangeOpener(body), 0o644, 4000); err == nil {
		t.Error("AppendFrom(gone[1]/grow.bin) with no gone[1] folder succeeded")
	}
	names, _ := os.ReadDir(mod)
	for _, e := range names {
		if strings.HasPrefix(e.Name(), "gone") && e.Name() != "gone1" {
			t.Errorf("a push created %q on the daemon", e.Name())
		}
	}

	for _, rel := range []string{"bad\nname", "s2/bad\rname"} {
		if _, err := b.List(ctx, rel); err == nil {
			t.Errorf("List(%q) took a name with a line break", rel)
		}
		if err := b.Remove(ctx, rel+"/x"); err == nil {
			t.Errorf("Remove(%q) took a name with a line break", rel+"/x")
		}
	}
}
