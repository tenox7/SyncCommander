package model

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIsFileName(t *testing.T) {
	for name, want := range map[string]bool{
		"a.txt": true, ".hidden": true, "...": true, "a b": true, `back\slash`: true, "ünï": true,
		`\x`: true, `x\`: true, `.\x`: true, `...\x`: true, `x\..y`: true,
		"": false, ".": false, "..": false, "../x": false, "a/b": false, "/": false, "x/": false, "nul\x00": false,
		`..\x`: false, `x\..`: false, `a\..\..\b`: false, `..\..\.ssh\authorized_keys`: false,
	} {
		if got := IsFileName(name); got != want {
			t.Errorf("IsFileName(%q) = %v, want %v", name, got, want)
		}
	}
}

// L7: a server lists the names it likes. A name that is no file name, or a
// RelPath that is not its folder joined with the name, never reaches the
// tree, whose paths copies and deletes act on, and each listing with one
// is reported once.
func TestScanLeavesOutNamesThatAreNoFileNames(t *testing.T) {
	var mu sync.Mutex
	var logged []string
	old := LogErr
	LogErr = func(proto, msg string) {
		mu.Lock()
		defer mu.Unlock()
		logged = append(logged, proto+": "+msg)
	}
	t.Cleanup(func() { LogErr = old })

	top := []FileEntry{
		fileEntry("ok.txt", "ok.txt", 1),
		fileEntry("../../evil", "../../evil", 2),
		fileEntry("a/b", "a/b", 3),
		fileEntry(`..\..\evil`, `..\..\evil`, 3),
		dirEntry("", ".."),
		fileEntry("nul\x00", "nul\x00", 4),
		fileEntry("other/x", "x", 5), // a name with another folder's path
		dirEntry("d", "d"),
	}
	inD := []FileEntry{
		fileEntry("d/in.txt", "in.txt", 6),
		dirEntry("d/../..", "../.."),
	}
	left := &stubBackend{dirs: map[string][]FileEntry{"": top, "d": inD}}
	right := &stubBackend{dirs: map[string][]FileEntry{}}
	topWas, inDWas := slices.Clone(top), slices.Clone(inD)

	// A folder listed with its parent's path made an old build scan for
	// ever, each time one level deeper.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s := NewScanner(left, right, 1, 1, true)
	s.Scan(ctx, CompareOpts{})
	if ctx.Err() != nil {
		t.Fatal("the scan did not end")
	}

	var paths []string
	s.ReadTree(func(root *TreeNode) {
		var walk func(n *TreeNode)
		walk = func(n *TreeNode) {
			for _, c := range n.Children {
				paths = append(paths, c.RelPath+" ("+c.Name+")")
				walk(c)
			}
		}
		walk(root)
	})
	slices.Sort(paths)
	if want := []string{"d (d)", "d/in.txt (in.txt)", "ok.txt (ok.txt)"}; !slices.Equal(paths, want) {
		t.Errorf("tree holds %q, want %q", paths, want)
	}

	mu.Lock()
	got := slices.Clone(logged)
	mu.Unlock()
	slices.Sort(got)
	if len(got) != 2 ||
		!strings.Contains(got[0], "left /: left out 6 listed entries") || !strings.Contains(got[0], `"../../evil"`) ||
		!strings.Contains(got[1], "left d: left out 1 listed entry") || !strings.Contains(got[1], `"../.."`) {
		t.Errorf("logged %q, want one line for / (6, the first ../../evil) and one for d (1, ../..)", got)
	}
	// A backend may serve a listing from its cache: the scan leaves it be.
	if !slices.Equal(top, topWas) || !slices.Equal(inD, inDWas) {
		t.Error("the scan changed the listings the backend handed it")
	}
}
