package transport

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sc/model"
)

// removeRuns counts the --delete runs do makes in dir: the REMOVE lines the
// log gains that name it. dir is a name no other test uses.
func removeRuns(dir string, do func()) int {
	count := func() (n int) {
		for _, l := range Log.Lines() {
			if strings.Contains(l, " rsync >>> REMOVE") && strings.Contains(l, dir) {
				n++
			}
		}
		return n
	}
	before := count()
	do()
	return count() - before
}

// writeTree makes each file of rels under root, its name for its content.
func writeTree(t *testing.T, root string, rels []string) {
	t.Helper()
	for _, rel := range rels {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(rel), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// R16: a mirror deleted each entry of a folder in a --delete run of its
// own, and each run lists the folder: V deletes in a folder of N entries
// listed it V times. RemoveIn deletes them in one run, with the rules a
// Remove or a RemoveAll sends, so it deletes what they did and no more: a
// name with a pattern in it names only itself (R2). What it deletes goes
// from the listing cache and from the folders known to be there, so a
// later delete under one makes no folder (R21). A long list goes in runs
// of removeBatch, and a run that cannot delete an entry fails.
func TestRsyncRemoveInDeletesAFolderInOneRun(t *testing.T) {
	for name, start := range rsyncDaemonsUnderTest {
		t.Run(name, func(t *testing.T) {
			url, mod := start(t)
			b, err := NewRsyncBackend(url)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			const dir = "r16-in-one-run"
			files := []string{
				`a\b/f.txt`, `a\b/sub/deep.txt`, // removed
				"ab/f.txt", "ab/sub/deep.txt", // what a\b/*** names unescaped
				`z\/f.txt`,                            // removed
				"z/f.txt",                             // what z\/*** names unescaped
				`c\[d/f.txt`, "c[d/f.txt", "cd/f.txt", // the first removed
				`t\*/f.txt`, "t*/f.txt", "tx/f.txt", // the first removed
				`x\y.txt`, "xy.txt", // the first removed
				`m\[n.txt`, "m[n.txt", "mn.txt", // the first removed
				"plain.txt", "with space.txt", "keep.txt", // the first two removed
			}
			writeTree(t, filepath.Join(mod, dir), files)
			if err := b.Mkdir(ctx, dir+"/made", 0o755); err != nil { // a folder known to be there
				t.Fatal(err)
			}
			if err := b.PreloadRecursive(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if got, err := b.List(ctx, dir+`/a\b`); err != nil || len(got) != 2 {
				t.Fatalf("List(a\\b) from the cache = %d entries, %v; want 2", len(got), err)
			}
			items := []model.RemoveItem{
				{Name: `a\b`, IsDir: true}, {Name: `z\`, IsDir: true}, {Name: `c\[d`, IsDir: true}, {Name: `t\*`, IsDir: true},
				{Name: `x\y.txt`}, {Name: `m\[n.txt`}, {Name: "plain.txt"}, {Name: "with space.txt"}, {Name: "made", IsDir: true},
			}
			if n := removeRuns(dir, func() { err = b.RemoveIn(ctx, dir, items) }); err != nil || n != 1 {
				t.Errorf("RemoveIn of %d entries: %v, in %d runs; want one", len(items), err, n)
			}
			for _, rel := range files {
				gone := false
				for _, it := range items {
					gone = gone || rel == it.Name || strings.HasPrefix(rel, it.Name+"/")
				}
				_, err := os.Stat(filepath.Join(mod, dir, filepath.FromSlash(rel)))
				switch {
				case gone && err == nil:
					t.Errorf("%s is still there after RemoveIn", rel)
				case !gone && err != nil:
					t.Errorf("%s was deleted with the others: %v", rel, err)
				}
			}
			listed, err := b.List(ctx, dir)
			var names []string
			for _, e := range listed {
				names = append(names, e.Name)
			}
			slices.Sort(names)
			if want := []string{"ab", "c[d", "cd", "keep.txt", "m[n.txt", "mn.txt", "t*", "tx", "xy.txt", "z"}; err != nil || !slices.Equal(names, want) {
				t.Errorf("List(%s) after RemoveIn = %q, %v; want %q", dir, names, err, want)
			}
			if got, err := b.List(ctx, dir+`/a\b`); err == nil && len(got) > 0 {
				t.Errorf("List(a\\b) after RemoveIn gives %d entries of the folder it deleted", len(got))
			}
			if err := b.Remove(ctx, dir+"/made/x"); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("Remove under made/, deleted by RemoveIn: %v, want that the folder is not there", err)
			}
			if _, err := os.Stat(filepath.Join(mod, dir, "made")); err == nil {
				t.Error("a delete under made/ made it again")
			}
			for _, bad := range []string{"", ".", "..", "made/x", "x\ny", "x\ry"} {
				if err := b.RemoveIn(ctx, dir, []model.RemoveItem{{Name: bad}, {Name: "keep.txt"}}); err == nil {
					t.Errorf("RemoveIn took %q for a name", bad)
				}
			}
			if _, err := os.Stat(filepath.Join(mod, dir, "keep.txt")); err != nil {
				t.Errorf("keep.txt went with a RemoveIn that was refused: %v", err)
			}

			const runs = "r16-in-runs"
			writeTree(t, filepath.Join(mod, runs), []string{"f1", "f2", "f3", "f4", "f5", "keep"})
			defer func(n int) { removeBatch = n }(removeBatch)
			removeBatch = 2
			five := []model.RemoveItem{{Name: "f1"}, {Name: "f2"}, {Name: "f3"}, {Name: "f4"}, {Name: "f5"}}
			if n := removeRuns(runs, func() { err = b.RemoveIn(ctx, runs, five) }); err != nil || n != 3 {
				t.Errorf("RemoveIn of 5 in runs of 2: %v, in %d runs; want 3", err, n)
			}
			if got := treeOf(t, filepath.Join(mod, runs)); got != "keep" {
				t.Errorf("%s holds %q after RemoveIn, want keep alone", runs, got)
			}

			if os.Geteuid() == 0 {
				return // root reads a folder it may not
			}
			// A folder the daemon may not read: rsync cannot empty it, and
			// deletes the rest.
			const fails = "r16-in-fails"
			writeTree(t, filepath.Join(mod, fails), []string{"v/f.txt", "x1", "x2"})
			v := filepath.Join(mod, fails, "v")
			if err := os.Chmod(v, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(v, 0o755) })
			removeBatch = 1000
			err = b.RemoveIn(ctx, fails, []model.RemoveItem{{Name: "v", IsDir: true}, {Name: "x1"}, {Name: "x2"}})
			if err == nil {
				t.Error("RemoveIn went through with a folder it could not delete")
			}
			if err := os.Chmod(v, 0o755); err != nil {
				t.Fatal(err)
			}
			if got := treeOf(t, filepath.Join(mod, fails)); got != "v v/f.txt" {
				t.Errorf("%s holds %q after RemoveIn, want the folder it could not delete alone", fails, got)
			}
		})
	}
}

// treeOf lists what root holds, its paths in walk order.
func treeOf(t *testing.T, root string) string {
	t.Helper()
	var got []string
	err := filepath.WalkDir(root, func(p string, _ os.DirEntry, err error) error {
		if err != nil || p == root {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		got = append(got, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(got, " ")
}
