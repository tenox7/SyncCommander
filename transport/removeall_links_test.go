package transport

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sc/model"
)

// A folder delete removes a link as a link and never deletes through one:
// not a link inside the folder, and not a link at the path itself, which
// is what a listed folder is when it was swapped for a link after the scan.
// The folders the links point to sit outside the base, in the test's own
// folder.
func TestRemoveAllNeverFollowsALink(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(t *testing.T, base string) model.Backend
	}{
		{"local", func(t *testing.T, base string) model.Backend { return NewLocalBackend(base) }},
		{"sftp", func(t *testing.T, base string) model.Backend {
			b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+startSFTPServer(t)+filepath.ToSlash(base), true, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { b.Close() })
			return b
		}},
		{"scp", func(t *testing.T, base string) model.Backend {
			b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+filepath.ToSlash(base), true, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { CloseBackend(b) })
			return b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			base := filepath.Join(root, "a", "base")
			putFile(t, filepath.Join(base, "d", "f.txt"), "f")
			putFile(t, filepath.Join(base, "kept.txt"), "kept")
			for _, out := range []string{"out1", "out2"} {
				putFile(t, filepath.Join(root, out, "keep.txt"), "keep")
			}
			for link, target := range map[string]string{"d/in-link": "out1", "top-link": "out2"} {
				if err := os.Symlink(filepath.Join(root, target), filepath.Join(base, link)); err != nil {
					t.Skip("no symbolic links here:", err)
				}
			}
			b := tc.open(t, base)
			for _, rel := range []string{"d", "top-link"} {
				if err := b.RemoveAll(ctx, rel); err != nil {
					t.Errorf("RemoveAll(%q) = %v", rel, err)
				}
			}
			for _, out := range []string{"out1", "out2"} {
				if _, err := os.Stat(filepath.Join(root, out, "keep.txt")); err != nil {
					t.Errorf("%s/keep.txt was deleted through a link: %v", out, err)
				}
			}
			if got := filesUnder(t, base); !slices.Equal(got, []string{"kept.txt"}) {
				t.Errorf("the base holds %q, want kept.txt alone", got)
			}
		})
	}
}

// The same over FTP, which has no lstat: a server lists the folder a link
// points to when asked for the link's path. top-link is listed in its
// folder as a link, and listed through as the server does; what is stored
// under top-link/ stands for the folder it points to.
func TestFTPRemoveAllNeverFollowsALink(t *testing.T) {
	link := "lrwxrwxrwx    1 1000     1000            9 Nov 16 10:00 top-link -> /srv/out2"
	f := &fakeFTPD{
		ls: map[string][]string{
			"/srv":               {lsLine("base", true), lsLine("out2", true)},
			"/srv/base":          {lsLine("d", true), link},
			"/srv/base/d":        {lsLine("f.txt", false)},
			"/srv/base/top-link": {lsLine("keep.txt", false)},
			"/srv/out2":          {lsLine("keep.txt", false)},
		},
		store: map[string][]byte{
			"/srv/base/d/f.txt":           []byte("f"),
			"/srv/base/top-link":          []byte("/srv/out2"),
			"/srv/base/top-link/keep.txt": []byte("keep"),
		},
	}
	b := openFakeFTP(t, f, "/srv/base")
	for _, rel := range []string{"top-link", "d"} {
		if err := b.RemoveAll(context.Background(), rel); err != nil {
			t.Errorf("RemoveAll(%q) = %v", rel, err)
		}
	}
	if got, want := f.storedPaths(), []string{"/srv/base/top-link/keep.txt"}; !slices.Equal(got, want) {
		t.Errorf("the server holds %q, want %q: the link and d removed, nothing through the link", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if want := []string{"/srv/base/d"}; !slices.Equal(f.rmd, want) {
		t.Errorf("RMD %q, want %q: a link is no folder", f.rmd, want)
	}
}

// What a name is comes from its folder's listing. When that listing
// fails, the folder delete does not guess: it fails and removes nothing.
func TestFTPRemoveAllFailsWhenItsFolderCannotBeListed(t *testing.T) {
	f := &fakeFTPD{
		ls: map[string][]string{
			"/srv":        {lsLine("base", true)},
			"/srv/base":   {lsLine("d", true), "this line is no listing"},
			"/srv/base/d": {lsLine("f.txt", false)},
		},
		store: map[string][]byte{"/srv/base/d/f.txt": []byte("f")},
	}
	b := openFakeFTP(t, f, "/srv/base")
	if err := b.RemoveAll(context.Background(), "d"); err == nil {
		t.Error("RemoveAll(d) = nil, want the listing's error")
	}
	if got, want := f.storedPaths(), []string{"/srv/base/d/f.txt"}; !slices.Equal(got, want) {
		t.Errorf("the server holds %q, want %q", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rmd) != 0 {
		t.Errorf("RMD %q, want none", f.rmd)
	}
}
