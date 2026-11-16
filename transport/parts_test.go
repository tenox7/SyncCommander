package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"sc/internal/sshtest"
	"sc/model"
)

// writeBackends opens dir as each backend that writes part files and runs
// on this machine: local, sftp and scp (the shell, no sftp subsystem).
func writeBackends(t *testing.T, dir string) map[string]model.Backend {
	t.Helper()
	bs := map[string]model.Backend{"local": NewLocalBackend(dir)}
	for scheme, withSFTP := range map[string]bool{"sftp": true, "scp": false} {
		b, err := OpenBackend(scheme+"://x:pw@"+sshtest.Start(t, withSFTP)+dir, true, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { CloseBackend(b) })
		bs[scheme] = b
	}
	return bs
}

func readOr(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return "<missing>"
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// L14, S8: AppendFrom created a missing file and cut or extended the one
// there to the offset, so the bytes before it were zeros or another file's.
// It must find exactly offset bytes, or write nothing.
func TestAppendNeverFillsAGap(t *testing.T) {
	ctx := context.Background()
	src := memRangeOpener("0123456789abcdefghijTAIL-0123456789")
	dir := t.TempDir()
	for name, b := range writeBackends(t, dir) {
		r, ok := b.(model.Resumer)
		if !ok {
			t.Fatalf("%s: %T cannot append", name, b)
		}
		for _, tc := range []struct{ file, before string }{
			{"missing", "<missing>"},
			{"shorter", "01234"},
			{"longer", "0123456789abcdefghijXXXXXXXXXXXXXXXXXXXX"},
		} {
			rel := name + "-" + tc.file
			if tc.before != "<missing>" {
				writeFile(t, filepath.Join(dir, rel), tc.before)
			}
			err := r.AppendFrom(ctx, rel, src, model.PutParams{Mode: 0o644}, 20)
			if !errors.Is(err, ErrAppendOffset) {
				t.Errorf("%s: append at 20 onto %s: err = %v, want ErrAppendOffset", name, tc.file, err)
			}
			if got := readOr(t, filepath.Join(dir, rel)); got != tc.before {
				t.Errorf("%s: append at 20 onto %s left %q, want it untouched", name, tc.file, got)
			}
		}
		rel := name + "-exact"
		writeFile(t, filepath.Join(dir, rel), string(src[:20]))
		if err := r.AppendFrom(ctx, rel, src, model.PutParams{Mode: 0o644}, 20); err != nil {
			t.Errorf("%s: append at 20 onto 20 bytes: %v", name, err)
		}
		if got := readOr(t, filepath.Join(dir, rel)); got != string(src) {
			t.Errorf("%s: append at 20 onto 20 bytes gave %q", name, got)
		}
	}
}

// L14, S4: a part is renamed over the real file, which replaces a file or a
// link, the link itself: its target keeps its bytes. A folder is never
// replaced, and a read-only file is (it could not be written in place).
func TestCommitPartReplacesTheNameOnly(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes read-only files")
	}
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "d")
	writeFile(t, filepath.Join(root, "outside.txt"), "keep me")
	for name, b := range writeBackends(t, dir) {
		pw := b.(model.PartWriter)
		file, link, folder, ro := name+"-file.txt", name+"-link.txt", name+"-folder", name+"-ro.txt"
		writeFile(t, filepath.Join(dir, file), "old")
		writeFile(t, filepath.Join(dir, folder, "x"), "in a folder")
		writeFile(t, filepath.Join(dir, ro), "old")
		if err := os.Chmod(filepath.Join(dir, ro), 0o444); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "outside.txt"), filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
		for _, rel := range []string{file, link, folder, ro} {
			if n, err := pw.PartSize(ctx, rel); n != 0 || err != nil {
				t.Errorf("%s: PartSize(%s) with no part = %d, %v", name, rel, n, err)
			}
			if err := b.CopyFrom(ctx, model.PartPath(rel), strings.NewReader("new "+rel), model.PutParams{Mode: model.PartMode(0o644)}); err != nil {
				t.Fatalf("%s: writing the part of %s: %v", name, rel, err)
			}
			if n, err := pw.PartSize(ctx, rel); n != int64(len("new "+rel)) || err != nil {
				t.Errorf("%s: PartSize(%s) = %d, %v; want %d", name, rel, n, err, len("new "+rel))
			}
		}
		for _, rel := range []string{file, link} {
			if err := pw.CommitPart(ctx, rel, 0o644); err != nil {
				t.Errorf("%s: CommitPart(%s): %v", name, rel, err)
			}
		}
		if err := pw.CommitPart(ctx, ro, 0o444); err != nil {
			t.Errorf("%s: CommitPart of a read-only file: %v", name, err)
		}
		if err := pw.CommitPart(ctx, folder, 0o644); !errors.Is(err, fs.ErrExist) {
			t.Errorf("%s: CommitPart onto a folder: err = %v, want fs.ErrExist", name, err)
		}
		for rel, want := range map[string]string{file: "new " + file, link: "new " + link, ro: "new " + ro, folder + "/x": "in a folder", model.PartPath(folder): "new " + folder} {
			if got := readOr(t, filepath.Join(dir, rel)); got != want {
				t.Errorf("%s: %s holds %q, want %q", name, rel, got, want)
			}
		}
		for _, rel := range []string{file, link, ro} {
			if _, err := os.Lstat(filepath.Join(dir, model.PartPath(rel))); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("%s: the part of %s is still there after the commit", name, rel)
			}
		}
		if fi, err := os.Lstat(filepath.Join(dir, link)); err != nil || !fi.Mode().IsRegular() {
			t.Errorf("%s: %s after the commit: %v, %v; want a file", name, link, fi.Mode(), err)
		}
		if fi, err := os.Stat(filepath.Join(dir, ro)); err != nil || fi.Mode().Perm() != 0o444 {
			t.Errorf("%s: %s has mode %v, %v; want -r--r--r--", name, ro, fi.Mode(), err)
		}
	}
	if got := readOr(t, filepath.Join(root, "outside.txt")); got != "keep me" {
		t.Errorf("the link's target holds %q: written through", got)
	}
}

// A link at a part's name is not a part: it is reported, never followed.
func TestPartSizeOfALink(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "d")
	writeFile(t, filepath.Join(root, "outside.txt"), "keep me")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "outside.txt"), filepath.Join(dir, model.PartPath("f.txt"))); err != nil {
		t.Fatal(err)
	}
	for name, b := range writeBackends(t, dir) {
		if n, err := b.(model.PartWriter).PartSize(context.Background(), "f.txt"); !errors.Is(err, model.ErrPartInTheWay) {
			t.Errorf("%s: PartSize with a link at the part's name = %d, %v; want ErrPartInTheWay", name, n, err)
		}
	}
}

// The engine resumes into a part only when its bytes sum the same as the
// source's first bytes: ChecksumRange must sum exactly those bytes.
func TestChecksumRangeSumsTheseBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	data := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), 4000)
	writeFile(t, filepath.Join(dir, "f.bin"), string(data))
	for name, b := range writeBackends(t, dir) {
		if p, ok := b.(model.ChecksumProber); ok {
			p.ProbeChecksums()
			p.SetChecksumAlgo("sha256")
		}
		rc := b.(model.RegionChecksummer)
		for _, r := range [][2]int64{{0, 1}, {0, 100000}, {1000, 5000}, {0, int64(len(data))}} {
			sum := sha256.Sum256(data[r[0] : r[0]+r[1]])
			got, err := rc.ChecksumRange(ctx, "f.bin", r[0], r[1])
			if err != nil || got != hex.EncodeToString(sum[:]) {
				t.Errorf("%s: ChecksumRange(%d, %d) = %s, %v; want %x", name, r[0], r[1], got, err, sum)
			}
		}
	}
}

// L14, S8 on FTP: REST past the end of a shorter or missing file leaves a
// gap of zeros on a server like vsftpd. AppendFrom must check the size.
// The commit renames over the file, deleting it first on a server that
// refuses that, and never while the part is not there to replace it.
func TestFTPParts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no fake server sockets")
	}
	ctx := context.Background()
	src := memRangeOpener("0123456789abcdefghijTAIL")
	f := &fakeFTPD{ls: map[string][]string{"/srv": nil}, store: map[string][]byte{
		"/srv/shorter": []byte("01234"),
		"/srv/exact":   []byte("0123456789abcdefghij"),
		"/srv/a.txt":   []byte("old a"),
		"/srv/b.txt":   []byte("old b"),
		"/srv/c.txt":   []byte("old c"),
	}}
	b := openFakeFTP(t, f, "/srv")
	stored := func(p string) string {
		f.mu.Lock()
		defer f.mu.Unlock()
		if body, ok := f.store[p]; ok {
			return string(body)
		}
		return "<missing>"
	}
	for _, rel := range []string{"missing", "shorter"} {
		before := stored("/srv/" + rel)
		if err := b.AppendFrom(ctx, rel, src, model.PutParams{}, 20); !errors.Is(err, ErrAppendOffset) {
			t.Errorf("append at 20 onto %s: err = %v, want ErrAppendOffset", rel, err)
		}
		if got := stored("/srv/" + rel); got != before {
			t.Errorf("append at 20 onto %s left %q, want %q", rel, got, before)
		}
	}
	if err := b.AppendFrom(ctx, "exact", src, model.PutParams{}, 20); err != nil || stored("/srv/exact") != string(src) {
		t.Errorf("append at 20 onto 20 bytes: %v, file %q", err, stored("/srv/exact"))
	}

	if n, err := b.PartSize(ctx, "a.txt"); n != 0 || err != nil {
		t.Errorf("PartSize with no part = %d, %v", n, err)
	}
	if err := b.CopyFrom(ctx, model.PartPath("a.txt"), strings.NewReader("new a"), model.PutParams{}); err != nil {
		t.Fatal(err)
	}
	if n, err := b.PartSize(ctx, "a.txt"); n != 5 || err != nil {
		t.Errorf("PartSize = %d, %v; want 5", n, err)
	}
	if err := b.CommitPart(ctx, "a.txt", 0); err != nil || stored("/srv/a.txt") != "new a" || stored("/srv/"+model.PartPath("a.txt")) != "<missing>" {
		t.Errorf("commit: %v; a.txt %q, part %q", err, stored("/srv/a.txt"), stored("/srv/"+model.PartPath("a.txt")))
	}
	f.mu.Lock()
	f.noRenameOver = true
	f.mu.Unlock()
	if err := b.CopyFrom(ctx, model.PartPath("b.txt"), strings.NewReader("new b"), model.PutParams{}); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitPart(ctx, "b.txt", 0); err != nil || stored("/srv/b.txt") != "new b" {
		t.Errorf("commit on a server that will not rename over a file: %v; b.txt %q", err, stored("/srv/b.txt"))
	}
	if err := b.CommitPart(ctx, "c.txt", 0); err == nil || stored("/srv/c.txt") != "old c" {
		t.Errorf("commit with no part: %v; c.txt %q, want an error and the file kept", err, stored("/srv/c.txt"))
	}
}
