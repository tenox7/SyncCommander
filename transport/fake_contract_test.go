package transport

// The fake keeps the rules of a real file system (L22). Before, it was a
// tree generator with writes on top: any path listed as a full generated
// folder, and a copy wrote a file without its folders.

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

func newFake(t *testing.T, url string) *FakeBackend {
	t.Helper()
	b, err := NewFakeBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeNames lists relDir as sorted names, folders with a trailing slash.
func fakeNames(t *testing.T, b *FakeBackend, relDir string) []string {
	t.Helper()
	entries, err := b.List(context.Background(), relDir)
	if err != nil {
		t.Fatalf("List %q: %v", relDir, err)
	}
	var out []string
	for _, e := range entries {
		if e.RelPath != joinRel(relDir, e.Name) {
			t.Errorf("List %q: %s has the path %q", relDir, e.Name, e.RelPath)
		}
		name := e.Name
		if e.IsDir {
			name += "/"
		}
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

func wantNames(t *testing.T, b *FakeBackend, relDir string, want ...string) {
	t.Helper()
	if got := fakeNames(t, b, relDir); !slices.Equal(got, want) {
		t.Errorf("%q lists %q, want %q", relDir, got, want)
	}
}

func fakeRead(t *testing.T, b *FakeBackend, relPath string) string {
	t.Helper()
	rd, err := b.Open(context.Background(), relPath)
	if err != nil {
		t.Fatalf("Open %s: %v", relPath, err)
	}
	defer rd.Close()
	data, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(data)
}

func fakeWrite(t *testing.T, b *FakeBackend, relPath, body string) {
	t.Helper()
	if err := b.CopyFrom(context.Background(), relPath, strings.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err != nil {
		t.Fatalf("CopyFrom %s: %v", relPath, err)
	}
}

// A copy makes the folders it goes into, and they hold only what was written
// into them; over a removed file or folder too, as after a type conflict.
// The fake wrote the file and not its folders: a new folder listed as a
// generated one, without the file, and a file written where something was
// removed could not be read.
func TestFakeCopyMakesItsFolders(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")

	fakeWrite(t, b, "new/sub/f.txt", "hello")
	wantNames(t, b, "new", "sub/")
	wantNames(t, b, "new/sub", "f.txt")
	if got := fakeRead(t, b, "new/sub/f.txt"); got != "hello" {
		t.Errorf("new/sub/f.txt holds %q", got)
	}
	if e, err := b.Stat(ctx, "new/sub"); err != nil || !e.IsDir {
		t.Errorf("new/sub: %+v, %v; want a folder", e, err)
	}

	// A copy over a generated file replaces it, with the copy's mode.
	before := fakeNames(t, b, "d00002")
	if err := b.CopyFrom(ctx, "d00002/f00003.dat", strings.NewReader("replaced"), model.PutParams{Mode: 0o600}); err != nil {
		t.Fatal(err)
	}
	wantNames(t, b, "d00002", before...)
	if got := fakeRead(t, b, "d00002/f00003.dat"); got != "replaced" {
		t.Errorf("d00002/f00003.dat holds %q", got)
	}
	if e, err := b.Stat(ctx, "d00002/f00003.dat"); err != nil || e.Mode != 0o600 || e.Size != 8 {
		t.Errorf("d00002/f00003.dat: %+v, %v; want 8 bytes, mode 0600", e, err)
	}

	// A file's name becomes a folder's, as a copy over a type conflict does.
	if err := b.Remove(ctx, "f00001.dat"); err != nil {
		t.Fatal(err)
	}
	fakeWrite(t, b, "f00001.dat/inner.txt", "in a folder now")
	wantNames(t, b, "f00001.dat", "inner.txt")
	if got := fakeRead(t, b, "f00001.dat/inner.txt"); got != "in a folder now" {
		t.Errorf("f00001.dat/inner.txt holds %q", got)
	}

	// A generated folder removed and written to again holds the new file.
	if err := b.RemoveAll(ctx, "d00000"); err != nil {
		t.Fatal(err)
	}
	fakeWrite(t, b, "d00000/back.txt", "back")
	wantNames(t, b, "d00000", "back.txt")

	// No copy over a folder, and none under a file.
	if err := b.CopyFrom(ctx, "d00001", strings.NewReader("x"), model.PutParams{}); err == nil {
		t.Error("a copy over the folder d00001 succeeded")
	}
	if e, err := b.Stat(ctx, "d00001"); err != nil || !e.IsDir {
		t.Errorf("d00001 after a copy over it: %+v, %v; want the folder", e, err)
	}
	if err := b.CopyFrom(ctx, "f00002.dat/x.txt", strings.NewReader("x"), model.PutParams{}); err == nil {
		t.Error("a copy under the file f00002.dat succeeded")
	}
}

// A path that is not there is an error for every call, as on a server. The
// fake generated a full folder for any path: a mistyped one listed as a
// folder full of files, which could be read and summed.
func TestFakeMissingPathsFail(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny") // 3 folders and 10 files in each, 2 levels of folders
	for _, rel := range []string{"nope", "d00000/nope", "d00003", "d00000/d00000/d00000", "nope/d00000", "d0", "d000000"} {
		if _, err := b.List(ctx, rel); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("List %q: %v, want not there", rel, err)
		}
	}
	if _, err := b.List(ctx, "f00000.dat"); err == nil {
		t.Error("the file f00000.dat listed as a folder")
	}
	for _, rel := range []string{"nope/f00000.dat", "f00010.dat", "d00000/f0000.dat", "d00000/d00000/d00000/f00000.dat", "f00000.dat/f00000.dat"} {
		if _, err := b.Stat(ctx, rel); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat %q: %v, want not there", rel, err)
		}
		if _, err := b.Open(ctx, rel); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Open %q: %v, want not there", rel, err)
		}
		if _, err := b.Checksum(ctx, rel, "xxh3"); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Checksum %q: %v, want not there", rel, err)
		}
		if err := b.SetTimes(ctx, rel, time.Now(), time.Time{}, time.Time{}); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("SetTimes %q: %v, want not there", rel, err)
		}
		if err := b.Remove(ctx, rel); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Remove %q: %v, want not there", rel, err)
		}
		if err := b.Rename(ctx, rel, "moved.dat"); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Rename %q: %v, want not there", rel, err)
		}
	}
	for _, rel := range []string{"d00002", "d00000/d00001", "f00009.dat", "d00001/d00002/f00009.dat"} {
		if _, err := b.Stat(ctx, rel); err != nil {
			t.Errorf("Stat %q: %v", rel, err)
		}
	}
}

// A folder Mkdir makes, or makes again where one was removed, starts empty:
// what was written in the removed one does not come back. The fake filled
// each with generated files and folders, and one made again where one was
// removed stayed removed.
func TestFakeFoldersAWriteMadeStartEmpty(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")
	if err := b.Mkdir(ctx, "made/a/b", 0o755); err != nil {
		t.Fatal(err)
	}
	wantNames(t, b, "made", "a/")
	wantNames(t, b, "made/a", "b/")
	wantNames(t, b, "made/a/b")

	fakeWrite(t, b, "d00001/w.txt", "written")
	fakeWrite(t, b, "d00001/d00000/deep.txt", "written")
	if err := b.Remove(ctx, "d00001/f00001.dat"); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveAll(ctx, "d00001"); err != nil {
		t.Fatal(err)
	}
	if err := b.Mkdir(ctx, "d00001/d00000", 0o755); err != nil {
		t.Fatal(err)
	}
	wantNames(t, b, "d00001", "d00000/")
	wantNames(t, b, "d00001/d00000")
	if _, err := b.Stat(ctx, "d00001/f00000.dat"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat d00001/f00000.dat in the folder made again: %v, want not there", err)
	}

	// A folder that is there stays as it is.
	before := fakeNames(t, b, "d00002")
	if err := b.Mkdir(ctx, "d00002", 0o755); err != nil {
		t.Fatal(err)
	}
	wantNames(t, b, "d00002", before...)
	if err := b.Mkdir(ctx, "f00000.dat/sub", 0o755); err == nil {
		t.Error("Mkdir under the file f00000.dat succeeded")
	}
}

// A renamed folder takes what is in it along, and a renamed file its bytes.
// The fake generated a new folder at the new name, with other files in it,
// and gave a renamed file the bytes of its new name.
func TestFakeRenameTakesWhatIsInside(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")
	listing := func(relDir string) []model.FileEntry {
		t.Helper()
		entries, err := b.List(ctx, relDir)
		if err != nil {
			t.Fatalf("List %q: %v", relDir, err)
		}
		return entries
	}
	fakeWrite(t, b, "d00000/w.txt", "written")
	if err := b.Remove(ctx, "d00000/f00002.dat"); err != nil {
		t.Fatal(err)
	}
	top, sub := listing("d00000"), listing("d00000/d00001")
	body := fakeRead(t, b, "d00000/d00001/f00004.dat")

	if err := b.Rename(ctx, "d00000", "moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, "d00000"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("List d00000 after the rename: %v, want not there", err)
	}
	for dir, want := range map[string][]model.FileEntry{"moved": top, "moved/d00001": sub} {
		got := listing(dir)
		if len(got) != len(want) {
			t.Errorf("%s lists %d entries, it listed %d before the rename", dir, len(got), len(want))
			continue
		}
		for i, e := range got {
			w := want[i]
			w.RelPath = joinRel(dir, w.Name)
			if e != w {
				t.Errorf("%s after the rename: %+v, want %+v", dir, e, w)
			}
		}
	}
	if got := fakeRead(t, b, "moved/d00001/f00004.dat"); got != body {
		t.Error("moved/d00001/f00004.dat holds other bytes after its folder's rename")
	}
	if got := fakeRead(t, b, "moved/w.txt"); got != "written" {
		t.Errorf("moved/w.txt holds %q", got)
	}

	sum, err := b.Checksum(ctx, "f00003.dat", "sha256")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Rename(ctx, "f00003.dat", "renamed.dat"); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Checksum(ctx, "renamed.dat", "sha256"); err != nil || got != sum {
		t.Errorf("renamed.dat sums to %s, %v; before the rename %s", got, err, sum)
	}

	// Never onto a name in use, into itself, or into a folder not there.
	if err := b.Rename(ctx, "f00004.dat", "f00005.dat"); !errors.Is(err, fs.ErrExist) {
		t.Errorf("a rename onto f00005.dat: %v, want a name in use", err)
	}
	if err := b.Rename(ctx, "moved", "moved/inner"); err == nil {
		t.Error("a folder was moved into itself")
	}
	if err := b.Rename(ctx, "f00004.dat", "nope/f.dat"); err == nil {
		t.Error("a file was moved into a folder that is not there")
	}

	// A folder made again at the old name starts empty.
	if err := b.Mkdir(ctx, "d00000", 0o755); err != nil {
		t.Fatal(err)
	}
	wantNames(t, b, "d00000")
}

// Remove leaves a folder that holds something, as os.Remove does, and
// nothing removes the base. The fake removed a full folder with Remove, and
// said the base was removed.
func TestFakeRemoveKeepsAFolderThatHoldsSomething(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")
	if err := b.Remove(ctx, "d00000"); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Remove of the full folder d00000: %v, want not empty", err)
	}
	if _, err := b.Stat(ctx, "d00000/f00000.dat"); err != nil {
		t.Errorf("d00000/f00000.dat after Remove d00000: %v", err)
	}

	// Emptied, it goes.
	leaf := "d00001/d00002"
	for _, name := range fakeNames(t, b, leaf) {
		if err := b.Remove(ctx, leaf+"/"+name); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Remove(ctx, leaf); err != nil {
		t.Errorf("Remove of the emptied folder %s: %v", leaf, err)
	}
	if _, err := b.Stat(ctx, leaf); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat %s after Remove: %v, want not there", leaf, err)
	}
	// A folder renamed to its name lists all it holds: nothing of the
	// removed one is left over.
	moving := fakeNames(t, b, "d00001/d00001")
	if err := b.Rename(ctx, "d00001/d00001", leaf); err != nil {
		t.Fatal(err)
	}
	wantNames(t, b, leaf, moving...)

	// A folder a write made, with a file in it.
	fakeWrite(t, b, "made/x.txt", "x")
	if err := b.Remove(ctx, "made"); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Remove of made/, which holds x.txt: %v, want not empty", err)
	}

	if err := b.Remove(ctx, ""); err == nil {
		t.Error("Remove of the base succeeded")
	}
	if err := newFake(t, "fake://e?dirs=0&files=0").Remove(ctx, ""); err == nil {
		t.Error("Remove of an empty base succeeded")
	}
	if err := b.RemoveAll(ctx, ""); err == nil {
		t.Error("RemoveAll of the base succeeded")
	}
	if len(fakeNames(t, b, "")) == 0 {
		t.Error("the base is empty")
	}
	if err := b.RemoveAll(ctx, "nope/deeper"); err != nil {
		t.Errorf("RemoveAll of a path not there: %v, want none, as os.RemoveAll", err)
	}
}

// Stat, Open and Checksum find a file one step at a time from the base. The
// fake made the whole listing of the file's folder for each call, which is
// O(n²) over a wide folder.
func TestFakeFindsAFileWithoutListingItsFolder(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://x?dirs=0&files=100000&depth=1&size=64")
	const rel = "f54321.dat"
	for name, call := range map[string]func(){
		"Stat": func() { b.Stat(ctx, rel) },
		"Open": func() {
			if rd, err := b.Open(ctx, rel); err == nil {
				rd.Close()
			}
		},
		"Checksum": func() { b.Checksum(ctx, rel, "xxh3") },
	} {
		if n := testing.AllocsPerRun(3, call); n > 50 {
			t.Errorf("%s of one file in a folder of 100000 made %.0f allocations, want a few", name, n)
		}
	}
}

// SetTimes sets the times it is given; a zero time leaves that time as it
// is, and a zero atime is mtime, as on a local disk. The base has no times
// to keep: setting them is no error.
func TestFakeSetTimesAsALocalDiskDoes(t *testing.T) {
	ctx := context.Background()
	b := newFake(t, "fake://tiny")
	was, err := b.Stat(ctx, "f00000.dat")
	if err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2025, 5, 5, 5, 5, 5, 0, time.UTC)
	if err := b.SetTimes(ctx, "f00000.dat", mtime, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	e, err := b.Stat(ctx, "f00000.dat")
	if err != nil {
		t.Fatal(err)
	}
	if !e.ModTime.Equal(mtime) || e.ATime != model.NanoOf(mtime) || e.BirthTime != was.BirthTime {
		t.Errorf("after SetTimes(mtime): %+v; want mtime and atime %v, birth time %v as before", e, mtime, was.BirthTime)
	}
	if names := fakeNames(t, b, ""); len(slices.DeleteFunc(names, func(n string) bool { return n != "f00000.dat" })) != 1 {
		t.Errorf("the base lists f00000.dat %d times, want once", len(names))
	}
	if err := b.SetTimes(ctx, "", mtime, mtime, mtime); err != nil {
		t.Errorf("SetTimes of the base: %v", err)
	}
}

// A server sums its own files: no bytes cross the network, so the bandwidth
// limit does not slow a checksum. The fake charged every byte it summed to
// the limit for downloads.
func TestFakeChecksumIsNotChargedToTheBandwidthLimit(t *testing.T) {
	in := BandwidthIn()
	SetBandwidthIn(1024)
	t.Cleanup(func() { SetBandwidthIn(in) })
	b := newFake(t, "fake://x?dirs=0&files=1&depth=1&size=2000000")
	e, err := b.Stat(context.Background(), "f00000.dat")
	if err != nil || e.Size < 100_000 {
		t.Fatalf("f00000.dat: %+v, %v; want a file of 100 KB or more, which takes minutes at 1 KiB/s", e, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.Checksum(ctx, "f00000.dat", "xxh3"); err != nil {
		t.Errorf("a checksum of %d bytes with downloads limited to 1 KiB/s: %v", e.Size, err)
	}
}
