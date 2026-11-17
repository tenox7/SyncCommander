package transfer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/internal/sshtest"
	"sc/model"
	"sc/transport"
)

// hostileBackend lists one more entry in dir than the folder holds, as a
// hostile server does, and serves that file when asked for it.
type hostileBackend struct {
	model.Backend
	dir   string
	entry model.FileEntry
	body  string
}

func (h *hostileBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	e, err := h.Backend.List(ctx, relDir)
	if err == nil && relDir == h.dir {
		e = append(slices.Clone(e), h.entry)
	}
	return e, err
}

func (h *hostileBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	if relPath == h.entry.RelPath {
		return h.entry, nil
	}
	return h.Backend.Stat(ctx, relPath)
}

func (h *hostileBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	if relPath == h.entry.RelPath {
		return io.NopCloser(strings.NewReader(h.body)), nil
	}
	return h.Backend.Open(ctx, relPath)
}

// L7, S6: a server lists the names it likes, and a tree path is where a
// copy writes and a mirror deletes. A source that lists "../../evil" had
// it written two folders above the destination's base; a destination that
// lists "../../victim" had a mirror delete the file of that name two
// folders above its base. The destination is sftp, which joined the name
// onto its base as it was; each base sits two folders deep in the test's
// own folder, which is all an old build could write to or delete in.
func TestScenarioHostileNamesStayInTheBase(t *testing.T) {
	addr := sshtest.Start(t, true)
	openSFTP := func(t *testing.T, dir string) model.Backend {
		b, err := transport.TryOpenBackend("sftp://x:pw@"+addr+filepath.ToSlash(dir), true, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { transport.CloseBackend(b) })
		return b
	}
	// A hostile entry lists the size and time of the file it names, as
	// the server that has it would: a mirror deletes only what is still
	// as the dialog showed it.
	mtime := time.Unix(1700000000, 0)
	hostile := func(name, body string) model.FileEntry {
		return model.FileEntry{RelPath: name, Name: name, Size: int64(len(body)), ModTime: mtime, Mode: 0o644}
	}
	const victim = "outside the base"
	setUp := func(t *testing.T) (root, left, right string) {
		root = t.TempDir()
		left, right = filepath.Join(root, "left"), filepath.Join(root, "a", "b", "right")
		writeFile(t, filepath.Join(left, "ok.txt"), "ok")
		writeFile(t, filepath.Join(root, "a", "victim"), victim)
		if err := os.Chtimes(filepath.Join(root, "a", "victim"), mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(right, 0o755); err != nil {
			t.Fatal(err)
		}
		return root, left, right
	}
	want := []string{"a/", "a/b/", "a/b/right/", "a/b/right/ok.txt", "a/victim", "left/", "left/ok.txt"}

	t.Run("a source name, copied", func(t *testing.T) {
		root, left, right := setUp(t)
		src := &hostileBackend{Backend: transport.NewLocalBackend(left), entry: hostile("../../evil", "evil"), body: "evil"}
		s := scanned(t, left, right, src, openSFTP(t, right))
		if p := s.copy(t, "", true, false); p.Failed.Load() != 0 {
			t.Errorf("the copy failed %d files", p.Failed.Load())
		}
		wantTree(t, "the test folder after the copy", root, want...)
	})

	t.Run("a destination name, mirrored", func(t *testing.T) {
		root, left, right := setUp(t)
		dst := &hostileBackend{Backend: openSFTP(t, right), entry: hostile("../../victim", victim)}
		s := scanned(t, left, right, transport.NewLocalBackend(left), dst)
		if p := s.copy(t, "", true, true); p.Failed.Load() != 0 {
			t.Errorf("the mirror failed %d files", p.Failed.Load())
		}
		wantTree(t, "the test folder after the mirror", root, want...)
	})
}
