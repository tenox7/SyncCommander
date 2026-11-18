package transfer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"sc/model"
	"sc/transport"
)

func newFake(t *testing.T, url string) *transport.FakeBackend {
	t.Helper()
	b, err := transport.NewFakeBackend(url)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// L25: a destination on Windows took names it cannot hold as they are: a
// copy of a\b.txt wrote the file b.txt in a folder a, and nul.txt went to
// the device, both "OK". Such names are refused now, before a byte moves,
// with the reason, and so is a file in a folder with such a name; the
// rest is copied. On a folder that folds letter case, as Windows does by
// default, and on one that keeps it, as Windows can be made to.
func TestCopyRefusesNamesWindowsCannotHold(t *testing.T) {
	for _, rule := range []model.NameRule{
		{Case: model.Folded, Forms: model.Apart, Windows: true},
		{Case: model.Apart, Forms: model.Apart, Windows: true},
	} {
		t.Run(fmt.Sprintf("case folded=%v", rule.Case == model.Folded), func(t *testing.T) {
			copyRefusesWindowsNames(t, rule)
		})
	}
}

func copyRefusesWindowsNames(t *testing.T, rule model.NameRule) {
	ctx := context.Background()
	src := newFake(t, "fake://src?dirs=0&files=0")
	files := map[string]string{ // file -> the name its refusal names; "" for a file copied
		"d/ok.txt":     "",
		"d/con2.txt":   "",
		`d/a\b.txt`:    `a\b.txt`,
		"d/x:y.txt":    "x:y.txt",
		"d/nul.txt":    "nul.txt",
		"d/trail.":     "trail.",
		"d/dir./f.txt": "dir.",
		"d/aux/g.txt":  "aux",
	}
	for name := range files {
		if err := src.Mkdir(ctx, model.DirOf(name), 0o755); err != nil {
			t.Fatal(err)
		}
		body := "body of " + name
		if err := src.CopyFrom(ctx, name, strings.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	fake := newFake(t, "fake://dst?dirs=0&files=0")
	dst := namesAs{Backend: fake, rule: rule}
	s := scanned(t, "", "", src, dst)
	mark := len(transport.Log.Lines())
	p := s.copy(t, "d", true, false)
	refused := 0
	for name, why := range files {
		got, ok := holds(t, fake, name)
		if why == "" {
			if got != "body of "+name {
				t.Errorf("%s holds %q (there: %v), want its body", name, got, ok)
			}
			continue
		}
		refused++
		if ok {
			t.Errorf("%s was written, holding %q; it was to be refused", name, got)
		}
		if want := fmt.Sprintf("COPY %s: refused, the destination cannot hold the name %#q: ", name, why); !logged(mark, want) {
			t.Errorf("no log line %q", want)
		}
	}
	if got := p.Failed.Load(); got != int64(refused) {
		t.Errorf("failed=%d, want %d (one for each refused name)", got, refused)
	}
}

// noLocalPath is a local source with no local path for the file refused,
// as for a name the system cannot hold.
type noLocalPath struct {
	*transport.LocalBackend
	refused string
}

func (b noLocalPath) LocalPath(relPath string) string {
	if relPath == b.refused {
		return ""
	}
	return b.LocalBackend.LocalPath(relPath)
}

// pathSpy is a destination that takes files by their local path, as rsync
// does, and keeps the paths it is given; it takes none of them, so the
// copy goes on without it.
type pathSpy struct {
	*transport.FakeBackend
	mu    sync.Mutex
	paths []string
	trees int // SendLocalTree calls
}

func (s *pathSpy) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := s.FakeBackend.Capabilities(ctx)
	caps.SendLocal, caps.RecvLocal, caps.Batch = true, true, true
	return caps, err
}

func (s *pathSpy) given(paths ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paths = append(s.paths, paths...)
}

func (s *pathSpy) SendLocalFile(_ context.Context, srcPath, _ string, _ model.PutParams) error {
	s.given(srcPath)
	return errors.New("pathSpy sends nothing")
}

func (s *pathSpy) RecvToLocalFile(_ context.Context, _, dstPath string) error {
	s.given(dstPath)
	return errors.New("pathSpy receives nothing")
}

func (s *pathSpy) SendLocalTree(_ context.Context, srcRoot, _ string, files []string, _ func(string)) error {
	s.mu.Lock()
	s.trees++
	s.mu.Unlock()
	s.given(srcRoot)
	return errors.New("pathSpy sends nothing")
}

// L25: a name the system cannot hold has no local path, "". A copy by
// rsync took that for a path, and rsync for the folder sc runs in: a
// download of such a file went there, and an upload was sent from there.
// Now no such file goes by its local path: not alone, and not in a
// folder's one rsync run, which goes file by file instead.
func TestNoRsyncByLocalPathForANameWithNone(t *testing.T) {
	ctx := context.Background()
	entry := model.FileEntry{RelPath: "a.txt", Name: "a.txt", Size: 1}
	local := noLocalPath{transport.NewLocalBackend(t.TempDir()), "a.txt"}
	spy := &pathSpy{FakeBackend: newFake(t, "fake://spy?dirs=0&files=0")}
	if tryDirectTransfer(ctx, local, spy, "a.txt", &entry) {
		t.Error("an upload by local path of a file with none worked")
	}
	if tryDirectTransfer(ctx, spy, local, "a.txt", &entry) {
		t.Error("a download by local path of a file with none worked")
	}
	if len(spy.paths) != 0 {
		t.Errorf("rsync was given the local paths %q", spy.paths)
	}

	srcDir := t.TempDir()
	writeFile(t, filepath.Join(srcDir, "d", "a.txt"), "a")
	writeFile(t, filepath.Join(srcDir, "d", "b.txt"), "b")
	tree := &pathSpy{FakeBackend: newFake(t, "fake://tree?dirs=0&files=0")}
	s := scanned(t, "", "", noLocalPath{transport.NewLocalBackend(srcDir), "d/a.txt"}, tree)
	s.batch = true
	p := s.copy(t, "d", true, false)
	if tree.trees != 0 {
		t.Errorf("a folder holding a file with no local path went to rsync in one run (%d)", tree.trees)
	}
	if slices.Contains(tree.paths, "") {
		t.Errorf("rsync was given the local paths %q", tree.paths)
	}
	for name, body := range map[string]string{"d/a.txt": "a", "d/b.txt": "b"} {
		if got, ok := holds(t, tree.FakeBackend, name); got != body {
			t.Errorf("%s holds %q (there: %v), want %q", name, got, ok, body)
		}
	}
	if got := p.Failed.Load(); got != 0 {
		t.Errorf("failed=%d, want 0", got)
	}
}
