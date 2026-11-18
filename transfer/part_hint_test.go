package transfer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sc/model"
	"sc/transport"
)

// hintSource is a local source that records, for each checksum call, what
// the call says the source hashes for it (model.HashBytes).
type hintSource struct {
	*transport.LocalBackend
	mu    sync.Mutex
	hints map[string][]int64
}

func (b *hintSource) note(call string, ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hints[call] = append(b.hints[call], model.HashBytes(ctx))
}

func (b *hintSource) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	b.note("sum "+relPath, ctx)
	return b.LocalBackend.Checksum(ctx, relPath, algo)
}

func (b *hintSource) ChecksumRange(ctx context.Context, relPath, algo string, off, n int64) (string, error) {
	b.note(fmt.Sprintf("range %s %d", relPath, n), ctx)
	return b.LocalBackend.ChecksumRange(ctx, relPath, algo, off, n)
}

// R17: an rsync daemon source hashes a file for its checksum and says
// nothing while it does. The checks of a part run outside the copy's
// stall guard and said nothing of that, so a daemon run could not allow
// the time. Now each says what it hashes: the part's bytes before a
// resume, the whole file for --verify-resume.
func TestPartChecksSayWhatTheyHash(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	body := strings.Repeat("0123456789", 100)
	writeFile(t, filepath.Join(left, "half.txt"), body)
	writeFile(t, filepath.Join(left, "whole.txt"), body)
	writeFile(t, filepath.Join(right, model.PartPath("half.txt")), body[:400])
	writeFile(t, filepath.Join(right, model.PartPath("whole.txt")), body)
	src := &hintSource{LocalBackend: transport.NewLocalBackend(left), hints: map[string][]int64{}}
	s := scanned(t, left, right, src, transport.NewLocalBackend(right))
	s.verifyResume = true
	for _, f := range []string{"half.txt", "whole.txt"} {
		if p := s.copy(t, f, true, false); p.Failed.Load() != 0 {
			t.Errorf("%s: copy failed", f)
		}
		if got := readFile(t, filepath.Join(right, f)); got != body {
			t.Errorf("%s holds %q", f, got)
		}
	}
	for call, want := range map[string]int64{
		"range half.txt 400": 400,  // the part, before the resume
		"sum half.txt":       1000, // --verify-resume, after it
		"sum whole.txt":      1000, // a part as long as the source
	} {
		got := src.hints[call]
		if len(got) == 0 {
			t.Errorf("no call %q; calls: %v", call, src.hints)
		}
		for _, n := range got {
			if n != want {
				t.Errorf("%q says it hashes %d bytes, want %d", call, n, want)
			}
		}
	}
}
