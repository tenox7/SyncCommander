package model_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// timeoutRecorder records the listing timeout each List gets, as a
// backend that waits for its own preload reads it (model.ListWait).
type timeoutRecorder struct {
	model.Backend
	mu       sync.Mutex
	timeouts map[string]time.Duration
}

func (b *timeoutRecorder) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	_, d, stop := model.ListWait(ctx)
	stop()
	b.mu.Lock()
	b.timeouts[relDir] = d
	b.mu.Unlock()
	return b.Backend.List(ctx, relDir)
}

// W-M2: the scan gives each listing its timeout as a listing's
// (model.WithListTimeout), so a backend that waits for its own preload
// first does not count the wait against it.
func TestTheScanGivesEachListingAListingTimeout(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(left, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	lb := &timeoutRecorder{Backend: transport.NewLocalBackend(left), timeouts: map[string]time.Duration{}}
	s := model.NewScanner(lb, transport.NewLocalBackend(right), 1, 1, true)
	s.Scan(context.Background(), model.CompareOpts{Size: true})
	for _, dir := range []string{"", "d"} {
		switch d, ok := lb.timeouts[dir]; {
		case !ok:
			t.Errorf("%q was not listed; listed: %v", dir, lb.timeouts)
		case d != model.ListTimeout:
			t.Errorf("the listing of %q has a listing timeout of %v, want %v", dir, d, model.ListTimeout)
		}
	}
}
