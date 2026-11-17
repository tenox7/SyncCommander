package model

import (
	"context"
	"testing"
	"time"
)

// hungBackend is a stubBackend whose List of hang does not stop with its
// context: it waits until release is closed, as a hung network mount does.
type hungBackend struct {
	*stubBackend
	hang    string
	arrived chan struct{} // closed when the List of hang starts to wait
	release chan struct{}
}

func (b *hungBackend) List(ctx context.Context, relDir string) ([]FileEntry, error) {
	if relDir == b.hang {
		close(b.arrived)
		<-b.release
	}
	return b.stubBackend.List(ctx, relDir)
}

// F25: a folder on one side only is listed on that side alone. That
// listing was waited for without ctx, so a backend that hangs there kept
// the scan going after a stop (x) until the backend came back. Both sides
// listed, the wait already ended with ctx.
func TestScanStopsWhileAOneSidedListingHangs(t *testing.T) {
	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			hung := &hungBackend{stubBackend: &stubBackend{dirs: map[string][]FileEntry{
				"":     {dirEntry("only", "only")},
				"only": {fileEntry("only/a.txt", "a.txt", 1)},
			}}, hang: "only", arrived: make(chan struct{}), release: make(chan struct{})}
			defer close(hung.release) // lets a scan that did not stop end
			var left, right Backend = hung, &stubBackend{dirs: map[string][]FileEntry{}}
			if side == "right" {
				left, right = right, left
			}
			s := NewScanner(left, right, 1, 1, true)
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.Scan(context.Background(), CompareOpts{})
			}()
			select {
			case <-hung.arrived:
			case <-done:
				t.Fatal("the scan ended without listing only")
			case <-time.After(30 * time.Second):
				t.Fatal("the scan has not listed only after 30s")
			}
			s.Cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("the scan did not stop: it still waits for the %s listing that hangs", side)
			}
		})
	}
}
