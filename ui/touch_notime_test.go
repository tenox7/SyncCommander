package ui

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// timelessSide is a side whose listing gives its files no time, as an FTP
// or HTTP server may, and that counts the SetTimes it is asked for.
type timelessSide struct {
	model.Backend
	set *atomic.Int32
}

func (b timelessSide) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	ents, err := b.Backend.List(ctx, relDir)
	for i := range ents {
		ents[i].ModTime = time.Time{}
	}
	return ents, err
}

func (b timelessSide) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	e, err := b.Backend.Stat(ctx, relPath)
	e.ModTime = time.Time{}
	return e, err
}

func (b timelessSide) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	b.set.Add(1)
	return b.Backend.SetTimes(ctx, relPath, mtime, atime, btime)
}

// L25: touch (t) gives the older side the newer side's time. With no time
// on either side it gave a zero one, which FTP wrote as year 1. It sets
// none now, and the log says why.
func TestTouchWithNoTimeOnEitherSide(t *testing.T) {
	var set atomic.Int32
	left := timelessSide{openBackendOrSkip(t, "fake://tiny"), &set}
	right := timelessSide{openBackendOrSkip(t, "fake://tiny"), &set}
	m := newTestModel(t, left, right)
	m.scanner.Scan(context.Background(), model.CompareOpts{})
	node := firstBothFile(m.scanner.Tree())
	if node == nil {
		t.Fatal("no file present on both sides")
	}
	mark := len(transport.Log.Lines())
	m.Update(runFirst(m.touchNode(node)))
	if n := set.Load(); n != 0 {
		t.Errorf("SetTimes was asked for %d times with no time on either side", n)
	}
	if !slices.ContainsFunc(transport.Log.Lines()[mark:], func(l string) bool {
		return strings.Contains(l, "neither side has a time to give the other")
	}) {
		t.Errorf("the log does not say why nothing was set: %q", transport.Log.Lines()[mark:])
	}
}
