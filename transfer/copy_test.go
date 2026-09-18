package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"sc/model"
)

// listStub serves a fixed directory map and fails List for any path in fail.
type listStub struct {
	dirs map[string][]model.FileEntry
	fail map[string]bool
}

func (b *listStub) BasePath() string { return "/stub" }

func (b *listStub) List(_ context.Context, relDir string) ([]model.FileEntry, error) {
	if b.fail[relDir] {
		return nil, errors.New("connection reset")
	}
	return b.dirs[relDir], nil
}

func (b *listStub) Checksum(context.Context, string) (string, error) { return "", nil }
func (b *listStub) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}
func (b *listStub) CopyFrom(context.Context, string, io.Reader, os.FileMode) error { return nil }
func (b *listStub) Mkdir(context.Context, string, os.FileMode) error               { return nil }
func (b *listStub) Rename(context.Context, string, string) error                   { return nil }
func (b *listStub) Remove(context.Context, string) error                           { return nil }
func (b *listStub) RemoveAll(context.Context, string) error                        { return nil }
func (b *listStub) Open(context.Context, string) (io.ReadCloser, error)            { return nil, nil }

// A copy whose subtree cannot be listed is abandoned; that must show up as a
// failure in the progress counters, not only as a log line.
func TestCopyAbortOnListFailureCountsAsFailed(t *testing.T) {
	src := &listStub{
		dirs: map[string][]model.FileEntry{"": {{RelPath: "d", Name: "d", IsDir: true}}},
		fail: map[string]bool{"d": true},
	}
	dst := &listStub{dirs: map[string][]model.FileEntry{}}
	scanner := model.NewScanner(src, dst, 1, 1, false)
	scanner.Scan(context.Background(), model.CompareOpts{})
	var node *model.TreeNode
	for _, c := range scanner.Tree().Children {
		if c.Name == "d" {
			node = c
		}
	}
	if node == nil {
		t.Fatal("d not in tree")
	}
	p := &Progress{}
	res := Copy(context.Background(), Request{Src: src, Dst: dst, Scanner: scanner, Node: node, RelPath: "d", LeftToRight: true, Progress: p})
	if res.RescanRoot != nil {
		t.Error("aborted copy still asked for a rescan")
	}
	if got := p.Failed.Load(); got != 1 {
		t.Errorf("Failed = %d, want 1", got)
	}
}
