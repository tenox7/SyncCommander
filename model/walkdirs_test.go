package model

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWalkDirsVisitsEveryJobOnce(t *testing.T) {
	const depth = 5
	var mu sync.Mutex
	seen := make(map[string]int)

	walkDirs(context.Background(), 8, []dirJob{{relDir: "r", depth: 0}}, func(j dirJob) []dirJob {
		mu.Lock()
		seen[j.relDir]++
		mu.Unlock()
		if j.depth >= depth {
			return nil
		}
		return []dirJob{
			{relDir: j.relDir + "/a", depth: j.depth + 1},
			{relDir: j.relDir + "/b", depth: j.depth + 1},
		}
	})

	want := 1<<(depth+1) - 1 // full binary tree
	if len(seen) != want {
		t.Fatalf("visited %d distinct jobs, want %d", len(seen), want)
	}
	for path, n := range seen {
		if n != 1 {
			t.Errorf("%s processed %d times, want 1", path, n)
		}
	}
}

// A single worker must still drain a queue that grows while it runs.
func TestWalkDirsSingleWorker(t *testing.T) {
	var n atomic.Int64
	walkDirs(context.Background(), 1, []dirJob{{relDir: "r", depth: 0}}, func(j dirJob) []dirJob {
		n.Add(1)
		if j.depth >= 3 {
			return nil
		}
		return []dirJob{{relDir: j.relDir + "/a", depth: j.depth + 1}}
	})
	if got := n.Load(); got != 4 {
		t.Errorf("processed %d jobs, want 4", got)
	}
}

// Cancellation must unpark idle workers, not just stop the queue from growing.
func TestWalkDirsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int64
	walkDirs(ctx, 4, []dirJob{{relDir: "r"}}, func(j dirJob) []dirJob {
		if n.Add(1) == 10 {
			cancel()
		}
		return []dirJob{{relDir: j.relDir + "/a"}, {relDir: j.relDir + "/b"}}
	})
	if got := n.Load(); got < 10 {
		t.Errorf("processed %d jobs before cancel, want at least 10", got)
	}
}
