package model_test

// Perf harness for large trees, driven by the fake:// backend.
//
//	SC_PERF=fake://large go test ./model -run TestPerfScan -v -timeout 30m
//
// Reports the two costs that decide whether the UI stays responsive: the
// one-off scan, and the per-tick FlattenTree the UI runs at 10 Hz.

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

func TestPerfScan(t *testing.T) {
	target := os.Getenv("SC_PERF")
	if target == "" {
		t.Skip("set SC_PERF=fake://large (or any two paths via SC_PERF_LEFT/RIGHT) to run")
	}
	left, right := target, os.Getenv("SC_PERF_RIGHT")
	if right == "" {
		right = target
	}
	lb, err := transport.TryOpenBackend(left, false, 4)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := transport.TryOpenBackend(right, false, 4)
	if err != nil {
		t.Fatal(err)
	}

	opts := &model.CompareOpts{Size: true, ModTime: true, TimeGrace: true, IgnoreTZDST: true}
	scanner := model.NewScanner(lb, rb, 4, true)

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)

	t0 := time.Now()
	scanner.Scan(context.Background(), false, false, true, true)
	scanElapsed := time.Since(t0)

	runtime.ReadMemStats(&m1)
	tree := scanner.Tree()
	p := scanner.Progress()
	nodes := p.TotalFiles + p.TotalDirs

	t.Logf("scan:      %v for %d files + %d dirs (%.0f nodes/s)",
		scanElapsed.Round(time.Millisecond), p.TotalFiles, p.TotalDirs,
		float64(nodes)/scanElapsed.Seconds())
	t.Logf("heap:      %d MB in use, %d MB allocated during scan",
		m1.HeapInuse>>20, (m1.TotalAlloc-m0.TotalAlloc)>>20)

	timeFlatten := func(label string) {
		t1 := time.Now()
		flat := model.FlattenTree(tree, opts)
		d := time.Since(t1)
		t.Logf("%-10s %v for %d visible rows (%.1f ticks/s at 100ms budget)",
			label+":", d.Round(time.Microsecond), len(flat), 1/d.Seconds())
	}
	timeFlatten("collapsed")
	model.SetExpandedAll(tree, true)
	timeFlatten("expanded")
}
