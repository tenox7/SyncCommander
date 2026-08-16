package ui

import (
	"runtime/metrics"
	"sync"
	"time"
)

// MemUsage is a heap snapshot for the status bar. It reads runtime/metrics
// rather than runtime.ReadMemStats: the latter stops the world, which is not
// something to do ten times a second on a multi-gigabyte tree.
type MemUsage struct {
	Live     int64 // heap objects still reachable
	Resident int64 // mapped by the runtime, minus what was handed back to the OS
	GCCycles uint64
}

var memSamples = []metrics.Sample{
	{Name: "/memory/classes/heap/objects:bytes"},
	{Name: "/memory/classes/total:bytes"},
	{Name: "/memory/classes/heap/released:bytes"},
	{Name: "/gc/cycles/total:gc-cycles"},
}

var (
	memMu     sync.Mutex
	memCache  MemUsage
	memSample time.Time
)

func readMemUsage() MemUsage {
	memMu.Lock()
	defer memMu.Unlock()
	if !memSample.IsZero() && time.Since(memSample) < 500*time.Millisecond {
		return memCache
	}
	metrics.Read(memSamples)
	val := func(i int) uint64 {
		if memSamples[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return memSamples[i].Value.Uint64()
	}
	memCache = MemUsage{
		Live:     int64(val(0)),
		Resident: int64(val(1) - val(2)),
		GCCycles: val(3),
	}
	memSample = time.Now()
	return memCache
}
