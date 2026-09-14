// Package transfer copies files and subtrees between two backends: it
// enumerates the compared tree, resumes partial uploads, prefers direct
// path-to-path transfers, runs files in parallel and reports progress through
// atomics the UI polls.
package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Progress is the shared state of one copy. Every field is written by the
// transfer goroutines and read by the UI without a lock.
type Progress struct {
	Total              atomic.Int64
	Done               atomic.Int64
	Failed             atomic.Int64
	InFlight           atomic.Int64
	Parallel           atomic.Int64
	Batched            atomic.Bool
	Listing            atomic.Bool // enumerating an unlisted subtree before the first file moves
	Bytes              atomic.Int64
	BaseBytes          atomic.Int64
	CompletedBytes     atomic.Int64
	CompletedBaseBytes atomic.Int64
	TotalBytes         atomic.Int64
	Start              atomic.Int64
	File               atomic.Value
	FileSize           atomic.Int64
	FileStart          atomic.Int64
	FileStartBytes     atomic.Int64
	FileStartBaseBytes atomic.Int64
	LeftToRight        atomic.Bool
	Cancel             atomic.Pointer[context.CancelFunc]
	Sem                atomic.Pointer[DynSem]

	slotsMu sync.RWMutex
	slots   []*Slot
}

// Slot tracks one in-flight file for the multi-slot popup. Each parallel
// worker claims a slot at start and releases it at the end; the transport
// writes byte deltas into Bytes via the context counter.
type Slot struct {
	File      atomic.Value
	Size      atomic.Int64
	Bytes     atomic.Int64
	BaseBytes atomic.Int64
	Start     atomic.Int64
	Active    atomic.Bool
}

type SlotSnapshot struct {
	File      string
	Size      int64
	Bytes     int64
	BaseBytes int64
	Start     int64
}

// Reset rearms every counter for a new transfer, so the popup never shows the
// previous copy's numbers while the subtree is still being listed.
func (p *Progress) Reset(parallel, slots int, leftToRight bool) {
	for _, c := range []*atomic.Int64{&p.Total, &p.TotalBytes, &p.Done, &p.Failed, &p.InFlight, &p.Bytes, &p.BaseBytes,
		&p.CompletedBytes, &p.CompletedBaseBytes, &p.FileSize, &p.FileStart, &p.FileStartBytes, &p.FileStartBaseBytes} {
		c.Store(0)
	}
	p.Parallel.Store(int64(parallel))
	p.Batched.Store(false)
	p.Listing.Store(false)
	p.LeftToRight.Store(leftToRight)
	p.File.Store("")
	p.Start.Store(time.Now().UnixNano())
	p.resetSlots(slots)
}

func (p *Progress) resetSlots(n int) {
	p.slotsMu.Lock()
	defer p.slotsMu.Unlock()
	p.slots = make([]*Slot, n)
	for i := range p.slots {
		p.slots[i] = &Slot{}
	}
}

func (p *Progress) claimSlot() *Slot {
	p.slotsMu.RLock()
	defer p.slotsMu.RUnlock()
	for _, s := range p.slots {
		if s.Active.CompareAndSwap(false, true) {
			s.File.Store("")
			s.Size.Store(0)
			s.Bytes.Store(0)
			s.BaseBytes.Store(0)
			s.Start.Store(0)
			return s
		}
	}
	return nil
}

func (p *Progress) releaseSlot(s *Slot) {
	if s == nil {
		return
	}
	p.CompletedBytes.Add(s.Bytes.Load())
	p.CompletedBaseBytes.Add(s.BaseBytes.Load())
	s.Active.Store(false)
}

func (p *Progress) SnapshotSlots() []SlotSnapshot {
	p.slotsMu.RLock()
	defer p.slotsMu.RUnlock()
	out := make([]SlotSnapshot, 0, len(p.slots))
	for _, s := range p.slots {
		if !s.Active.Load() {
			continue
		}
		file, _ := s.File.Load().(string)
		out = append(out, SlotSnapshot{
			File:      file,
			Size:      s.Size.Load(),
			Bytes:     s.Bytes.Load(),
			BaseBytes: s.BaseBytes.Load(),
			Start:     s.Start.Load(),
		})
	}
	return out
}

// SyncTotals refreshes Bytes/BaseBytes to the sum of completed bytes plus the
// in-flight slot counters. Per-slot writers update only their slot, so
// without this the status bar counter would freeze during parallel copies.
// Batch mode writes Bytes directly and has no slots to sum from.
func (p *Progress) SyncTotals() {
	if p.Batched.Load() {
		return
	}
	bytes := p.CompletedBytes.Load()
	base := p.CompletedBaseBytes.Load()
	p.slotsMu.RLock()
	for _, s := range p.slots {
		if !s.Active.Load() {
			continue
		}
		bytes += s.Bytes.Load()
		base += s.BaseBytes.Load()
	}
	p.slotsMu.RUnlock()
	p.Bytes.Store(bytes)
	p.BaseBytes.Store(base)
}

func (p *Progress) beginFile(size int64) {
	p.FileSize.Store(size)
	p.FileStartBytes.Store(p.Bytes.Load())
	p.FileStartBaseBytes.Store(p.BaseBytes.Load())
	p.FileStart.Store(time.Now().UnixNano())
}
