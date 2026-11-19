// Package transfer copies files and subtrees between two backends: it
// enumerates the compared tree, resumes partial uploads, prefers direct
// path-to-path transfers, runs files in parallel and reports progress through
// atomics the UI polls.
package transfer

import (
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
	TimesNotSet        atomic.Int64 // copied, but not their times: no failure, a mirror still runs (F17)
	ModesNotSet        atomic.Int64 // copied, but not their modes: no failure either (S20)
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
	FileStartBytes     atomic.Int64
	LeftToRight        atomic.Bool
	Sem                atomic.Pointer[DynSem]

	parallelMu sync.Mutex // orders SetParallel against runParallel publishing Sem
	slotsMu    sync.RWMutex
	slots      []*Slot
}

// SetParallel is + and - mid-copy: from now on up to n files are in flight.
// Before the workers start (while the subtree is listed) there is no Sem yet;
// runParallel picks the value up when it publishes one.
func (p *Progress) SetParallel(n int) {
	p.parallelMu.Lock()
	defer p.parallelMu.Unlock()
	p.Parallel.Store(int64(n))
	if s := p.Sem.Load(); s != nil {
		s.Resize(n)
	}
}

// Slot tracks one in-flight file for the multi-slot popup. Each parallel
// worker claims a slot at start and releases it at the end; the transport
// writes byte deltas into Bytes via the context counter. Slots are added as
// workers need them, since + can raise the worker count mid-copy. A free
// slot keeps the file that ended in it until the next file claims it.
type Slot struct {
	File      atomic.Value
	Size      atomic.Int64
	Bytes     atomic.Int64
	BaseBytes atomic.Int64
	Start     atomic.Int64
	End       atomic.Int64 // when its file ended, UnixNano; 0 while one is in flight
	Active    atomic.Bool
}

// SlotSnapshot is one slot as the popup draws it. A free slot (not Active)
// holds the file that ended in it, or none.
type SlotSnapshot struct {
	File      string
	Size      int64
	Bytes     int64
	BaseBytes int64
	Start     int64
	End       int64
	Active    bool
}

// Reset rearms every counter for a new transfer, so the popup never shows the
// previous copy's numbers while the subtree is still being listed.
func (p *Progress) Reset(parallel int, leftToRight bool) {
	for _, c := range []*atomic.Int64{&p.Total, &p.TotalBytes, &p.Done, &p.Failed, &p.TimesNotSet, &p.ModesNotSet, &p.InFlight, &p.Bytes, &p.BaseBytes,
		&p.CompletedBytes, &p.CompletedBaseBytes, &p.FileSize, &p.FileStartBytes} {
		c.Store(0)
	}
	p.Parallel.Store(int64(parallel))
	p.Batched.Store(false)
	p.Listing.Store(false)
	p.LeftToRight.Store(leftToRight)
	p.File.Store("")
	p.Start.Store(time.Now().UnixNano())
	p.resetSlots(parallel)
}

func (p *Progress) resetSlots(n int) {
	p.slotsMu.Lock()
	defer p.slotsMu.Unlock()
	p.slots = make([]*Slot, max(n, 0))
	for i := range p.slots {
		p.slots[i] = &Slot{}
	}
}

// claimSlot hands the worker the first free slot for file, adding one when
// every slot is busy. It runs once per file, so the write lock costs
// nothing. The slot takes file, size and the start under the lock the
// snapshot reads with, so no snapshot holds a slot in flight with no file.
func (p *Progress) claimSlot(file string, size int64) *Slot {
	p.slotsMu.Lock()
	defer p.slotsMu.Unlock()
	var s *Slot
	for _, free := range p.slots {
		if !free.Active.Load() {
			s = free
			break
		}
	}
	if s == nil {
		s = &Slot{}
		p.slots = append(p.slots, s)
	}
	s.File.Store(file)
	s.Size.Store(size)
	s.Bytes.Store(0)
	s.BaseBytes.Store(0)
	s.Start.Store(time.Now().UnixNano())
	s.End.Store(0)
	s.Active.Store(true)
	return s
}

// releaseSlot frees s. It keeps its file and counts until the next claim:
// SyncTotals counts the slots in flight only.
func (p *Progress) releaseSlot(s *Slot) {
	p.CompletedBytes.Add(s.Bytes.Load())
	p.CompletedBaseBytes.Add(s.BaseBytes.Load())
	s.End.Store(time.Now().UnixNano())
	s.Active.Store(false)
}

// SnapshotSlots copies every slot, in its place, the free ones too. The
// popup gave a row to the slots in flight only: a file that ended a moment
// before the next one claimed its slot took its row with it for a tick,
// and the popup jumped by a row.
func (p *Progress) SnapshotSlots() []SlotSnapshot {
	p.slotsMu.RLock()
	defer p.slotsMu.RUnlock()
	out := make([]SlotSnapshot, 0, len(p.slots))
	for _, s := range p.slots {
		file, _ := s.File.Load().(string)
		out = append(out, SlotSnapshot{
			File:      file,
			Size:      s.Size.Load(),
			Bytes:     s.Bytes.Load(),
			BaseBytes: s.BaseBytes.Load(),
			Start:     s.Start.Load(),
			End:       s.End.Load(),
			Active:    s.Active.Load(),
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
}
