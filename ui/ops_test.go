package ui

import (
	"context"
	"testing"

	"sc/transport"
)

// An error logged as a scan ends, such as a refused connection after the last
// retry, must open the log: the tick loop has already stopped by then.
func TestScanEndOpensLogOnNewError(t *testing.T) {
	src := openBackendOrSkip(t, "fake://tiny")
	m := newTestModel(t, src, src)
	// One error the dialog has not shown yet, without adding to the shared
	// log that other tests' models read.
	m.logView.lastSeenErrs = transport.Log.ErrCount() - 1
	_ = m.startOp(opScan, func(context.Context) {})
	m.Update(opDoneMsg{kind: opScan, gen: m.scan.gen})
	if !m.logView.IsOpen() {
		t.Fatal("scan ended with a new error but the log stayed closed")
	}
}

// A done message from a cancelled predecessor must not clear the flag that
// keeps the tick loop alive for the current operation.
func TestStaleOpDoneIsIgnored(t *testing.T) {
	src := openBackendOrSkip(t, "fake://tiny")
	m := newTestModel(t, src, src)
	if m.scanning() {
		t.Fatal("model scanning before any op started")
	}
	_ = m.startOp(opScan, func(context.Context) {})
	gen := m.scan.gen
	m.Update(opDoneMsg{kind: opScan, gen: gen - 1})
	if !m.scanning() {
		t.Fatal("stale done message cleared the running scan")
	}
	m.Update(opDoneMsg{kind: opChecksum, gen: gen})
	if !m.scanning() {
		t.Fatal("a checksum done message cleared the scan")
	}
	m.Update(opDoneMsg{kind: opScan, gen: gen})
	if m.scanning() {
		t.Fatal("matching done message did not clear the scan")
	}
}

// A rescan requested while a scan runs waits for it instead of racing it.
func TestRescanQueuedBehindRunningScan(t *testing.T) {
	src := openBackendOrSkip(t, "fake://tiny")
	m := newTestModel(t, src, src)
	_ = m.startOp(opScan, func(context.Context) {})
	if cmd := m.queueRescan(m.scanner.Tree(), nil); cmd != nil || m.pendingRescan == nil {
		t.Fatal("rescan started while a scan was running")
	}
	if cmd := m.finishOp(opDoneMsg{kind: opScan, gen: m.scan.gen}); cmd == nil || m.pendingRescan != nil {
		t.Fatal("pending rescan did not start when the scan finished")
	}
	m.cancelOps()
}
