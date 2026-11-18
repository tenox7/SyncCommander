package transport

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
)

// overdrawn sets both budgets to 4 KiB/s and spends a gigabyte of each, so
// that any byte charged now waits days: a call that is charged runs into
// the deadline of the context it returns, and one that is not finishes at
// once. Spending does not wait: a take with a done context charges and
// returns.
func overdrawn(t *testing.T) func() context.Context {
	t.Helper()
	SetBandwidthIn(4 << 10)
	SetBandwidthOut(4 << 10)
	t.Cleanup(func() { SetBandwidthIn(0); SetBandwidthOut(0) })
	done, cancel := context.WithCancel(context.Background())
	cancel()
	bwIn.take(done, 1<<30)
	bwOut.take(done, 1<<30)
	return func() context.Context {
		ctx, stop := context.WithTimeout(context.Background(), 500*time.Millisecond)
		t.Cleanup(stop)
		return ctx
	}
}

// L23: a local disk uses no network, but its reads, writes and sums were
// charged to the bandwidth limits: -bwlimit 512k summed 1 TB of local
// files in about three weeks, and a download to local disk paid the
// outbound limit as well as the inbound one.
func TestLocalDiskIsNotChargedToTheLimits(t *testing.T) {
	dir := t.TempDir()
	body := bytes.Repeat([]byte("sc"), 32<<10)
	if err := os.WriteFile(filepath.Join(dir, "a"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	b := NewLocalBackend(dir)
	ctx := overdrawn(t)
	if _, err := b.Checksum(ctx(), "a", "sha256"); err != nil {
		t.Errorf("a sum of a local file: %v", err)
	}
	if _, err := b.ChecksumRange(ctx(), "a", "md5", 100, 1000); err != nil {
		t.Errorf("a sum of part of a local file: %v", err)
	}
	if rc, err := b.OpenAt(ctx(), "a", 10); err != nil {
		t.Errorf("open: %v", err)
	} else {
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || !bytes.Equal(got, body[10:]) {
			t.Errorf("a read of a local file: %d bytes, %v", len(got), err)
		}
	}
	if err := b.CopyFrom(ctx(), "b", bytes.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err != nil {
		t.Errorf("a write of a local file: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "b")); !bytes.Equal(got, body) {
		t.Errorf("the written file holds %d bytes, want %d", len(got), len(body))
	}
}

// L23: what stands for a server is still charged: the streams of an
// rclone remote, which dials inside rclone, and of a fake:// tree.
func TestRemoteStreamsAreStillCharged(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), bytes.Repeat([]byte("r"), 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	rc, err := NewRcloneBackend(context.Background(), "rclone://"+dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rc.Close() })
	fake := newFake(t, "fake://x?dirs=0&files=1&depth=1&size=2000000")
	ctx := overdrawn(t)
	for _, c := range []struct {
		name string
		b    model.Backend
		file string
	}{{"rclone", rc, "a"}, {"fake", fake, "f00000.dat"}} {
		r, err := c.b.Open(ctx(), c.file)
		if err != nil {
			t.Fatalf("%s: open: %v", c.name, err)
		}
		_, err = io.ReadAll(r)
		r.Close()
		if err == nil {
			t.Errorf("%s: a download was not charged to the inbound limit", c.name)
		}
		body := bytes.Repeat([]byte("w"), 64<<10)
		if err := c.b.CopyFrom(ctx(), "up", bytes.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err == nil {
			t.Errorf("%s: an upload was not charged to the outbound limit", c.name)
		}
	}
}
