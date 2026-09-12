package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The limit has to bite on a live socket, not just on a wrapped buffer, so
// these drive real servers. rsync:// is the interesting one: gorsync dials the
// daemon itself and only meters it through the Cmd.DialContext hook.
func TestBandwidthOverRsyncDaemon(t *testing.T) {
	port, moduleDir := startRsyncDaemon(t)
	body := bytes.Repeat([]byte("q"), 4<<20)
	if err := os.WriteFile(filepath.Join(moduleDir, "big.bin"), body, 0644); err != nil {
		t.Fatal(err)
	}
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}

	timed := func(label string) time.Duration {
		start := time.Now()
		rc, err := b.Open(context.Background(), "big.bin")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		d := time.Since(start)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s: n=%d err=%v", label, len(got), err)
		}
		t.Logf("%s: download 4MB in %v", label, d)
		return d
	}
	fast := timed("unlimited")

	SetBandwidthIn(1 << 20)
	defer SetBandwidthIn(0)
	if d := timed("1M/s"); d < 2500*time.Millisecond {
		t.Errorf("download NOT throttled: %v", d)
	}
	if fast > time.Second {
		t.Errorf("unlimited download took %v", fast)
	}

	// Upload: the daemon-receiver side must be paced too.
	SetBandwidthIn(0)
	SetBandwidthOut(1 << 20)
	defer SetBandwidthOut(0)
	start := time.Now()
	if err := b.CopyFrom(context.Background(), "up.bin", bytes.NewReader(body), 0644); err != nil {
		t.Fatal(err)
	}
	up := time.Since(start)
	t.Logf("1M/s: upload 4MB in %v", up)
	if up < 2500*time.Millisecond {
		t.Errorf("upload NOT throttled: %v", up)
	}
	got, _ := os.ReadFile(filepath.Join(moduleDir, "up.bin"))
	if !bytes.Equal(got, body) {
		t.Fatalf("corrupt upload: %d bytes", len(got))
	}
}

func TestBandwidthOverWebDAV(t *testing.T) {
	root := t.TempDir()
	body := bytes.Repeat([]byte("z"), 4<<20)
	if err := os.WriteFile(filepath.Join(root, "big.bin"), body, 0644); err != nil {
		t.Fatal(err)
	}
	port, _ := startRcloneWebDAV(t, root)
	b, err := NewWebDAVBackend(fmt.Sprintf("webdav://u:p@127.0.0.1:%d/", port), true, 4)
	if err != nil {
		t.Fatal(err)
	}

	SetBandwidthIn(1 << 20)
	defer SetBandwidthIn(0)
	start := time.Now()
	rc, err := b.Open(context.Background(), "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	down := time.Since(start)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("download: n=%d err=%v", len(got), err)
	}
	t.Logf("1M/s: download 4MB in %v", down)
	if down < 2500*time.Millisecond {
		t.Errorf("download NOT throttled: %v", down)
	}

	SetBandwidthIn(0)
	SetBandwidthOut(1 << 20)
	defer SetBandwidthOut(0)
	start = time.Now()
	if err := b.CopyFrom(context.Background(), "up.bin", bytes.NewReader(body), 0644); err != nil {
		t.Fatal(err)
	}
	up := time.Since(start)
	t.Logf("1M/s: upload 4MB in %v", up)
	if up < 2500*time.Millisecond {
		t.Errorf("upload NOT throttled: %v", up)
	}
}
