package transport

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

func TestParseRate(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		bad  bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1024", 1024, false},
		{"512k", 512 << 10, false},
		{"512K", 512 << 10, false},
		{"4M", 4 << 20, false},
		{"1.5M", 3 << 19, false},
		{"2g", 2 << 30, false},
		{"1MB", 1 << 20, false},
		{"-1", 0, true},
		{"fast", 0, true},
	}
	for _, c := range cases {
		got, err := ParseRate(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("ParseRate(%q) = %d, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRate(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseRate(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestFormatRate(t *testing.T) {
	cases := map[int64]string{0: "off", 900: "900", 512 << 10: "512K", 1 << 20: "1M", 3 << 19: "1.5M", 2 << 30: "2G"}
	for in, want := range cases {
		if got := FormatLimit(in); got != want {
			t.Errorf("FormatLimit(%d) = %q, want %q", in, got, want)
		}
	}
}

// Unlimited must be a straight pass-through: no chunking, no sleeping.
func TestLimiterUnlimited(t *testing.T) {
	var l Limiter
	if c := l.chunk(); c != 0 {
		t.Fatalf("chunk with no rate = %d, want 0", c)
	}
	start := time.Now()
	l.take(1 << 30)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("unlimited take slept %v", d)
	}
}

// A limited reader must deliver everything and take roughly size/rate seconds.
func TestLimitReaderThrottles(t *testing.T) {
	const rate = 256 << 10
	SetBandwidthIn(rate)
	defer SetBandwidthIn(0)

	src := bytes.Repeat([]byte("x"), 512<<10) // 2x the rate => ~1s after burst
	start := time.Now()
	got, err := io.ReadAll(LimitReader(bytes.NewReader(src)))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("read %d bytes, want %d", len(got), len(src))
	}
	// One second of burst is free, so only the second 256K is paid for.
	if elapsed < 700*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("512K at 256K/s took %v, want ~1s", elapsed)
	}
}

// The write side shares the budget the same way, and must not short-write.
func TestLimitWriterThrottles(t *testing.T) {
	const rate = 256 << 10
	SetBandwidthOut(rate)
	defer SetBandwidthOut(0)

	src := bytes.Repeat([]byte("y"), 512<<10)
	var dst bytes.Buffer
	start := time.Now()
	n, err := LimitWriter(&dst).Write(src)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(src) || dst.Len() != len(src) {
		t.Fatalf("wrote n=%d buf=%d, want %d", n, dst.Len(), len(src))
	}
	if elapsed < 700*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("512K at 256K/s took %v, want ~1s", elapsed)
	}
}

// Concurrent streams share one budget rather than each getting the full rate.
func TestLimiterSharedAcrossStreams(t *testing.T) {
	const rate = 256 << 10
	SetBandwidthIn(rate)
	defer SetBandwidthIn(0)

	var wg sync.WaitGroup
	start := time.Now()
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			io.Copy(io.Discard, LimitReader(bytes.NewReader(make([]byte, 256<<10))))
		}()
	}
	wg.Wait()
	// 1M total, 256K free burst, 768K left at 256K/s => ~3s.
	if elapsed := time.Since(start); elapsed < 2*time.Second {
		t.Fatalf("4x256K at 256K/s took %v, want ~3s", elapsed)
	}
}

// Raising the limit mid-stream must take effect without reopening the stream.
func TestLimiterRateChangeTakesEffect(t *testing.T) {
	SetBandwidthIn(32 << 10)
	defer SetBandwidthIn(0)

	r := LimitReader(bytes.NewReader(make([]byte, 4<<20)))
	buf := make([]byte, 1<<20)
	if _, err := io.ReadFull(r, buf[:4<<10]); err != nil {
		t.Fatal(err)
	}
	SetBandwidthIn(0)
	start := time.Now()
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("read after clearing the limit took %v", d)
	}
}
