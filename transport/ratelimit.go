package transport

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Bandwidth limiting is applied at the lowest layer so every protocol inherits
// it without knowing about it: network backends throttle their net.Conn, local
// and synthetic backends throttle the file reader/writer. In counts bytes
// entering sc (downloads, local reads), Out bytes leaving it (uploads, local
// writes) — a copy therefore passes through both budgets.
var (
	bwIn  Limiter
	bwOut Limiter
)

const (
	bwMinChunk = 4 << 10
	bwMaxChunk = 256 << 10
	bwMaxSleep = 2 * time.Second
)

// Limiter is a token bucket over a byte budget with a one-second burst. A rate
// of 0 means unlimited and costs a single atomic load per call.
type Limiter struct {
	rate   atomic.Int64
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func (l *Limiter) Rate() int64 { return l.rate.Load() }

func (l *Limiter) SetRate(bytesPerSec int64) {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	l.mu.Lock()
	l.rate.Store(bytesPerSec)
	l.tokens = 0
	l.last = time.Time{}
	l.mu.Unlock()
}

// chunk caps how much one throttled read or write moves: about a tenth of a
// second of budget, so sleeps stay short and a rate changed mid-transfer takes
// effect within one chunk. 0 means unlimited.
func (l *Limiter) chunk() int {
	r := l.rate.Load()
	if r <= 0 {
		return 0
	}
	switch n := r / 10; {
	case n < bwMinChunk:
		return bwMinChunk
	case n > bwMaxChunk:
		return bwMaxChunk
	default:
		return int(n)
	}
}

// take charges n bytes and sleeps for as long as the bucket is overdrawn.
// Concurrent callers stagger naturally: each drives the pool further negative
// and so waits proportionally longer. A sleep clamped by bwMaxSleep leaves the
// remaining debt in the bucket for the next caller to pay, so the average rate
// holds either way.
func (l *Limiter) take(n int) {
	if n <= 0 || l.rate.Load() <= 0 {
		return
	}
	l.mu.Lock()
	r := l.rate.Load()
	if r <= 0 {
		l.mu.Unlock()
		return
	}
	now := time.Now()
	switch {
	case l.last.IsZero():
		l.tokens = float64(r)
	default:
		l.tokens += now.Sub(l.last).Seconds() * float64(r)
		if l.tokens > float64(r) {
			l.tokens = float64(r)
		}
	}
	l.last = now
	l.tokens -= float64(n)
	deficit := l.tokens
	l.mu.Unlock()

	if deficit >= 0 {
		return
	}
	d := time.Duration(-deficit / float64(r) * float64(time.Second))
	if d > bwMaxSleep {
		d = bwMaxSleep
	}
	time.Sleep(d)
}

func SetBandwidthIn(bytesPerSec int64)  { bwIn.SetRate(bytesPerSec) }
func SetBandwidthOut(bytesPerSec int64) { bwOut.SetRate(bytesPerSec) }
func BandwidthIn() int64                { return bwIn.Rate() }
func BandwidthOut() int64               { return bwOut.Rate() }

// ParseRate reads a bandwidth limit: a bare number is bytes per second, a
// k/m/g suffix (case-insensitive, optional trailing "b") multiplies by 1024.
// "" and "0" mean unlimited.
func ParseRate(in string) (int64, error) {
	s := strings.TrimSpace(in)
	if s == "" {
		return 0, nil
	}
	s = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "b")
	mult := int64(1)
	if s != "" {
		switch s[len(s)-1] {
		case 'k', 'K':
			mult = 1 << 10
		case 'm', 'M':
			mult = 1 << 20
		case 'g', 'G':
			mult = 1 << 30
		}
	}
	if mult > 1 {
		s = s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("bad bandwidth limit %q", in)
	}
	if v < 0 {
		return 0, fmt.Errorf("negative bandwidth limit %q", in)
	}
	return int64(v * float64(mult)), nil
}

// FormatRate renders a bytes-per-second limit compactly for display.
func FormatRate(bps int64) string {
	if bps <= 0 {
		return "off"
	}
	for _, u := range []struct {
		div  int64
		unit string
	}{{1 << 30, "G"}, {1 << 20, "M"}, {1 << 10, "K"}} {
		if bps >= u.div {
			if bps%u.div == 0 {
				return strconv.FormatInt(bps/u.div, 10) + u.unit
			}
			return strconv.FormatFloat(float64(bps)/float64(u.div), 'f', 1, 64) + u.unit
		}
	}
	return strconv.FormatInt(bps, 10)
}

type limitedReader struct {
	r io.Reader
	l *Limiter
}

func (lr limitedReader) Read(p []byte) (int, error) {
	if c := lr.l.chunk(); c > 0 && len(p) > c {
		p = p[:c]
	}
	n, err := lr.r.Read(p)
	lr.l.take(n)
	return n, err
}

type limitedReadCloser struct {
	limitedReader
	c io.Closer
}

func (lr limitedReadCloser) Close() error { return lr.c.Close() }

type limitedWriter struct {
	w io.Writer
	l *Limiter
}

func (lw limitedWriter) Write(p []byte) (int, error) {
	c := lw.l.chunk()
	if c <= 0 || len(p) <= c {
		n, err := lw.w.Write(p)
		lw.l.take(n)
		return n, err
	}
	total := 0
	for len(p) > 0 {
		if c <= 0 || c > len(p) {
			c = len(p)
		}
		n, err := lw.w.Write(p[:c])
		total += n
		lw.l.take(n)
		if err != nil {
			return total, err
		}
		p = p[n:]
		c = lw.l.chunk()
	}
	return total, nil
}

// LimitReader throttles a source stream against the inbound budget.
func LimitReader(r io.Reader) io.Reader { return limitedReader{r: r, l: &bwIn} }

// LimitReadCloser is LimitReader for a stream the caller must close.
func LimitReadCloser(rc io.ReadCloser) io.ReadCloser {
	return limitedReadCloser{limitedReader: limitedReader{r: rc, l: &bwIn}, c: rc}
}

// LimitWriter throttles a destination stream against the outbound budget.
func LimitWriter(w io.Writer) io.Writer { return limitedWriter{w: w, l: &bwOut} }

// TakeIn charges the inbound budget for bytes a backend read outside of a
// wrapped stream (e.g. a hash loop reading straight into its own buffer).
func TakeIn(n int) { bwIn.take(n) }

type limitedConn struct{ net.Conn }

func (c limitedConn) Read(p []byte) (int, error) {
	if n := bwIn.chunk(); n > 0 && len(p) > n {
		p = p[:n]
	}
	n, err := c.Conn.Read(p)
	bwIn.take(n)
	return n, err
}

func (c limitedConn) Write(p []byte) (int, error) {
	return limitedWriter{w: c.Conn, l: &bwOut}.Write(p)
}

// LimitConn throttles both directions of a socket. Every backend that dials
// wraps its connection here, which is what makes the limit protocol-agnostic:
// control chatter, listings and file data all share the same budget.
func LimitConn(c net.Conn) net.Conn { return limitedConn{Conn: c} }
