package model

import (
	"math"
	"time"
)

// UnixNano is a time kept in 8 bytes, where a time.Time takes 24: the
// nanoseconds since 1970 UTC. A tree keeps three such times for each entry,
// and one for each checksum, and compares them rarely (F13). The zero value
// is no time. A time at the very instant of 1970 is kept a nanosecond later,
// and one before 1678 or after 2262 at that end of the range.
type UnixNano int64

var (
	minUnixNano = time.Unix(0, math.MinInt64)
	maxUnixNano = time.Unix(0, math.MaxInt64)
)

// NanoOf is t as a UnixNano.
func NanoOf(t time.Time) UnixNano {
	switch {
	case t.IsZero():
		return 0
	case t.Before(minUnixNano):
		return math.MinInt64
	case t.After(maxUnixNano):
		return math.MaxInt64
	}
	if n := t.UnixNano(); n != 0 {
		return UnixNano(n)
	}
	return 1
}

// Time is n in the local time zone, as time.Unix gives it, which is how the
// backends read times; the zero time for no time.
func (n UnixNano) Time() time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(n))
}

// IsZero reports no time.
func (n UnixNano) IsZero() bool { return n == 0 }
