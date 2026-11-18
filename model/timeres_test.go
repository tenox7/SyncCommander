package model

import (
	"testing"
	"time"
)

func TestTimeResOf(t *testing.T) {
	for _, c := range []struct {
		step time.Duration
		want TimeRes
	}{
		{0, TimeNone}, {-time.Second, TimeNone},
		{time.Nanosecond, TimeExact}, {100 * time.Nanosecond, TimeMicro}, {time.Microsecond, TimeMicro},
		{time.Millisecond, TimeMilli}, {10 * time.Millisecond, TimeSecond}, {time.Second, TimeSecond},
		{2 * time.Second, Time2Sec}, {time.Minute, TimeMinute}, {24 * time.Hour, TimeDay},
		{100 * 365 * 24 * time.Hour, TimeNone},
	} {
		if got := TimeResOf(c.step); got != c.want {
			t.Errorf("TimeResOf(%v) = %d, want %d", c.step, got, c.want)
		}
	}
}

// timedNode is a file of 3 bytes on both sides: here at local, exact;
// there at remote, kept to res.
func timedNode(local, remote time.Time, res TimeRes) *TreeNode {
	n := &TreeNode{Name: "f", RelPath: "f"}
	n.Sides[SideLeft].Entry = &FileEntry{RelPath: "f", Name: "f", Size: 3, ModTime: local}
	n.Sides[SideRight].Entry = &FileEntry{RelPath: "f", Name: "f", Size: 3, ModTime: remote, TimeRes: res}
	return n
}

// L17: an mtime kept to the minute, the day, two seconds or the second
// compares equal to the time it was set from; one further off than that
// differs; an upload time is left out of the compare.
func TestModTimeComparesAsExactlyAsItIsKept(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 34, 56, 789_000_000, time.UTC)
	opts := CompareOpts{Size: true, ModTime: true}
	sub := opts
	sub.SubSecond = true
	for _, c := range []struct {
		name   string
		remote time.Time
		res    TimeRes
		opts   CompareOpts
		want   AttrStatus
	}{
		{"LIST, to the minute", at.Truncate(time.Minute), TimeMinute, opts, AttrEqual},
		{"LIST, a minute off", at.Truncate(time.Minute).Add(-time.Minute), TimeMinute, opts, AttrDifferent},
		{"LIST with a year", at.Truncate(24 * time.Hour), TimeDay, opts, AttrEqual},
		{"LIST with a year, a day off", at.Truncate(24 * time.Hour).Add(-24 * time.Hour), TimeDay, opts, AttrDifferent},
		{"FAT, rounded up", at.Truncate(2 * time.Second).Add(2 * time.Second), Time2Sec, opts, AttrEqual},
		{"FAT, 3 s off", at.Add(3 * time.Second), Time2Sec, opts, AttrDifferent},
		{"whole seconds, sub-second compare", at.Truncate(time.Second), TimeSecond, sub, AttrEqual},
		{"exact, sub-second compare", at.Truncate(time.Second), TimeExact, sub, AttrDifferent},
		{"exact, to the second", at.Truncate(time.Second), TimeExact, opts, AttrEqual},
		{"an upload time", at.Add(3 * time.Hour), TimeNone, opts, AttrNA},
	} {
		n := timedNode(at, c.remote, c.res)
		compareNode(n, c.opts)
		if n.Compare.ModTime != c.want {
			t.Errorf("%s: mtime %v, want %v", c.name, n.Compare.ModTime, c.want)
		}
	}
}

// L17: a file whose copy there has an upload time for its mtime is equal
// by its size, and the upload time is not "newer": a copy over it is no
// overwrite of a newer file.
func TestUploadTimeIsNotNewer(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 34, 56, 0, time.UTC)
	opts := CompareOpts{Size: true, ModTime: true}
	n := timedNode(at, at.Add(3*time.Hour), TimeNone)
	compareNode(n, opts)
	if got := NodeStatus(n, &opts); got != AttrEqual {
		t.Errorf("NodeStatus = %v, want equal by size", got)
	}
	if why := OverwritesNewer(n, true, opts); why != "" {
		t.Errorf("OverwritesNewer = %q, want none", why)
	}
	n = timedNode(at, at.Add(3*time.Hour), TimeExact)
	compareNode(n, opts)
	if why := OverwritesNewer(n, true, opts); why != "newer" {
		t.Errorf("with an exact time 3h later: OverwritesNewer = %q, want newer", why)
	}
}
