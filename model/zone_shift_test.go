package model

import (
	"fmt"
	"testing"
	"time"
)

// F4: with -tzdst (on by default) two mtimes any whole number of hours
// apart compared equal, ±1 s: a disk image rewritten daily at one size
// matched yesterday's, a file from a year ago matched today's. Only real
// time-zone shifts count now, up to 14 hours, and the UI can tell such an
// equal from a plain one.
func TestTimeZoneRuleOnlyCoversZoneShifts(t *testing.T) {
	opts := CompareOpts{Size: true, ModTime: true, TimeGrace: true, IgnoreTZDST: true}
	base := time.Date(2026, 3, 29, 1, 30, 0, 0, time.UTC)
	for _, tc := range []struct {
		apart time.Duration
		equal bool
		shift int
	}{
		{0, true, 0},
		{time.Second, true, 0},
		{5 * time.Second, false, 0},
		{time.Hour, true, 1},
		{time.Hour + time.Second, true, 1},
		{time.Hour - time.Second, true, 1},
		{90 * time.Minute, false, 0},
		{14 * time.Hour, true, 14},
		{14*time.Hour + time.Second, true, 14},
		{15 * time.Hour, false, 0},
		{24 * time.Hour, false, 0},
		{168 * time.Hour, false, 0},
		{8760 * time.Hour, false, 0},
	} {
		for _, sign := range []time.Duration{1, -1} {
			l := &FileEntry{Name: "f", Size: 10, ModTime: base}
			r := &FileEntry{Name: "f", Size: 10, ModTime: base.Add(sign * tc.apart)}
			n := &TreeNode{Name: "f", Sides: [2]SideState{{Entry: l}, {Entry: r}}}
			compareNode(n, opts)
			what := fmt.Sprintf("mtimes %v apart", sign*tc.apart)
			if got := n.Compare.ModTime == AttrEqual; got != tc.equal {
				t.Errorf("%s: equal=%v, want %v", what, got, tc.equal)
			}
			if got := ZoneShiftHours(l.ModTime, r.ModTime, opts); got != tc.shift {
				t.Errorf("%s: ZoneShiftHours = %d, want %d", what, got, tc.shift)
			}
		}
	}
	off := CompareOpts{Size: true, ModTime: true, TimeGrace: true}
	if got := cmpTime(base, base.Add(time.Hour), off); got != AttrDifferent {
		t.Errorf("-tzdst=false: mtimes 1h apart compare %v, want different", got)
	}
	if got := ZoneShiftHours(base, base.Add(time.Hour), off); got != 0 {
		t.Errorf("-tzdst=false: ZoneShiftHours = %d, want 0", got)
	}
}
