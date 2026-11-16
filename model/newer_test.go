package model

import (
	"testing"
	"time"
)

// F16: what counts as newer or larger on the destination. Times count as
// the compare counts them.
func TestOverwritesNewer(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	opts := CompareOpts{Size: true, ModTime: true, TimeGrace: true, IgnoreTZDST: true}
	for _, c := range []struct {
		name     string
		src, dst FileEntry
		want     string
	}{
		{"older", FileEntry{Size: 5, ModTime: at.Add(time.Minute)}, FileEntry{Size: 5, ModTime: at}, ""},
		{"newer", FileEntry{Size: 5, ModTime: at}, FileEntry{Size: 5, ModTime: at.Add(10 * time.Minute)}, "newer"},
		{"larger", FileEntry{Size: 5, ModTime: at}, FileEntry{Size: 9, ModTime: at}, "larger"},
		{"both", FileEntry{Size: 5, ModTime: at}, FileEntry{Size: 9, ModTime: at.Add(10 * time.Minute)}, "newer and larger"},
		{"within the grace", FileEntry{Size: 5, ModTime: at}, FileEntry{Size: 5, ModTime: at.Add(time.Second)}, ""},
		{"a zone shift", FileEntry{Size: 5, ModTime: at}, FileEntry{Size: 5, ModTime: at.Add(time.Hour)}, ""},
		{"no time on the source", FileEntry{Size: 5}, FileEntry{Size: 5, ModTime: at}, ""},
		{"folders", FileEntry{IsDir: true, ModTime: at}, FileEntry{IsDir: true, Size: 9, ModTime: at.Add(10 * time.Minute)}, ""},
	} {
		src, dst := c.src, c.dst
		n := &TreeNode{Sides: sides(&src, &dst)}
		if got := OverwritesNewer(n, true, opts); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
		// Copied the other way, the newer file is the source.
		if got := OverwritesNewer(&TreeNode{Sides: sides(&dst, &src)}, false, opts); got != c.want {
			t.Errorf("%s, right to left: %q, want %q", c.name, got, c.want)
		}
		if c.want != "" {
			if got := OverwritesNewer(n, false, opts); got != "" {
				t.Errorf("%s: copying the newer file over the older one is %q", c.name, got)
			}
		}
	}
}
