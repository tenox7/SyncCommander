package model

import "testing"

// A count of folders or files is short as a size is: +943545d was hard to
// read beside +80.0G.
func TestFormatCount(t *testing.T) {
	for _, c := range []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{-999, "-999"},
		{1000, "1.0k"},
		{1049, "1.0k"},
		{1050, "1.1k"},
		{943545, "943.5k"},
		{-943545, "-943.5k"},
		{999949, "999.9k"},
		// It never shows 1000.0k: that is 1.0M.
		{999950, "1.0M"},
		{5345645, "5.3M"},
		{999949999, "999.9M"},
		{999950000, "1.0G"},
		{1234567890123, "1.2T"},
		{9223372036854775807, "9.2E"},
		{-9223372036854775808, "-9.2E"},
	} {
		if got := FormatCount(c.n); got != c.want {
			t.Errorf("FormatCount(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}
