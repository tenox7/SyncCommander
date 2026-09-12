package ui

import "testing"

func TestStepRate(t *testing.T) {
	cases := []struct {
		cur   int64
		delta int
		want  int64
	}{
		{0, 1, 32 << 10},
		{0, -1, 0},
		{32 << 10, -1, 0},
		{1 << 20, 1, 2 << 20},
		{1 << 30, 1, 1 << 30},
		// Off-ladder values from -bwlimit snap to the next rung either way.
		{100 << 10, 1, 128 << 10},
		{100 << 10, -1, 64 << 10},
	}
	for _, c := range cases {
		if got := stepRate(c.cur, c.delta); got != c.want {
			t.Errorf("stepRate(%d, %d) = %d, want %d", c.cur, c.delta, got, c.want)
		}
	}
}

// A bandwidth option must not respond to space/enter (no bool to flip).
func TestToggleIgnoresRateOption(t *testing.T) {
	var set int64 = -1
	d := NewSettingsDialog()
	d.SetOptions([]Option{{Label: "bw", GetRate: func() int64 { return 0 }, SetRate: func(v int64) { set = v }}})
	d.Open()
	d.Toggle()
	if set != -1 {
		t.Fatalf("Toggle wrote %d", set)
	}
	d.Adjust(1)
	if set != 32<<10 {
		t.Fatalf("Adjust wrote %d", set)
	}
}
