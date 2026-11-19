package transport

import (
	"context"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// mdtmsAsked counts the MDTM commands the fake heard, on every connection.
func mdtmsAsked(f *fakeFTPD) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, lines := range f.heard {
		for _, l := range lines {
			if strings.HasPrefix(strings.ToUpper(l), "MDTM ") {
				n++
			}
		}
	}
	return n
}

// L15: on a server without MLSD, as vsftpd is, a listing asked MDTM for
// each entry: a round trip a file, inside the time a listing has, and 14
// hours for 1M files at 50 ms. A listing now takes the times LIST gives,
// to the minute or the day, which the compare takes as exactly as that
// (L17). Stat, for one file, still asks MDTM.
func TestFTPListingAsksNoMDTM(t *testing.T) {
	exact := time.Date(2026, 11, 16, 10, 0, 42, 0, time.UTC)
	f := &fakeFTPD{mdtm: true, ls: map[string][]string{"/": {
		lsLine(".", true), lsLine("..", true), lsLine("a.txt", false), lsLine("b.txt", false), lsLine("c.txt", false),
	}}, mtimes: map[string]time.Time{"/a.txt": exact, "/b.txt": exact, "/c.txt": exact}}
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	entries, err := b.List(ctx, "")
	if err != nil || len(entries) != 3 {
		t.Fatalf("List = %d entries, %v; want 3", len(entries), err)
	}
	for _, e := range entries {
		if e.TimeRes != model.TimeMinute || e.ModTime.Second() != 0 {
			t.Errorf("%s lists %v, to %v; want the time LIST gives, to the minute", e.Name, e.ModTime, e.TimeRes)
		}
	}
	if n := mdtmsAsked(f); n != 0 {
		t.Errorf("a listing of 3 files asked MDTM %d times", n)
	}
	e, err := b.Stat(ctx, "a.txt")
	if err != nil || !e.ModTime.Equal(exact) || e.TimeRes != model.TimeSecond {
		t.Errorf("Stat(a.txt) = %v, to %v, %v; want %v to the second, by MDTM", e.ModTime, e.TimeRes, err, exact)
	}
}
