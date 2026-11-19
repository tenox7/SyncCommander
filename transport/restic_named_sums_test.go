package transport

import (
	"context"
	"testing"

	"sc/model"
)

// W-M3: restic's sha256 is each object's name, never read from its
// content, so a compare with a restic side checks the names. The scanner
// learns it from the side's Caps, through the wrapper that holds every
// remote, and keeps it by side: a side swap moves it.
func TestResticSumsAreNamesToTheScanner(t *testing.T) {
	f := &fakeRestic{objects: map[string]string{"config": "cfg", "keys/" + keyA: "k"}}
	url := fakeResticURL(t, f)
	right := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) {
		return NewResticBackend(ctx, url, false, 1)
	})
	defer CloseBackend(right)
	s := model.NewScanner(NewLocalBackend(t.TempDir()), right, 4, 8, true)
	if algo, ok := s.NegotiateChecksum(context.Background()); !ok || algo != "sha256" {
		t.Fatalf("the sides agreed on %q, %v; want sha256", algo, ok)
	}
	if l, r := s.ChecksumNamed(model.SideLeft), s.ChecksumNamed(model.SideRight); l || !r {
		t.Errorf("sums that are names: left %v, right %v; want the restic side's alone", l, r)
	}
	s.SwapSides()
	if l, r := s.ChecksumNamed(model.SideLeft), s.ChecksumNamed(model.SideRight); !l || r {
		t.Errorf("after a swap, sums that are names: left %v, right %v; want the left side's alone", l, r)
	}
}
