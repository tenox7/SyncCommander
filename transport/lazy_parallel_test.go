package transport

import (
	"context"
	"io"
	"testing"

	"sc/model"
)

// parallelStub records the pool size it was last given.
type parallelStub struct {
	copyStub
	n int
}

func (s *parallelStub) SetParallel(n int) { s.n = n }

// The factory sizes each new connection's pool for the start value; after +/-
// a redial must come back with the value the user picked.
func TestLazySetParallelSurvivesRedial(t *testing.T) {
	var dials []*parallelStub
	b := NewLazyBackend("stub://host/path", func(context.Context) (model.Backend, error) {
		s := &parallelStub{n: 4} // what OpenBackend(…, parallel=4) builds
		dials = append(dials, s)
		return s, nil
	})
	ctx := context.Background()

	SetParallel(b, 6) // not connected yet: kept for the first dial
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if got := dials[0].n; got != 6 {
		t.Fatalf("first dial sized for %d, want 6", got)
	}
	SetParallel(b, 9) // connected: applied at once
	if got := dials[0].n; got != 9 {
		t.Fatalf("connected backend sized for %d, want 9", got)
	}

	l := b.(*lazyBackend)
	l.markBrokenIf(l.current(), io.EOF)
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if len(dials) != 2 {
		t.Fatalf("want a redial, got %d dial(s)", len(dials))
	}
	if got := dials[1].n; got != 9 {
		t.Fatalf("redial sized for %d, want 9", got)
	}
}
