package transport

import (
	"testing"
	"time"
)

// W-L3: invalidateAncestors walks up with parentDir, and parentDir("/")
// was "/": a path with a slash at its start never reached the top, and
// the walk went on for ever, holding the cache's lock. parentDir gives ""
// for the top since c9bed1c; this keeps it so.
func TestInvalidateAncestorsOfASlashPathEnds(t *testing.T) {
	c := newListCache()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.invalidateAncestors("/x")
		c.invalidateAncestors("/x/y")
		c.invalidateAncestors("/")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(`invalidateAncestors("/x") does not end`)
	}
	if got := parentDir("/x"); got != "" {
		t.Errorf(`parentDir("/x") = %q, want ""`, got)
	}
}
