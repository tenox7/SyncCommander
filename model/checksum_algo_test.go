package model

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// algoStub offers sha256, or cannot say, as a server that is away.
type algoStub struct {
	stubBackend
	away atomic.Bool
}

func (b *algoStub) ChecksumAlgos(context.Context) ([]string, error) {
	if b.away.Load() {
		return nil, errors.New("connection refused")
	}
	return []string{"sha256"}, nil
}

// D2: the scanner asked both sides for their checksums once and kept the
// answer for good. An ask that failed, a server away for a moment, left
// checksums off until sc was started again. A failed ask keeps nothing.
func TestChecksumAskThatFailedIsAskedAgain(t *testing.T) {
	ctx := context.Background()
	left, right := &algoStub{}, &algoStub{}
	left.away.Store(true)
	s := NewScanner(left, right, 1, 1, true)
	if algo, ok := s.NegotiateChecksum(ctx); ok {
		t.Fatalf("with one side away the scanner picked %q", algo)
	}
	left.away.Store(false)
	if algo, ok := s.NegotiateChecksum(ctx); !ok || algo != "sha256" {
		t.Errorf("once both sides answer, the scanner picked %q, %v; want sha256", algo, ok)
	}
}
