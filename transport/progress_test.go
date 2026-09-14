package transport

import (
	"io"
	"strings"
	"testing"
)

type unwrapping struct {
	io.Reader
	inner io.Reader
}

func (u unwrapping) Unwrap() io.Reader { return u.inner }

type opaque struct{ io.Reader }

// Destinations that own their progress check the source for the pre-counted
// marker; a wrapper that exposes Unwrap must not hide it.
func TestIsPreCountedSeesThroughUnwrap(t *testing.T) {
	pre := WrapPreCounted(io.NopCloser(strings.NewReader("")))
	if !IsPreCounted(unwrapping{Reader: pre, inner: unwrapping{Reader: pre, inner: pre}}) {
		t.Error("marker lost through two Unwrap layers")
	}
	if IsPreCounted(opaque{Reader: pre}) {
		t.Error("an opaque wrapper must hide the marker")
	}
	if IsPreCounted(strings.NewReader("")) {
		t.Error("plain reader reported as pre-counted")
	}
}
