package transport

import (
	"strings"
	"testing"
)

// A warning is kept in the log but is not an error: it neither counts nor
// shows in the errors-only view, so it never pops the console.
func TestLogWarnIsNotAnError(t *testing.T) {
	l := &RemoteLog{}
	l.Add("webdav", DirWarn, "mkcol parents: MKCOL x: 423 Locked")
	l.Add("webdav", DirErr, "PUT y: 500 Internal Server Error")
	if l.ErrCount() != 1 || l.ErrLen() != 1 || l.Len() != 2 {
		t.Fatalf("errCount=%d errLen=%d len=%d, want 1 1 2", l.ErrCount(), l.ErrLen(), l.Len())
	}
	if line := l.Lines()[0]; !strings.Contains(line, " WARN ") {
		t.Fatalf("warn line = %q", line)
	}
}
