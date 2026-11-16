package transport

import (
	"context"
	"io"
	"strings"
	"testing"
)

// L3: an FTP server ends the data connection the same way for a whole file
// and for one it gave up on half way, and says which only on the control
// connection afterwards: 226, or 426. sc read that reply only in Close, and
// the copy dropped Close's error, so a download the server aborted was a
// whole file. Read now ends with the reply's error, and keeps returning it.
func TestFTPReadEndsWithTheServersReply(t *testing.T) {
	f := &fakeFTPD{
		ls: map[string][]string{"/": nil},
		files: map[string]fakeFTPFile{
			"/cut.bin":   {strings.Repeat("x", 4000), "426 Connection closed; transfer aborted."},
			"/whole.bin": {strings.Repeat("y", 4000), "226 Transfer complete."},
		},
	}
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()

	r, err := b.Open(ctx, "cut.bin")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(r)
	if err == nil || !strings.Contains(err.Error(), "426") {
		t.Errorf("read to the end of a download the server aborted: %d bytes, err=%v; want the 426", len(body), err)
	}
	if _, err := r.Read(make([]byte, 10)); err == nil || err == io.EOF {
		t.Errorf("a Read after the end of an aborted download: %v; want the 426 again", err)
	}
	if err := r.Close(); err == nil {
		t.Error("Close of an aborted download: no error")
	}

	// A whole file ends with io.EOF and stays there: a copy reads once more
	// to see whether the source grew.
	r, err = b.Open(ctx, "whole.bin")
	if err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(r); err != nil || len(body) != 4000 {
		t.Errorf("a whole file: %d bytes, err=%v", len(body), err)
	}
	if n, err := r.Read(make([]byte, 10)); n != 0 || err != io.EOF {
		t.Errorf("a Read after the end of a whole file: %d, %v; want 0, io.EOF", n, err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close of a whole file: %v", err)
	}
}
