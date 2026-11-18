package transport

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

// R16: a download from an rsync daemon goes through a temp file. A temp
// disk that filled during it read as a full destination, which no file
// gets through, and stopped the whole copy. Now it fails that file and
// lets it be tried again, and the temp file goes. SC_SMALL_TMPDIR names a
// folder on a disk with less than 2 MiB free, a 1 MiB tmpfs.
func TestDaemonDownloadIntoAFullTemp(t *testing.T) {
	small := os.Getenv("SC_SMALL_TMPDIR")
	if small == "" {
		t.Skip("SC_SMALL_TMPDIR names no small disk")
	}
	mod := t.TempDir()
	writeTestFile(t, mod, "big.bin", strings.Repeat("b", 2<<20))
	writeTestFile(t, mod, "small.bin", strings.Repeat("s", 100<<10))
	b, err := NewRsyncBackend(startGorsyncdModule(t, mod, true))
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := os.MkdirTemp(small, "t-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })
	setTemp(t, tmp)
	ctx := context.Background()
	for _, offset := range []int64{0, 10} {
		r, err := b.OpenAt(ctx, "big.bin", offset)
		if err == nil {
			r.Close()
			t.Fatalf("a download of 2 MiB from %d went through into 1 MiB", offset)
		}
		fullTemp(t, fmt.Sprintf("a daemon download from %d", offset), err)
		if left := leftInTemp(t, tmp); len(left) > 0 {
			t.Errorf("the temp folder holds %v after a download that did not fit", left)
		}
	}
	r, err := b.Open(ctx, "small.bin")
	if err != nil {
		t.Fatalf("a download that fits: %v", err)
	}
	defer r.Close()
	if got, err := io.ReadAll(r); err != nil || len(got) != 100<<10 {
		t.Errorf("a download that fits read %d bytes, %v", len(got), err)
	}
}
