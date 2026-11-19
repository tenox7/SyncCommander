package transport

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
)

// R14: a failed try kept what it credited, and the copy layer adds each
// try's count to the file's: an rsync upload that failed once and then
// worked counted one and a half times its size, as only the push half was
// taken back, not the staging half. A failed upload counts nothing now, on
// a daemon and over ssh.
func TestRsyncFailedUploadTakesBackItsProgress(t *testing.T) {
	port, _ := startRsyncDaemonConf(t, func(root string) string {
		return "[ro]\n  path = " + filepath.Join(root, "ro") + "\n  read only = yes\n"
	})
	daemon, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/ro", port))
	if err != nil {
		t.Fatal(err)
	}
	backends := map[string]struct {
		b    model.Backend
		path string
	}{"daemon": {daemon, "x.txt"}}
	root := t.TempDir()
	d := filepath.Join(root, "d")
	if err := os.Mkdir(d, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(d, 0o755) })
	if os.WriteFile(filepath.Join(d, "probe"), nil, 0o644) != nil {
		ssh, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+startExecSSHServer(t)+root, true, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer CloseBackend(ssh)
		backends["ssh"] = struct {
			b    model.Backend
			path string
		}{ssh, "d/x.txt"}
	} else {
		t.Log("the folder can be written to after chmod 555 (root?): no ssh case")
	}
	for name, c := range backends {
		var n atomic.Int64
		ctx := ContextWithProgressBudget(ContextWithProgress(context.Background(), &n), 1000)
		mark := len(Log.Lines())
		err := c.b.CopyFrom(ctx, c.path, bytes.NewReader(make([]byte, 1000)), model.PutParams{Size: 1000, Mode: 0o644})
		if err == nil {
			t.Errorf("%s: an upload the server cannot take: no error", name)
			continue
		}
		if logCount(mark, "SEND "+c.path) == 0 {
			t.Errorf("%s: %v before the push; the test proves nothing", name, err)
			continue
		}
		if got := n.Load(); got != 0 {
			t.Errorf("%s: a failed upload left %d of its 1000 bytes counted; want 0", name, got)
		}
	}
}

// A failed receive kept what it credited, of rsync's temp file and of a
// file received in place, and of the base it found there.
func TestFailedReceiveTakesBackItsProgress(t *testing.T) {
	dir := t.TempDir()
	counters := func() (context.Context, *atomic.Int64, *atomic.Int64) {
		n, base := new(atomic.Int64), new(atomic.Int64)
		return ContextWithProgressBudget(ContextWithBaseProgress(ContextWithProgress(context.Background(), n), base), 1000), n, base
	}
	waitFor := func(what string, n *atomic.Int64, want int64) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); n.Load() != want && time.Now().Before(deadline); {
			time.Sleep(20 * time.Millisecond)
		}
		if got := n.Load(); got != want {
			t.Fatalf("%s: %d credited; want %d (the test proves nothing)", what, got, want)
		}
	}

	ctx, n, _ := counters()
	stop := tailProgress(ctx, dir, "f.bin", false, 0)
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(dir, ".f.bin.123"), make([]byte, 600), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor("rsync's temp file", n, 600)
	stop(false)
	if got := n.Load(); got != 0 {
		t.Errorf("a failed receive left %d bytes counted; want 0", got)
	}

	ctx, n, base := counters()
	inPlace := filepath.Join(dir, "g.bin")
	if err := os.WriteFile(inPlace, make([]byte, 300), 0o644); err != nil {
		t.Fatal(err)
	}
	stop = tailProgress(ctx, dir, "g.bin", true, 100)
	waitFor("the base beyond the offset", n, 200)
	if err := os.WriteFile(inPlace, make([]byte, 700), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor("the file received in place", n, 600)
	stop(false)
	if got, gotBase := n.Load(), base.Load(); got != 0 || gotBase != 0 {
		t.Errorf("a failed receive in place left %d bytes counted, %d of them base; want 0, 0", got, gotBase)
	}

	ctx, n, _ = counters()
	stop = tailProgress(ctx, dir, "h.bin", false, 0)
	stop(true)
	if got := n.Load(); got != 1000 {
		t.Errorf("a receive that worked counted %d; want its 1000", got)
	}
}
