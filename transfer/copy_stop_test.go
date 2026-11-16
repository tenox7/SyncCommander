package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// goneStub is a remote destination that answers the scan, then goes away:
// the connection drops, and no new one can be made.
type goneStub struct {
	listStub
	gone    atomic.Bool
	uploads atomic.Int64
}

func (g *goneStub) CopyFrom(_ context.Context, _ string, src io.Reader, _ model.PutParams) error {
	g.uploads.Add(1)
	if g.gone.Load() {
		return io.ErrUnexpectedEOF
	}
	_, err := io.Copy(io.Discard, src)
	return err
}

// F7: a copy to a server that went away tried every file in turn, each
// with its tries of tries of connects: about 11 minutes per file at the
// default settings, so a large copy never ended. The copy now stops once
// the destination stops connecting, and the log says why.
func TestCopyStopsWhenTheDestinationIsGone(t *testing.T) {
	transport.SetMaxRetries(1)
	defer transport.SetMaxRetries(5)
	const files = 8
	left := t.TempDir()
	for i := range files {
		writeFile(t, filepath.Join(left, fmt.Sprintf("f%d.bin", i)), "data")
	}
	inner := &goneStub{}
	var dials atomic.Int64
	dst := transport.NewLazyBackend("sftp://host/dst", func(context.Context) (model.Backend, error) {
		if dials.Add(1) == 1 {
			return inner, nil
		}
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}
	})
	src := transport.NewLocalBackend(left)
	s := scanned(t, left, "", src, dst)
	inner.gone.Store(true)

	mark := len(transport.Log.Lines())
	p := &Progress{}
	t0 := time.Now()
	Copy(context.Background(), Request{Src: src, Dst: dst, Scanner: s.sc, Node: s.sc.Tree(), RelPath: "",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p})
	t.Logf("copy of %d files to a gone server: %d connects, %d uploads, %d files tried, in %v",
		files, dials.Load()-1, inner.uploads.Load(), p.Done.Load(), time.Since(t0).Round(100*time.Millisecond))
	if n := dials.Load() - 1; n > 2 {
		t.Errorf("%d connects after the server went away, want at most 2 (max-retries+1)", n)
	}
	if n := p.Done.Load(); n >= files {
		t.Errorf("all %d files were tried, want the copy to stop", n)
	}
	if !logged(mark, "COPY stopped") {
		t.Error("no log line says the copy stopped")
	}
}
