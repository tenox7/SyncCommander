package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"sc/model"
	"sc/transport"
)

// dropStub is a remote destination whose first upload drops after a few
// bytes, as a link that goes down in the middle of a file.
type dropStub struct {
	listStub
	uploads atomic.Int64
	got     atomic.Pointer[string]
}

func (d *dropStub) CopyFrom(_ context.Context, _ string, src io.Reader, _ os.FileMode) error {
	if d.uploads.Add(1) == 1 {
		_, _ = io.CopyN(io.Discard, src, 4)
		return errors.New("write tcp 10.0.0.2:50000->10.0.0.1:22: write: connection reset by peer")
	}
	b, err := io.ReadAll(src)
	s := string(b)
	d.got.Store(&s)
	return err
}

// F29: an upload to a remote that dropped after its first byte was never
// tried again: the reconnecting wrapper marked the error "source consumed"
// so as not to retry it itself, and the copy layer's retry stopped on the
// same mark. Every drop in the middle of an upload failed that file.
func TestUploadCutOffMidFileIsRetried(t *testing.T) {
	const body = "0123456789abcdef"
	left := t.TempDir()
	writeFile(t, filepath.Join(left, "f.bin"), body)
	inner := &dropStub{}
	dst := transport.NewLazyBackend("sftp://host/dst", func(context.Context) (model.Backend, error) { return inner, nil })
	src := transport.NewLocalBackend(left)
	sc := model.NewScanner(src, dst, 1, 1, true)
	sc.Scan(context.Background(), scenarioOpts)
	var node *model.TreeNode
	for _, c := range sc.Tree().Children {
		if c.Name == "f.bin" {
			node = c
		}
	}
	if node == nil {
		t.Fatal("f.bin not in the tree")
	}
	p := &Progress{}
	Copy(context.Background(), Request{Src: src, Dst: dst, Scanner: sc, Node: node, RelPath: "f.bin",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p})
	if n := p.Failed.Load(); n != 0 {
		t.Errorf("%d file(s) failed, want the upload tried again", n)
	}
	if n := inner.uploads.Load(); n != 2 {
		t.Errorf("%d upload(s), want 2: the cut-off one and one more", n)
	}
	if got := inner.got.Load(); got == nil || *got != body {
		t.Errorf("the destination got %v, want the whole file", got)
	}
}
