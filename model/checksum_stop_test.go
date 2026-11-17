package model_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// checksumStop stops a checksum run: once limit sums are done, on either
// side, the next one stops the run and returns when it has stopped. The sum
// of failOn fails, as a file that cannot be read.
type checksumStop struct {
	sums   atomic.Int64
	limit  int64
	cancel context.CancelFunc
	failOn string
}

type stoppingBackend struct {
	model.Backend
	stop *checksumStop
}

func (b stoppingBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	if relPath == b.stop.failOn {
		return "", fs.ErrPermission
	}
	if b.stop.cancel != nil && b.stop.sums.Add(1) > b.stop.limit {
		b.stop.cancel()
		<-ctx.Done()
		return "", ctx.Err()
	}
	return b.Backend.Checksum(ctx, relPath, algo)
}

func statusName(s model.AttrStatus) string {
	switch s {
	case model.AttrUnknown:
		return "not compared"
	case model.AttrScanning:
		return "being summed"
	case model.AttrEqual:
		return "equal"
	case model.AttrDifferent:
		return "different"
	}
	return "not applicable"
}

// checksumState is what the tree holds of a checksum run: each file's
// status and sums, and the folders still marked as having work scheduled.
type checksumState struct {
	status  map[string]model.AttrStatus
	sums    map[string][2]string
	pending []string
}

func readChecksumState(sc *model.Scanner) checksumState {
	st := checksumState{status: map[string]model.AttrStatus{}, sums: map[string][2]string{}}
	var walk func(n *model.TreeNode)
	walk = func(n *model.TreeNode) {
		if !n.IsDir {
			st.status[n.RelPath] = n.Compare.Checksum
			st.sums[n.RelPath] = [2]string{n.Sides[model.SideLeft].Checksum, n.Sides[model.SideRight].Checksum}
			return
		}
		for _, side := range n.Sides {
			if side.ChecksumPending || side.ChecksumActive {
				st.pending = append(st.pending, n.RelPath)
				break
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	sc.ReadTree(func(root *model.TreeNode) { walk(root) })
	return st
}

// F11: a checksum run cleared the sums of every file it was going to sum
// before it started, and marked them "scanning". Stopped half way, the
// files it had not reached kept no sum and stayed "scanning", and a pass
// over what is pending skipped them: hours of checksums, gone. Now a sum is
// replaced only when the new one arrives, and a stopped run gives each file
// it did not finish the state its sums give. A sum that fails for real
// still goes, and a new sum still replaces the old one.
func TestAStoppedChecksumRunKeepsTheSums(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	write := func(side, rel, body string) {
		t.Helper()
		p := filepath.Join(root, side, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	for _, side := range []string{"l", "r"} {
		for d := range 3 {
			for f := range 10 {
				write(side, fmt.Sprintf("d%d/f%d.txt", d, f), fmt.Sprintf("file %d %d", d, f))
			}
		}
	}
	stop := &checksumStop{}
	lb := stoppingBackend{transport.NewLocalBackend(filepath.Join(root, "l")), stop}
	rb := stoppingBackend{transport.NewLocalBackend(filepath.Join(root, "r")), stop}
	ctx := context.Background()
	sc := model.NewScanner(lb, rb, 1, 1, true)
	sc.Scan(ctx, model.CompareOpts{Size: true, ModTime: true, Checksum: true, TimeGrace: true})
	summed := readChecksumState(sc)
	if len(summed.status) != 30 {
		t.Fatalf("the scan found %d files, want 30", len(summed.status))
	}
	for rel, status := range summed.status {
		if s := summed.sums[rel]; status != model.AttrEqual || s[0] == "" || s[0] != s[1] {
			t.Fatalf("after the scan %s is %s with sums %q", rel, statusName(status), s)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	stop.limit, stop.cancel = 7, cancel
	sc.ChecksumNode(runCtx, sc.Tree())
	cancel()
	if got := stop.sums.Load(); got >= 60 {
		t.Fatalf("the run was not stopped: %d sums asked for", got)
	}
	stopped := readChecksumState(sc)
	for rel, status := range stopped.status {
		if status != model.AttrEqual || stopped.sums[rel] != summed.sums[rel] {
			t.Errorf("after a stopped run %s is %s with sums %q, want equal with %q", rel, statusName(status), stopped.sums[rel], summed.sums[rel])
		}
	}
	if len(stopped.pending) > 0 {
		t.Errorf("after a stopped run the folders %q still show work scheduled", stopped.pending)
	}

	// d0/f0.txt grows on the right: a rescan of it drops the right sum and
	// sums it again, and is stopped. The left sum still fits and stays.
	write("r", "d0/f0.txt", "a longer body on the right")
	var f0 *model.TreeNode
	sc.ReadTree(func(root *model.TreeNode) {
		for _, d := range root.Children {
			for _, f := range d.Children {
				if f.RelPath == "d0/f0.txt" {
					f0 = f
				}
			}
		}
	})
	fileCtx, cancelFile := context.WithCancel(ctx)
	stop.limit, stop.cancel = stop.sums.Load(), cancelFile
	sc.RescanNode(fileCtx, f0, model.CompareOpts{Size: true, ModTime: true, Checksum: true, TimeGrace: true}, nil)
	cancelFile()
	one := readChecksumState(sc)
	if st, sums := one.status["d0/f0.txt"], one.sums["d0/f0.txt"]; st != model.AttrUnknown || sums != [2]string{summed.sums["d0/f0.txt"][0], ""} {
		t.Errorf("after a stopped rescan of d0/f0.txt it is %s with sums %q, want not compared with the left sum %q", statusName(st), sums, summed.sums["d0/f0.txt"][0])
	}

	// d2/f5.txt changes on the right, same size and time: its old sums still
	// fit it. d1/f3.txt cannot be read. A run to the end sums all again.
	write("r", "d2/f5.txt", "FILE 2 5")
	stop.cancel, stop.failOn = nil, "d1/f3.txt"
	sc.ChecksumNode(ctx, sc.Tree())
	done := readChecksumState(sc)
	for rel, status := range done.status {
		want := model.AttrEqual
		switch rel {
		case "d2/f5.txt", "d0/f0.txt":
			want = model.AttrDifferent
		case "d1/f3.txt":
			want = model.AttrUnknown
		}
		if status != want {
			t.Errorf("after a full run %s is %s, want %s", rel, statusName(status), statusName(want))
		}
	}
	if s := done.sums["d1/f3.txt"]; s != ([2]string{}) {
		t.Errorf("d1/f3.txt could not be read and kept the sums %q", s)
	}
	if s := done.sums["d2/f5.txt"]; s[1] == summed.sums["d2/f5.txt"][1] {
		t.Errorf("d2/f5.txt changed on the right and kept its old sum %q", s[1])
	}
}
