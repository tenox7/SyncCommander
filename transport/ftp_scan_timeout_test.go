package transport

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sc/model"
)

// lazyFakeFTP opens the fake as sc opens a remote: through the lazy
// backend, which tells the scanner whether FTP keeps its own deadlines.
func lazyFakeFTP(t *testing.T, f *fakeFTPD, parallel int) model.Backend {
	t.Helper()
	url := "ftp://u:p@" + f.start(t) + "/"
	b := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) {
		return NewFTPBackend(ctx, url, false, parallel)
	})
	t.Cleanup(func() { CloseBackend(b) })
	return b
}

// setListTimeout sets the time the scanner gives one listing, for one test.
func setListTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := model.ListTimeout
	model.ListTimeout = d
	t.Cleanup(func() { model.ListTimeout = old })
}

// pacedFolder is a server folder of n files, listed a line every pace.
func pacedFolder(n int, pace time.Duration) *fakeFTPD {
	lines := []string{lsLine(".", true), lsLine("..", true)}
	for i := range n {
		lines = append(lines, lsLine(fmt.Sprintf("f%03d.txt", i), false))
	}
	return &fakeFTPD{ls: map[string][]string{"/": lines}, listPace: pace}
}

// scanRight scans the empty folder local against b, and counts the files
// b listed, and whether its listing failed.
func scanRight(local string, b model.Backend) (files int, failed bool) {
	s := model.NewScanner(NewLocalBackend(local), b, 1, 1, true)
	s.Scan(context.Background(), model.CompareOpts{Size: true, ModTime: true})
	s.ReadTree(func(root *model.TreeNode) {
		failed = root.ListErr
		for _, c := range root.Children {
			if r := c.Sides[model.SideRight].Entry; r != nil && !r.IsDir {
				files++
			}
		}
	})
	return files, failed
}

// L15: the scanner gave every listing 120 s, and an FTP listing without
// MLSD cost a round trip per entry: a folder of 3,000 files at 50 ms could
// never be listed, and a copy into it was refused. FTP gives each read and
// write on its sockets a deadline of its own (L8), so the scanner adds
// none: a listing that is still coming is not cut. With -ftp-timeout 0
// there is no such deadline, and the scanner's cap stays.
func TestFTPListingThatMovesIsNotCut(t *testing.T) {
	SetMaxRetries(0)
	t.Cleanup(func() { SetMaxRetries(5) })
	setListTimeout(t, 300*time.Millisecond)
	files, failed := scanRight(t.TempDir(), lazyFakeFTP(t, pacedFolder(30, 25*time.Millisecond), 1))
	if failed || files != 30 {
		t.Errorf("30 lines 25 ms apart, with 300 ms for a listing: %d files, failed %v; want 30", files, failed)
	}

	setFTPIdle(t, 0)
	files, failed = scanRight(t.TempDir(), lazyFakeFTP(t, pacedFolder(30, 25*time.Millisecond), 1))
	if !failed {
		t.Errorf("with -ftp-timeout 0 the scanner's 300 ms for a listing did not hold: %d files listed", files)
	}
}

// With no cap from the scanner, a listing that stops still ends: after
// -ftp-timeout, as a scan of it did before the 120 s.
func TestFTPListingThatStopsFailsByTheFTPTimeout(t *testing.T) {
	SetMaxRetries(0)
	t.Cleanup(func() { SetMaxRetries(5) })
	setListTimeout(t, time.Hour)
	setFTPIdle(t, 300*time.Millisecond)
	f := pacedFolder(3, 0)
	f.listStall = true
	b, local := lazyFakeFTP(t, f, 1), t.TempDir()
	done := make(chan bool, 1)
	go func() {
		_, failed := scanRight(local, b)
		done <- failed
	}()
	select {
	case failed := <-done:
		if !failed {
			t.Error("a listing that stopped did not fail")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a scan of a listing that stopped still runs after 30 s, with a 300 ms -ftp-timeout")
	}
}
