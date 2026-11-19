package transport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"

	"sc/model"
)

// askSum asks b for name's algo sum with 3 s to answer, and says how long
// the answer took.
func askSum(b model.Backend, name, algo string) (string, error, time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t0 := time.Now()
	sum, err := b.Checksum(ctx, name, algo)
	return sum, err, time.Since(t0)
}

// wantNoSum checks an answer of no sum: final, at once.
func wantNoSum(t *testing.T, b model.Backend, name, algo string) {
	t.Helper()
	sum, err, took := askSum(b, name, algo)
	if err == nil || !errors.Is(err, ErrUnsupported) || took > 2*time.Second {
		t.Errorf("%s of %s = %q, %v after %v; want no sum, a final answer at once", algo, name, sum, err, took.Round(time.Millisecond))
	}
}

// logCount counts the log lines from mark on that hold every one of subs.
func logCount(mark int, subs ...string) int {
	n := 0
	for _, l := range Log.Lines()[mark:] {
		all := true
		for _, s := range subs {
			all = all && strings.Contains(l, s)
		}
		if all {
			n++
		}
	}
	return n
}

// L12: a file the daemon gives no md4 for, as one it cannot read (stock
// rsync sends a sum of zeros), has none on the next try either. The answer
// was taken for a passing failure: six dry runs and some 15 s of waits a
// file. Through the wrapper every remote goes through it is one ask now,
// at once, and one warning. So is an algorithm a daemon has not.
func TestRsyncFileWithNoSumIsOneAsk(t *testing.T) {
	port, dir := startRsyncDaemon(t)
	for _, n := range []string{"a.txt", "locked.txt", "locked2.txt"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("abc"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []string{"locked.txt", "locked2.txt"} {
		p := filepath.Join(dir, n)
		if err := os.Chmod(p, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(p, 0o644) })
	}
	if f, err := os.Open(filepath.Join(dir, "locked.txt")); err == nil {
		f.Close()
		t.Skip("this user reads a file of mode 000")
	}
	url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod/", port)
	b := NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
	defer CloseBackend(b)
	mark := len(Log.Lines())
	if sum, err, _ := askSum(b, "a.txt", "md4"); err != nil || len(sum) != 32 {
		t.Fatalf("md4 of a.txt = %q, %v; want its sum", sum, err)
	}
	wantNoSum(t, b, "locked.txt", "md4")
	wantNoSum(t, b, "locked2.txt", "md4")
	wantNoSum(t, b, "a.txt", "md5")
	if n := logCount(mark, " >>> ", "MD4 testmod/locked.txt"); n != 1 {
		t.Errorf("locked.txt asked %d times; want 1", n)
	}
	if n := logCount(mark, " rsync WARN ", "no md4 checksum"); n != 1 {
		t.Errorf("%d warnings of a file with no sum; want 1", n)
	}
}

// noSumFs is an rclone remote whose objects named in none have no sum:
// their Hash gives "" with the error none holds, as S3 gives for an object
// of a multipart upload that was given no sum.
type noSumFs struct {
	fs.Fs
	none    map[string]error
	lookups atomic.Int64
}

func (f *noSumFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	f.lookups.Add(1)
	o, err := f.Fs.NewObject(ctx, remote)
	if e, ok := f.none[remote]; ok && err == nil {
		return noSumObject{Object: o, err: e}, nil
	}
	return o, err
}

type noSumObject struct {
	fs.Object
	err error
}

func (o noSumObject) Hash(context.Context, hash.Type) (string, error) { return "", o.err }

// L12: an object with no sum, as an S3 object of a multipart upload made
// with none, was asked six times, with some 15 s of waits; so was an
// algorithm the remote has none of. Each is one final answer now, and one
// warning says so.
func TestRcloneFileWithNoSumIsOneAsk(t *testing.T) {
	b, _ := rcloneWithFiles(t, "l12nosum", []string{"a.txt", "multi.bin", "multi2.bin", "crypt.bin"})
	nf := &noSumFs{Fs: b.f, none: map[string]error{"multi.bin": nil, "multi2.bin": nil, "crypt.bin": hash.ErrUnsupported}}
	b.f = nf
	for _, n := range []string{"a.txt", "multi.bin", "multi2.bin", "crypt.bin"} {
		b.objs.drop(n)
	}
	lb := NewLazyBackend("rclone://:memory:l12nosum", func(context.Context) (model.Backend, error) { return b, nil })
	defer CloseBackend(lb)
	mark := len(Log.Lines())
	if sum, err, _ := askSum(lb, "a.txt", "md5"); err != nil || sum != md5Hex("a.txt") {
		t.Fatalf("md5 of a.txt = %q, %v; want %s", sum, err, md5Hex("a.txt"))
	}
	wantNoSum(t, lb, "multi.bin", "md5")
	wantNoSum(t, lb, "multi2.bin", "md5")
	wantNoSum(t, lb, "crypt.bin", "md5")
	wantNoSum(t, lb, "a.txt", "sha1")
	if n := nf.lookups.Load(); n != 4 {
		t.Errorf("%d lookups for 4 objects; want 4", n)
	}
	if n := logCount(mark, " rclone WARN "); n != 1 {
		t.Errorf("%d warnings of an object with no sum; want 1", n)
	}
	if n := logCount(mark, " RETRY "); n != 0 {
		t.Errorf("%d tries again; want none", n)
	}
}

// L12: a sum answer that holds no sum of the algorithm asked is the same
// on the next try: each file was asked six times, with some 15 s of waits.
// Through the wrapper it is one ask a file now, and one warning.
func TestFTPAnswerWithNoSumIsOneAsk(t *testing.T) {
	f := hashFTPD()
	f.sumReply = "Done."
	url := "ftp://u:p@" + f.start(t) + "/"
	b := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) { return NewFTPBackend(ctx, url, false, 1) })
	defer CloseBackend(b)
	mark := len(Log.Lines())
	wantNoSum(t, b, "a.txt", "sha256")
	wantNoSum(t, b, "b.txt", "sha256")
	wantNoSum(t, b, "a.txt", "md5") // a command the server has not
	f.mu.Lock()
	asked := 0
	for _, lines := range f.heard {
		for _, l := range lines {
			if strings.HasPrefix(l, "XSHA256 ") {
				asked++
			}
		}
	}
	f.mu.Unlock()
	if asked != 2 {
		t.Errorf("%d sums asked for 2 files; want 2", asked)
	}
	if n := logCount(mark, " ftp WARN ", "no sha256 sum"); n != 1 {
		t.Errorf("%d warnings of an answer with no sum; want 1", n)
	}
}
