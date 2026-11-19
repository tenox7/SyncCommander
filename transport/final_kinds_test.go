package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"

	"sc/model"
)

// goneFs is an rclone remote whose objects named in gone went since they
// were looked up: an open finds nothing, as a GET finds nothing on S3 for
// a key another client deleted.
type goneFs struct {
	fs.Fs
	gone map[string]bool
}

func (f *goneFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.Fs.NewObject(ctx, remote)
	if err == nil && f.gone[remote] {
		return goneObject{o}, nil
	}
	return o, err
}

type goneObject struct{ fs.Object }

func (goneObject) Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error) {
	return nil, fs.ErrorObjectNotFound
}

// finalAt runs op with 3 s to answer and checks it failed at once, with
// an error of kind.
func finalAt(t *testing.T, what string, kind error, op func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t0 := time.Now()
	err := op(ctx)
	if took := time.Since(t0); err == nil || !errors.Is(err, kind) || took > 2*time.Second {
		t.Errorf("%s: %v after %v; want %v, at once", what, err, took.Round(time.Millisecond), kind)
	}
}

func openOnce(b model.Backend, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		rc, err := b.Open(ctx, name)
		if err == nil {
			rc.Close()
		}
		return err
	}
}

// L12: rclone's "object not found" read as a passing failure: a file gone
// since the scan was asked for six times, with some 15 s of waits. It is
// not there now, a final answer, from a lookup and from an open alike.
func TestRcloneGoneObjectIsOneAsk(t *testing.T) {
	b, _ := rcloneWithFiles(t, "l12gone", []string{"a.txt", "b.txt"})
	ctx := context.Background()
	o, err := b.f.NewObject(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	b.f = &goneFs{Fs: b.f, gone: map[string]bool{"b.txt": true}}
	for _, n := range []string{"a.txt", "b.txt"} {
		b.objs.drop(n)
	}
	lb := NewLazyBackend("rclone://:memory:l12gone", func(context.Context) (model.Backend, error) { return b, nil })
	defer CloseBackend(lb)
	mark := len(Log.Lines())
	finalAt(t, "open a.txt", os.ErrNotExist, openOnce(lb, "a.txt"))
	finalAt(t, "md5 of a.txt", os.ErrNotExist, func(ctx context.Context) error {
		_, err := lb.Checksum(ctx, "a.txt", "md5")
		return err
	})
	finalAt(t, "open b.txt", os.ErrNotExist, openOnce(lb, "b.txt"))
	for _, off := range []int64{0, 1} {
		finalAt(t, fmt.Sprintf("open b.txt at %d", off), os.ErrNotExist, func(ctx context.Context) error {
			rc, err := b.OpenAt(ctx, "b.txt", off)
			if err == nil {
				rc.Close()
			}
			return err
		})
	}
	if n := logCount(mark, " RETRY "); n != 0 {
		t.Errorf("%d tries again; want none", n)
	}
}

// rcloneErr keeps the message and adds the kind.
func TestRcloneErrorsHaveKinds(t *testing.T) {
	plain := errors.New("x")
	for _, c := range []struct {
		err, kind error
	}{
		{fs.ErrorObjectNotFound, os.ErrNotExist},
		{fmt.Errorf("lookup: %w", fs.ErrorDirNotFound), os.ErrNotExist},
		{fs.ErrorPermissionDenied, os.ErrPermission},
		{plain, plain},
	} {
		got := rcloneErr(c.err)
		if !errors.Is(got, c.kind) || got.Error() != c.err.Error() {
			t.Errorf("rcloneErr(%v) = %v; want %v, with its message", c.err, got, c.kind)
		}
	}
	if rcloneErr(nil) != nil {
		t.Error("rcloneErr(nil) is not nil")
	}
}

// L12: an FTP reply is read by its class. 5yz is permanent (RFC 959):
// vsftpd's "550 Failed to open file." for a file gone since the scan was
// asked six times, some 15 s, because its words hold no "no such file".
// 4yz is a passing failure.
func TestFTPReplyClassIsFinal(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{&textproto.Error{Code: 550, Msg: "Failed to open file."}, true},
		{fmt.Errorf("XSHA256: %w", &textproto.Error{Code: 553, Msg: "Could not create file."}), true},
		{&textproto.Error{Code: 450, Msg: "Busy."}, false},
		{&textproto.Error{Code: 425, Msg: "Can't open data connection."}, false},
	} {
		if got := isPermanentError(c.err); got != c.want {
			t.Errorf("isPermanentError(%v) = %v; want %v", c.err, got, c.want)
		}
	}
}

// Through the wrapper every remote goes through: a download of a file gone
// since the scan is one RETR, and a refused OPTS HASH one ask and one
// warning, each a final answer.
func TestFTPPermanentReplyIsOneAsk(t *testing.T) {
	f := hashOnlyFTPD()
	f.hashRefuses = true
	url := "ftp://u:p@" + f.start(t) + "/"
	b := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) { return NewFTPBackend(ctx, url, false, 1) })
	defer CloseBackend(b)
	mark := len(Log.Lines())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t0 := time.Now()
	if rc, err := b.Open(ctx, "gone.txt"); err == nil {
		rc.Close()
		t.Error("gone.txt opened")
	} else if took := time.Since(t0); took > 2*time.Second {
		t.Errorf("open gone.txt: %v after %v; want a final answer at once", err, took.Round(time.Millisecond))
	}
	finalAt(t, "sha256 of a.txt", ErrUnsupported, func(ctx context.Context) error {
		_, err := b.Checksum(ctx, "a.txt", "sha256")
		return err
	})
	finalAt(t, "sha256 of b.txt", ErrUnsupported, func(ctx context.Context) error {
		_, err := b.Checksum(ctx, "b.txt", "sha256")
		return err
	})
	f.mu.Lock()
	retrs := 0
	for _, lines := range f.heard {
		for _, l := range lines {
			if strings.HasPrefix(l, "RETR ") {
				retrs++
			}
		}
	}
	f.mu.Unlock()
	if retrs != 1 {
		t.Errorf("%d RETRs for one download; want 1", retrs)
	}
	if n := logCount(mark, " ftp WARN ", "OPTS HASH"); n != 1 {
		t.Errorf("%d warnings of a refused OPTS HASH; want 1", n)
	}
}

// A sum's failure reply is read by its class too: "550 Failed to open
// file." to XSHA256 or HASH was asked six times, some 15 s a file. It is
// one ask now.
func TestFTPSumRefusalIsOneAsk(t *testing.T) {
	for _, c := range []struct {
		cmd string
		f   func() *fakeFTPD
	}{
		{"XSHA256", hashFTPD},
		{"HASH", hashOnlyFTPD},
	} {
		t.Run(c.cmd, func(t *testing.T) {
			f := c.f()
			f.sumFails = "550 Failed to open file."
			url := "ftp://u:p@" + f.start(t) + "/"
			b := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) { return NewFTPBackend(ctx, url, false, 1) })
			defer CloseBackend(b)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			t0 := time.Now()
			sum, err := b.Checksum(ctx, "a.txt", "sha256")
			if took := time.Since(t0); err == nil || ctx.Err() != nil || took > 2*time.Second {
				t.Errorf("sha256 of a.txt = %q, %v after %v; want a final answer at once", sum, err, took.Round(time.Millisecond))
			}
			f.mu.Lock()
			asked := 0
			for _, lines := range f.heard {
				for _, l := range lines {
					if strings.HasPrefix(l, c.cmd+" ") {
						asked++
					}
				}
			}
			f.mu.Unlock()
			if asked != 1 {
				t.Errorf("%s asked %d times; want 1", c.cmd, asked)
			}
		})
	}
}
