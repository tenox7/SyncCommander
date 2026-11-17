//go:build unix

package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
)

// fullListener returns the port of a listening socket whose accept queue
// is full: a dial to it gets no answer to its SYN, and waits.
func fullListener(t *testing.T) int {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Skip(err)
	}
	if err := syscall.Listen(fd, 0); err != nil {
		t.Skip(err)
	}
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Skip(err)
	}
	port := sa.(*syscall.SockaddrInet4).Port
	for range 1024 {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 300*time.Millisecond)
		if ne := net.Error(nil); errors.As(err, &ne) && ne.Timeout() {
			return port
		}
		if err != nil {
			t.Skipf("a dial to a full accept queue: %v; want it to wait", err)
		}
		t.Cleanup(func() { c.Close() })
	}
	t.Skip("the accept queue does not fill here")
	return 0
}

// L8: a cancel ends a data connection being dialed. It was dialed with no
// ctx, so a data port that takes no connection held the call for the
// dial's 10 s, cancelled or not.
func TestFTPCancelEndsADataDial(t *testing.T) {
	port := fullListener(t)
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			f := &fakeFTPD{epsvPort: port, files: map[string]fakeFTPFile{"/a.bin": {"x", "226 Transfer complete."}}}
			s := openStallFTP(t, f, parallel)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := goErr(func() error {
				r, err := s.Open(ctx, "a.bin")
				if r != nil {
					r.Close()
				}
				return err
			})
			time.Sleep(300 * time.Millisecond)
			cancel()
			if err := waitErr(t, done, 3*time.Second, "a cancelled data dial"); err == nil {
				t.Error("a cancelled data dial opened")
			}
		})
	}
}
