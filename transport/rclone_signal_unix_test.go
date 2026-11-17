//go:build unix

package transport

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/rclone/rclone/lib/atexit"
)

// rclone's SIGINT/SIGTERM handler, installed by the first copy, multipart
// upload or upload batch, ended the process with the terminal still raw
// (L13). sc's rclone installs none: a signal stays bubbletea's.
func TestRcloneInstallsNoSignalHandler(t *testing.T) {
	if rcloneChild(t) != "" {
		if _, err := NewRcloneBackend(context.Background(), "rclone://:memory:bkt", false); err != nil {
			t.Fatal(err)
		}
		atexit.Register(func() {}) // as rclone's copies, multipart uploads and batches do
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM) // as bubbletea does
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		<-sig
		time.Sleep(500 * time.Millisecond) // rclone's handler ends the process within this
		return
	}
	conf := filepath.Join(t.TempDir(), "rclone.conf") // none: rclone's defaults
	if r := runRcloneChild(t, "child", conf); !r.passed(t) {
		t.Fatalf("SIGTERM after sc set rclone up (143 is rclone's exit): %v", r)
	}
}
