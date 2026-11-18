package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"sc/model"
)

// S18: a command that ran when its connection ended stopped with no exit
// status, which sc did not take for a lost connection: a recursive
// listing cut off so kept nothing of what it had listed in full. Now it
// is one. A command its ctx stopped is not, and ends at once.
func TestShellCommandOnALostConnectionSaysSo(t *testing.T) {
	addr := startExecSSHServer(t)
	b, err := OpenBackend("scp://x:pw@"+addr+t.TempDir(), true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	sh := &b.(*SCPBackend).sshShell

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	if _, err := sh.run(ctx, "sleep 2"); err == nil || errors.Is(err, model.ErrConnLost) || time.Since(start) > 800*time.Millisecond {
		t.Errorf("a command stopped by its ctx = %v after %v; want it stopped at once, and no lost connection", err, time.Since(start).Round(time.Millisecond))
	}

	time.AfterFunc(200*time.Millisecond, func() { sh.client.Close() })
	if _, err := sh.run(context.Background(), "sleep 2"); !errors.Is(err, model.ErrConnLost) || !isConnLost(err) || !cutOff(err) {
		t.Errorf("a command whose connection closed under it = %v; want a lost connection", err)
	}
}
