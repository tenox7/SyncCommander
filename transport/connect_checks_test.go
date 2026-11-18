package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"sc/internal/sshtest"
)

// shellCheck is a command the connect of scheme runs on the login shell,
// with a remote path that has the connect run it.
type shellCheck struct{ scheme, cmd, path string }

func shellChecks(t *testing.T) []shellCheck {
	dir := t.TempDir()
	return []shellCheck{
		{"rsync+ssh", "echo " + shellWord, dir},
		{"rsync+ssh", "echo $HOME", "/"},
		{"rsync+ssh", "command -v rsync", dir},
		{"scp", "echo " + shellWord, dir},
		{"scp", "echo $HOME", "/"},
	}
}

// connectHeld connects as c says to a server that holds c.cmd, as a shell
// on a hung mount does. It returns once the server has the command, with
// the channel that gets the connect's error.
func connectHeld(t *testing.T, ctx context.Context, c shellCheck) <-chan error {
	t.Helper()
	held := make(chan struct{}, 1)
	addr := sshtest.StartWith(t, sshtest.Options{Hold: func(cmd string) bool {
		if cmd != c.cmd {
			return false
		}
		select {
		case held <- struct{}{}:
		default:
		}
		return true
	}})
	done := make(chan error, 1)
	go func() {
		b, err := openBackend(ctx, c.scheme+"://x:pw@"+addr+c.path, true, 1)
		if err == nil {
			CloseBackend(b)
		}
		done <- err
	}()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("the connect ended before the server got %q: %v", c.cmd, err)
	case <-time.After(20 * time.Second):
		t.Fatalf("the server never got %q", c.cmd)
	}
	return done
}

// R17: the connect of rsync+ssh:// asked the login shell for rsync with no
// context and no deadline. A shell whose PATH is on a hung mount never
// answers, while the server still answers keepalives: the connect hung,
// and a cancel did not stop it. The shell's echo and $HOME had a deadline
// but no cancel. Now each check ends on the cancel.
func TestTheConnectsShellChecksEndOnACancel(t *testing.T) {
	for _, c := range shellChecks(t) {
		t.Run(c.scheme+" "+c.cmd, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := connectHeld(t, ctx, c)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("err = %v, want the cancel", err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("the connect still waits for %q 5s after the cancel", c.cmd)
			}
		})
	}
}

// R17: and each fails when the shell does not answer in time, saying so.
func TestTheConnectsShellChecksEndOnTheirDeadline(t *testing.T) {
	old := shellCheckTimeout
	shellCheckTimeout = time.Second
	t.Cleanup(func() { shellCheckTimeout = old })
	for _, c := range shellChecks(t) {
		t.Run(c.scheme+" "+c.cmd, func(t *testing.T) {
			done := connectHeld(t, context.Background(), c)
			select {
			case err := <-done:
				if !errors.Is(err, errShellSilent) {
					t.Errorf("err = %v, want that the shell did not answer", err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("the connect still waits for %q after 10s", c.cmd)
			}
		})
	}
}
