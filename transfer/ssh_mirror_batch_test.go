package transfer

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sc/internal/sshtest"
	"sc/transport"
)

// R16: a mirror to an ssh destination deleted each entry with a command of
// its own, a round trip each: 100k deletes were 100k commands. A folder's
// deletes now go in one script, sh with the names on its input.
func TestSSHMirrorDeletesAFolderInOneCommand(t *testing.T) {
	for _, tc := range []struct {
		scheme string
		sftp   bool
	}{{"scp", false}, {"sftp", true}, {"rsync+ssh", false}} {
		t.Run(tc.scheme, func(t *testing.T) {
			if _, err := exec.LookPath("rsync"); tc.scheme == "rsync+ssh" && err != nil {
				t.Skip("no rsync binary")
			}
			var mu sync.Mutex
			single := 0
			addr := sshtest.StartWith(t, sshtest.Options{SFTP: tc.sftp, Hold: func(cmd string) bool {
				if strings.HasPrefix(cmd, "rm ") || strings.HasPrefix(cmd, "exec 3<&0; rm -rf ") {
					mu.Lock()
					single++
					mu.Unlock()
				}
				return false
			}})
			remote, local := t.TempDir(), t.TempDir()
			const dir = "r16-ssh-" + "mirror"
			writeFile(t, filepath.Join(local, dir, "keep.txt"), "k")
			writeFile(t, filepath.Join(remote, dir, "keep.txt"), "k")
			for i := range 30 {
				writeFile(t, filepath.Join(remote, dir, fmt.Sprintf("x %02d.txt", i)), "d")
			}
			writeFile(t, filepath.Join(remote, dir, "sub", "deep.txt"), "d")
			s := sshScenario(t, tc.scheme+"://x:pw@"+addr+remote, remote, local)
			mark := len(transport.Log.Lines())
			p := s.copy(t, dir, false, true)
			if f := p.Failed.Load(); f != 0 {
				t.Errorf("%d failed", f)
			}
			wantTree(t, "after the mirror", filepath.Join(remote, dir), "keep.txt")
			scripts := 0
			for _, l := range transport.Log.Lines()[mark:] {
				if strings.Contains(l, " >>> REMOVE ") && strings.Contains(l, dir) {
					scripts++
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if scripts != 1 || single != 0 {
				t.Errorf("31 deletes in %d scripts and %d commands of one; want 1 and 0", scripts, single)
			}
		})
	}
}
