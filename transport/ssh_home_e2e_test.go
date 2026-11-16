package transport

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

// S2: a URL without a path means the login home, which sc asks the server
// for. An answer that is not a folder path used to become the base anyway
// (an empty one made every path relative to the remote shell's folder), and
// a lookup that failed fell back to "/". Now the connect fails and says to
// write the full path. The test server runs commands with this process's
// environment, so an empty HOME here is an empty answer there. Nothing is
// written or deleted: on the old code the base is wrong.
func TestSSHHomeLookupFailureFailsConnect(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	t.Setenv("HOME", "")
	addr := startExecSSHServer(t)
	if b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+addr+"/", true, 1); err == nil {
		t.Errorf("rsync+ssh with no home folder: connected with base %q", b.base)
		b.Close()
	} else if !strings.Contains(err.Error(), "full remote path") {
		t.Errorf("rsync+ssh with no home folder: %v; want it to say to write the full path", err)
	}
	if b, err := OpenBackend("ssh://x:pw@"+addr, true, 1); err == nil {
		if s, ok := b.(*SCPBackend); ok {
			t.Errorf("ssh (shell) with no home folder: connected with base %q", s.base)
		} else {
			t.Errorf("ssh with no home folder: connected as %T", b)
		}
		CloseBackend(b)
	} else if !strings.Contains(err.Error(), "full remote path") {
		t.Errorf("ssh (shell) with no home folder: %v; want it to say to write the full path", err)
	}
	if b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+addr+"/srv/data", true, 1); err != nil {
		t.Errorf("rsync+ssh with a full path needs no home folder: %v", err)
	} else {
		b.Close()
	}
}
