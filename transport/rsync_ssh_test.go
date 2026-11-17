package transport

import (
	"testing"

	"sc/model"
)

func TestInvalidateAfterTreeSendClearsParentAndSubtree(t *testing.T) {
	c := newListCache()
	for _, dir := range []string{"", "mydir", "mydir/sub", "elsewhere"} {
		c.emit(dir, []model.FileEntry{{Name: "x"}})
		c.markComplete(dir)
	}
	b := &RsyncSSHBackend{sshShell: sshShell{listCache: c}}

	b.invalidateAfterTreeSend("mydir")

	for _, dir := range []string{"", "mydir", "mydir/sub"} {
		if _, hit, _, _ := c.lookup(dir); hit {
			t.Errorf("listing %q must be invalidated after tree send", dir)
		}
	}
	if _, hit, _, _ := c.lookup("elsewhere"); !hit {
		t.Error("unrelated listing must survive a scoped tree send")
	}
}
