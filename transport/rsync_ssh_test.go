package transport

import (
	"testing"

	"sc/model"
)

func TestInvalidateAfterTreeSendClearsParentAndSubtree(t *testing.T) {
	c := newListCache()
	for _, dir := range []string{"", "mydir", "mydir/sub", "elsewhere"} {
		c.emit(dir, []model.FileEntry{{Name: "x"}})
	}
	b := &RsyncSSHBackend{
		sshShell: sshShell{listCache: c},
		md4:      md4Cache{m: map[string]string{"mydir/f1": "h1", "elsewhere/f2": "h2"}},
	}

	b.invalidateAfterTreeSend("mydir")

	for _, dir := range []string{"", "mydir", "mydir/sub"} {
		if _, hit, _, _ := c.lookup(dir); hit {
			t.Errorf("listing %q must be invalidated after tree send", dir)
		}
	}
	if _, hit, _, _ := c.lookup("elsewhere"); !hit {
		t.Error("unrelated listing must survive a scoped tree send")
	}
	if _, ok := b.md4.m["mydir/f1"]; ok {
		t.Error("md4 under sent tree must be dropped")
	}
	if _, ok := b.md4.m["elsewhere/f2"]; !ok {
		t.Error("md4 outside sent tree must be preserved")
	}
}
