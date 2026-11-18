package ui

import (
	"strings"
	"testing"
)

// U19: the y dialog shows a password as xxxxx, and gave the real one back
// only for text left as it was: a changed path sent xxxxx as the password.
// Now the password goes back into the text as edited; for another place,
// a password left as xxxxx is refused, and the side stays as it was.
func TestOpenDialogPutsTheHiddenPasswordBack(t *testing.T) {
	m := scannedModel(t, "fake://u:secret@tiny/a", "fake://tiny")
	m.left.arg = "fake://u:secret@tiny/a" // as main gives it: the argument as typed
	press(m, "y")
	if l, _ := m.openDlg.Values(); l != "fake://u:xxxxx@tiny/a" {
		t.Fatalf("the dialog shows %q; want the password masked", l)
	}
	press(m, "esc")
	left := m.left.backend
	if _, errMsg := m.reopenBackends("fake://u:xxxxx@small/a", m.right.arg); !strings.HasPrefix(errMsg, "left: ") || m.left.backend != left {
		t.Errorf("a masked password for another place: err=%q replaced=%v; want it refused", errMsg, m.left.backend != left)
	}
	if _, errMsg := m.reopenBackends("fake://u:xxxxx@tiny/b", m.right.arg); errMsg != "" {
		t.Fatalf("reopen: %s", errMsg)
	}
	if m.left.arg != "fake://u:secret@tiny/b" {
		t.Errorf("the left side opened %q; want the edited path with the real password", m.left.arg)
	}
	m.cancelOps()
}
