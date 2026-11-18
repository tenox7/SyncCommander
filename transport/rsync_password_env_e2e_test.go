package transport

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
)

// R22: sc's daemon login took the password from the URL only. rsync takes
// it from RSYNC_PASSWORD too, and a password with a slash in it cannot be
// written in the URL. Now sc takes it from there when the URL has none.
func TestRsyncDaemonPasswordFromTheEnvironment(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync binary")
	}
	const secret = "Sekr1t/R22:x"
	port, root := startRsyncDaemonConf(t, func(root string) string {
		secrets := filepath.Join(root, "secrets")
		if err := os.WriteFile(secrets, []byte("carol:"+secret+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return "[priv]\n  path = " + filepath.Join(root, "priv") + "\n  read only = no\n  auth users = carol\n  secrets file = " + secrets + "\n"
	})
	writeTestFile(t, filepath.Join(root, "priv"), "a.txt", "a")
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://carol@127.0.0.1:%d/priv", port))
	if err != nil {
		t.Fatal(err)
	}
	ctx := model.WithFreshListing(context.Background())
	t.Setenv("RSYNC_PASSWORD", "")
	if _, err := b.List(ctx, ""); err == nil {
		t.Error("a listing with no password worked")
	}
	t.Setenv("RSYNC_PASSWORD", secret)
	mark := len(Log.Lines())
	entries, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List with RSYNC_PASSWORD set: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "a.txt" {
		t.Errorf("List = %v, want a.txt", entries)
	}
	if err := b.Mkdir(context.Background(), "new", 0o755); err != nil {
		t.Errorf("Mkdir with RSYNC_PASSWORD set: %v", err)
	}
	for _, l := range Log.Lines()[mark:] {
		if strings.Contains(l, secret) {
			t.Errorf("the log holds the password: %s", l)
		}
	}
}
