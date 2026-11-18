package transport

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"sc/internal/sshtest"
	"sc/model"
)

// S18: a run its ssh connection cut off runs again on the backend of the
// next connection, which goes on with the cache. The folders the cut run
// listed in full are kept, not added twice, and a waiter for the rest
// gets them from the new run.
func TestListCacheResumeRunsACutRunAgain(t *testing.T) {
	c := newListCache()
	c.start(context.Background(), "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}, {Name: "2"}})
		complete("a")
		emit("b", []model.FileEntry{{Name: "3"}})
		return fmt.Errorf("find: %w", model.ErrConnLost)
	})
	waitDone(t, c)
	gate := make(chan struct{})
	c.resume("test", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}, {Name: "2"}})
		complete("a")
		<-gate
		emit("b", []model.FileEntry{{Name: "3"}, {Name: "4"}})
		complete("b")
		return nil
	})
	b := serveAsync(c, "b")
	mustBlock(t, b)
	close(gate)
	mustServe(t, b, 2, false)
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 2, false)
}

// S18: a run still going on the old connection when the backend of the
// new one takes the cache over is stopped, and what it sends after that is
// dropped; the new run lists the rest.
func TestListCacheResumeTakesOverARunStillGoing(t *testing.T) {
	c := newListCache()
	sent, stopped := make(chan struct{}), make(chan struct{})
	c.start(context.Background(), "", func(ctx context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		emit("a", []model.FileEntry{{Name: "1"}})
		complete("a")
		close(sent)
		<-ctx.Done()
		emit("b", []model.FileEntry{{Name: "late"}})
		complete("b")
		close(stopped)
		return ctx.Err()
	})
	<-sent
	c.resume("test", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
		<-stopped
		emit("a", []model.FileEntry{{Name: "1"}})
		complete("a")
		emit("b", []model.FileEntry{{Name: "3"}, {Name: "4"}})
		complete("b")
		return nil
	})
	waitDone(t, c)
	mustServe(t, serveAsync(c, "a"), 1, false)
	mustServe(t, serveAsync(c, "b"), 2, false)
}

// S18: resume leaves alone a run that ended whole, one that failed on what
// the server answered, which would fail the same way again, and one whose
// scan has ended.
func TestListCacheResumeLeavesAloneWhatItCannotBetter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		end    error
		cancel bool
	}{
		{"whole", nil, false},
		{"answer", fmt.Errorf("listing: %w", ErrIncompleteListing), false},
		{"scan ended", fmt.Errorf("find: %w", model.ErrConnLost), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newListCache()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.start(ctx, "", func(_ context.Context, _ string, emit func(string, []model.FileEntry), complete func(string)) error {
				emit("a", []model.FileEntry{{Name: "1"}})
				complete("a")
				return tc.end
			})
			waitDone(t, c)
			if tc.cancel {
				cancel()
			}
			c.resume("test", func(context.Context, string, func(string, []model.FileEntry), func(string)) error {
				t.Error("ran again")
				return nil
			})
			c.mu.Lock()
			runs := c.runs
			c.mu.Unlock()
			if runs != 1 {
				t.Errorf("%d runs; want the first alone", runs)
			}
		})
	}
}

// cacheOf and clientOf reach into the backend of an ssh connection.
func cacheOf(b model.Backend) *listCache {
	switch b := b.(type) {
	case *SCPBackend:
		return b.listCache
	case *SFTPBackend:
		return b.listCache
	case *RsyncSSHBackend:
		return b.listCache
	}
	return nil
}

func clientOf(b model.Backend) *ssh.Client {
	switch b := b.(type) {
	case *SCPBackend:
		return b.client
	case *SFTPBackend:
		return b.client
	case *RsyncSSHBackend:
		return b.client
	}
	return nil
}

// reconnectTree is the tree the reconnect tests list.
func reconnectTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, p := range []string{"docs/a.txt", "deep/x/y.txt", "top.txt"} {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// mustListFromCache lists each of dirs through b and fails on any live
// listing of a folder under root that it sends.
func mustListFromCache(t *testing.T, b model.Backend, root string, dirs map[string][]string) {
	t.Helper()
	ctx := context.Background()
	mark := len(Log.Lines())
	for dir, want := range dirs {
		if names, err := listNames(ctx, b, dir); err != nil || !slices.Equal(names, want) {
			t.Errorf("List(%q) after the reconnect = %q, %v; want %q", dir, names, err, want)
		}
	}
	for _, line := range Log.Lines()[mark:] {
		if strings.Contains(line, ">>> ") && (strings.Contains(line, root+"/deep") || strings.Contains(line, root+"/docs")) {
			t.Errorf("listed live after the reconnect: %s", line)
		}
	}
}

// S18: a recursive listing its connection cut off runs again on the next
// connection, and the scan gets its folders from that run. The server
// holds the first try of the listing until its connection is closed.
func TestReconnectRunsACutListingAgain(t *testing.T) {
	root := reconnectTree(t)
	for _, scheme := range []string{"scp", "sftp", "rsync+ssh"} {
		t.Run(scheme, func(t *testing.T) {
			if _, err := exec.LookPath("rsync"); err != nil && scheme == "rsync+ssh" {
				t.Skip("no rsync binary")
			}
			var tries atomic.Int32
			holding := make(chan struct{})
			addr := sshtest.StartWith(t, sshtest.Options{SFTP: scheme == "sftp", Hold: func(cmd string) bool {
				deep := strings.Contains(cmd, "-ignore_readdir_race -mindepth 1") || strings.HasSuffix(cmd, "' 1") || strings.HasPrefix(cmd, "rsync ")
				if !deep || tries.Add(1) != 1 {
					return false
				}
				close(holding)
				return true
			}})
			b, err := TryOpenBackend(scheme+"://x:pw@"+addr+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			lazy := b.(*lazyBackend)
			ctx := context.Background()
			if err := lazy.PreloadRecursive(ctx, ""); err != nil {
				t.Fatal(err)
			}
			select {
			case <-holding:
			case <-time.After(10 * time.Second):
				t.Fatal("the recursive listing never reached the server")
			}
			first := lazy.current()
			clientOf(first).Close()
			if _, err := b.Stat(ctx, "top.txt"); err != nil {
				t.Fatal(err)
			}
			if lazy.current() == first {
				t.Fatal("no new connection")
			}
			mustListFromCache(t, b, root, map[string][]string{"deep/x": {"y.txt"}, "docs": {"a.txt"}})
		})
	}
}

// S18: a lost connection dropped the ssh backend, and its list cache with
// it: the rest of a scan listed each folder by itself. The backend of the
// new connection goes on with the cache, so a folder the recursive listing
// read in full is served from it.
func TestReconnectGoesOnWithTheListCache(t *testing.T) {
	root := reconnectTree(t)
	servers := map[string]string{"scp": startExecSSHServer(t), "sftp": startExecSFTPServer(t)}
	servers["rsync+ssh"] = servers["scp"]
	for _, scheme := range []string{"scp", "sftp", "rsync+ssh"} {
		t.Run(scheme, func(t *testing.T) {
			if _, err := exec.LookPath("rsync"); err != nil && scheme == "rsync+ssh" {
				t.Skip("no rsync binary")
			}
			b, err := TryOpenBackend(scheme+"://x:pw@"+servers[scheme]+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			lazy := b.(*lazyBackend)
			ctx := context.Background()
			if err := lazy.PreloadRecursive(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if names, err := listNames(ctx, b, "docs"); err != nil || !slices.Equal(names, []string{"a.txt"}) {
				t.Fatalf("List(docs) = %q, %v; want [a.txt]", names, err)
			}
			first := lazy.current()
			waitDone(t, cacheOf(first))
			clientOf(first).Close()
			// A call that goes to the server finds the connection lost,
			// and its next try connects again.
			if _, err := b.Stat(ctx, "top.txt"); err != nil {
				t.Fatal(err)
			}
			if lazy.current() == first {
				t.Fatal("no new connection")
			}
			mustListFromCache(t, b, root, map[string][]string{"deep/x": {"y.txt"}})
		})
	}
}
