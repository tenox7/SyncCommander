package transport

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// checksumWord names a file's checksum status in a failure message.
func checksumWord(s model.AttrStatus) string {
	switch s {
	case model.AttrUnknown:
		return "not compared"
	case model.AttrScanning:
		return "being summed"
	case model.AttrEqual:
		return "equal"
	case model.AttrDifferent:
		return "different"
	}
	return "not applicable"
}

// R11: the rsync backends kept every md4 sum a file list gave, with no size
// or time, until sc itself wrote the file. After a checksum scan, a file
// someone else changed on the server, same size, kept its old sum: summed
// again by itself, or rescanned, it showed "checksum equal" to the other
// side. A bulk fetch now gives its sums to the scan that asked, and the
// backend keeps none. The scan still sums a folder in one run.
//
// The daemon is checked against a local folder, which sums md4 too, and
// rsync+ssh against the daemon, so md4 is the sum both offer: on its own
// rsync+ssh would pick the shell's sha256.
func TestRsyncChecksumOfAFileChangedOnTheServerIsCurrent(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	write := func(t *testing.T, root, rel, body string, mtime time.Time) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	local := func(t *testing.T, dir string) model.Backend { return NewLocalBackend(dir) }
	for _, tc := range []struct {
		name string
		// left opens the left side on dir; the files change on the left
		// when changeLeft, else on the daemon's side. wrapped puts the
		// daemon behind the connection wrapper, as sc opens it.
		left       func(t *testing.T, dir string) model.Backend
		changeLeft bool
		wrapped    bool
	}{
		{"rsync daemon", local, false, false},
		{"rsync daemon, wrapped", local, false, true},
		{"rsync+ssh", func(t *testing.T, dir string) model.Backend {
			if _, err := exec.LookPath("rsync"); err != nil {
				t.Skip("no rsync binary")
			}
			b, err := NewRsyncSSHBackend(context.Background(), "rsync+ssh://x:pw@"+startExecSSHServer(t)+dir, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { b.Close() })
			return b
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port, moduleDir, calls := startArgLoggingDaemon(t)
			url := fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port)
			var right model.Backend
			if tc.wrapped {
				right = NewLazyBackend(url, func(context.Context) (model.Backend, error) { return NewRsyncBackend(url) })
			} else {
				rb, err := NewRsyncBackend(url)
				if err != nil {
					t.Fatal(err)
				}
				right = rb
			}
			leftDir := t.TempDir()
			for rel, body := range map[string]string{"d/f.txt": "same body", "d/g.txt": "other body", "top.txt": "top body"} {
				write(t, leftDir, rel, body, at)
				write(t, moduleDir, rel, body, at)
			}
			ctx := context.Background()
			opts := model.CompareOpts{Size: true, ModTime: true, Checksum: true}
			sc := model.NewScanner(tc.left(t, leftDir), right, 1, 1, true)
			sc.Scan(ctx, opts)
			if algo := sc.ChecksumAlgo(); algo != "md4" {
				t.Fatalf("the scan summed with %q, want md4", algo)
			}

			nodes := map[string]*model.TreeNode{}
			status := func() map[string]model.AttrStatus {
				st := map[string]model.AttrStatus{}
				var walk func(n *model.TreeNode)
				walk = func(n *model.TreeNode) {
					if !n.IsDir {
						nodes[n.RelPath], st[n.RelPath] = n, n.Compare.Checksum
					}
					for _, c := range n.Children {
						walk(c)
					}
				}
				sc.ReadTree(func(root *model.TreeNode) { walk(root) })
				return st
			}
			for rel, st := range status() {
				if st != model.AttrEqual {
					t.Fatalf("after the scan %s is %s by checksum, want equal", rel, checksumWord(st))
				}
			}
			var sums [][]string
			recursive := 0
			for _, c := range calls() {
				if f := shortFlags(c); strings.ContainsRune(f, 'c') && strings.ContainsRune(f, 'n') {
					sums = append(sums, c[1:])
					if strings.ContainsRune(f, 'r') {
						recursive++
					}
				}
			}
			if len(sums) != 2 || recursive != 1 {
				t.Errorf("the scan asked the daemon for sums %d times, %q; want twice: d/ in one run, top.txt by itself", len(sums), sums)
			}

			// Someone else changes two files on one server: same size, new
			// body.
			changed := moduleDir
			if tc.changeLeft {
				changed = leftDir
			}
			later := at.Add(time.Hour)
			write(t, changed, "d/f.txt", "SAME BODY", later)
			write(t, changed, "top.txt", "TOP BODY", later)

			sc.ChecksumNode(ctx, nodes["d/f.txt"])
			if st := status()["d/f.txt"]; st != model.AttrDifferent {
				t.Errorf("d/f.txt, changed on the server and summed again by itself, is %s by checksum, want different", checksumWord(st))
			}
			sc.RescanNode(ctx, nodes["top.txt"], opts, nil)
			if st := status()["top.txt"]; st != model.AttrDifferent {
				t.Errorf("top.txt, changed on the server and rescanned, is %s by checksum, want different", checksumWord(st))
			}
			if st := status()["d/g.txt"]; st != model.AttrEqual {
				t.Errorf("d/g.txt, not changed, is %s by checksum, want equal", checksumWord(st))
			}
		})
	}
}
