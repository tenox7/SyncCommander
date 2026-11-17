package transfer

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// startResticServer serves root, a repository folder, over the restic REST
// API with rclone, as rclone serve restic does in use.
func startResticServer(t *testing.T, root string) string {
	t.Helper()
	bin, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone not available")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	cmd := exec.Command(bin, "serve", "restic", root, "--addr", addr, "--user", "u", "--pass", "p")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	for range 200 {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return "restic://u:p@" + addr + "/"
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rclone serve restic did not come up")
	return ""
}

// resticObject is one object of a repository in restic's layout on disk:
// data/<first two characters of its name>/<name>, the other types flat.
func resticObject(typ, body string) (rel string) {
	sum := sha256.Sum256([]byte(body))
	name := hex.EncodeToString(sum[:])
	if typ == "data" {
		return "data/" + name[:2] + "/" + name
	}
	return typ + "/" + name
}

// resticRepo writes a small repository into each of dirs, as restic keeps
// one on disk (and rclone serve restic serves it), and returns its paths.
func resticRepo(t *testing.T, dirs ...string) []string {
	t.Helper()
	objs := map[string]string{"config": "the repository's config"}
	for i, typ := range []string{"data", "data", "data", "index", "keys", "snapshots"} {
		body := fmt.Sprintf("%s object %d", typ, i)
		objs[resticObject(typ, body)] = body
	}
	var paths []string
	for rel := range objs {
		paths = append(paths, rel)
	}
	slices.Sort(paths)
	for _, d := range dirs {
		for rel, body := range objs {
			writeFile(t, filepath.Join(d, rel), body)
		}
		if err := os.MkdirAll(filepath.Join(d, "locks"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return paths
}

func openRestic(t *testing.T, url string) model.Backend {
	t.Helper()
	b, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(b) })
	return b
}

// W-C3, D10: restic keeps a data pack at data/<aa>/<name> on disk, and the
// REST API names it data/<name>. restic:// showed the REST name, so a
// repository folder and the same repository over restic:// listed every
// pack under two names. A mirror from the folder wrote each pack onto
// itself, then deleted it under its other name: every pack of the backup.
// restic:// now shows the layout on disk, and nothing is deleted. Deletes
// are allowed here, so only the names keep the packs.
func TestScenarioMirrorRepositoryFolderToRestic(t *testing.T) {
	transport.SetResticDeletes(true)
	defer transport.SetResticDeletes(false)
	for _, rel := range []string{"data", ""} {
		t.Run("mirror of "+cmp.Or(rel, "the repository"), func(t *testing.T) {
			root := t.TempDir()
			folder, served := filepath.Join(root, "folder"), filepath.Join(root, "served")
			resticRepo(t, folder, served)
			before := diskTree(t, served)
			dst := openRestic(t, startResticServer(t, served))
			s := scanned(t, folder, served, transport.NewLocalBackend(folder), dst)
			mark := len(transport.Log.Lines())
			if p := s.copy(t, rel, true, true); p.Failed.Load() != 0 {
				t.Errorf("%d files failed", p.Failed.Load())
			}
			if logged(mark, "mirror delete") {
				t.Errorf("the mirror deleted from a repository that holds what the folder holds: %q", transport.Log.Lines()[mark:])
			}
			if after := diskTree(t, served); !slices.Equal(after, before) {
				t.Errorf("the served repository held %q, and after the mirror %q", before, after)
			}
		})
	}
}

// W-C3: a repository is a backup, and a mirror deletes whatever its source
// does not list. Unless sc was started with -restic-delete, no delete
// reaches the server; with it, the mirror deletes.
func TestScenarioMirrorToResticDeletesOnlyWhenAllowed(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(fmt.Sprintf("-restic-delete=%v", allowed), func(t *testing.T) {
			transport.SetResticDeletes(allowed)
			defer transport.SetResticDeletes(false)
			root := t.TempDir()
			folder, served := filepath.Join(root, "folder"), filepath.Join(root, "served")
			resticRepo(t, folder, served)
			extra := resticObject("data", "a pack only the server has")
			writeFile(t, filepath.Join(served, extra), "a pack only the server has")
			dst := openRestic(t, startResticServer(t, served))
			s := scanned(t, folder, served, transport.NewLocalBackend(folder), dst)
			mark := len(transport.Log.Lines())
			if p := s.copy(t, "data", true, true); p.Failed.Load() != 0 {
				t.Errorf("%d files failed", p.Failed.Load())
			}
			kept := onDisk(filepath.Join(served, extra))
			switch {
			case !allowed && !kept:
				t.Error("without -restic-delete the mirror deleted a pack on the server")
			case !allowed && !logged(mark, "deletes are off"):
				t.Error("the refused delete was not logged with its reason")
			case allowed && kept:
				t.Errorf("with -restic-delete the mirror kept the pack the source does not have: %q", transport.Log.Lines()[mark:])
			}
		})
	}
}

// W-H6: rclone answers a path that does not exist as an empty repository,
// and a mirror from one deletes what the other side holds. A restic://
// path with no config is refused unless sc runs with -restic-new; with it
// the path opens empty, and a repository folder copies into it whole. Its
// folders are the layout's, and that an object keeps no time is no
// failure and no line in the log for each file.
func TestScenarioCopyRepositoryIntoANewResticPath(t *testing.T) {
	root := t.TempDir()
	folder, served := filepath.Join(root, "folder"), filepath.Join(root, "served")
	paths := resticRepo(t, folder)
	if err := os.MkdirAll(served, 0o755); err != nil {
		t.Fatal(err)
	}
	url := startResticServer(t, served)

	transport.SetResticNew(false)
	_, err := openRestic(t, url).List(context.Background(), "")
	if !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), "-restic-new") {
		t.Fatalf("List of a path with no repository = %v; want it refused, naming -restic-new", err)
	}

	transport.SetResticNew(true)
	defer transport.SetResticNew(false)
	dst := openRestic(t, url)
	s := scanned(t, folder, served, transport.NewLocalBackend(folder), dst)
	mark := len(transport.Log.Lines())
	if p := s.copy(t, "", true, false); p.Failed.Load() != 0 {
		t.Errorf("%d files failed: %q", p.Failed.Load(), transport.Log.Lines()[mark:])
	}
	if logged(mark, "settimes") {
		t.Errorf("the copy logged for each object that it keeps no time: %q", transport.Log.Lines()[mark:])
	}
	for _, rel := range paths {
		if !onDisk(filepath.Join(served, rel)) {
			t.Errorf("%s is not on the server", rel)
		}
	}
}
