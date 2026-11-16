package transfer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// rsyncScenario scans the local folder left against the rsync daemon's
// module; the local side is s.lb, the daemon s.rb.
func rsyncScenario(t *testing.T, left, url, mod string) *scenario {
	t.Helper()
	rb, err := transport.TryOpenBackend(url, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.CloseBackend(rb) })
	return scanned(t, left, mod, transport.NewLocalBackend(left), rb)
}

// R12: rsync wrote every file in place (--inplace). A file with other hard
// links, as in a snapshot backup, is one inode: the copy changed every
// snapshot that shares it. Now rsync writes a temp file and renames it over
// the name, which gets a new inode; the snapshot keeps its bytes. Both ways:
// to the daemon (stock rsync receives) and from it (gorsync receives).
func TestRsyncOverwriteLeavesHardLinksAlone(t *testing.T) {
	url, mod := startRsyncd(t)
	left := t.TempDir()
	old, now := strings.Repeat("o", 1000), strings.Repeat("n", 1000) // one size: no resume
	writeFile(t, filepath.Join(left, "up.txt"), now)
	writeFile(t, filepath.Join(mod, "up.txt"), old)
	writeFile(t, filepath.Join(mod, "down.txt"), now)
	writeFile(t, filepath.Join(left, "down.txt"), old)
	for _, l := range [][2]string{{filepath.Join(mod, "up.txt"), filepath.Join(mod, "snap", "up.txt")}, {filepath.Join(left, "down.txt"), filepath.Join(left, "snap", "down.txt")}} {
		if err := os.MkdirAll(filepath.Dir(l[1]), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(l[0], l[1]); err != nil {
			t.Fatal(err)
		}
	}
	s := rsyncScenario(t, left, url, mod)
	for _, tc := range []struct {
		rel         string
		leftToRight bool
		dir         string
	}{{"up.txt", true, mod}, {"down.txt", false, left}} {
		if p := s.copy(t, tc.rel, tc.leftToRight, false); p.Failed.Load() != 0 {
			t.Errorf("%s: copy failed", tc.rel)
		}
		if got := readFile(t, filepath.Join(tc.dir, tc.rel)); got != now {
			t.Errorf("%s holds %.10q..., want the new version", tc.rel, got)
		}
		if got := readFile(t, filepath.Join(tc.dir, "snap", tc.rel)); got != old {
			t.Errorf("%s: the copy changed the snapshot's hard link too: it holds %.10q...", tc.rel, got)
		}
	}
}

// R12: an overwrite cut off half way left the new start on the old end
// under the file's name, at the old size: in place, there is no old version
// to fall back to. Now the cut-off temp file is removed and the old version
// stays, and nothing of the cut-off copy is left next to it.
func TestRsyncCutOffOverwriteKeepsTheOldFile(t *testing.T) {
	url, mod := startRsyncd(t)
	left := t.TempDir()
	const size = 8 << 20
	old, now := bytes.Repeat([]byte("o"), size), bytes.Repeat([]byte("n"), size)
	writeFile(t, filepath.Join(left, "up.bin"), string(now))
	writeFile(t, filepath.Join(mod, "up.bin"), string(old))
	writeFile(t, filepath.Join(mod, "down.bin"), string(now))
	writeFile(t, filepath.Join(left, "down.bin"), string(old))
	s := rsyncScenario(t, left, url, mod)
	t.Cleanup(func() { transport.SetBandwidthIn(0); transport.SetBandwidthOut(0) })
	transport.SetBandwidthIn(1 << 20)
	transport.SetBandwidthOut(1 << 20)
	for _, tc := range []struct {
		rel         string
		leftToRight bool
		dir         string
	}{{"up.bin", true, mod}, {"down.bin", false, left}} {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		src, dst := s.lb, s.rb
		if !tc.leftToRight {
			src, dst = s.rb, s.lb
		}
		p := &Progress{}
		Copy(ctx, Request{Src: src, Dst: dst, Scanner: s.sc, Node: s.node(t, tc.rel), RelPath: tc.rel,
			LeftToRight: tc.leftToRight, Opts: scenarioOpts, Parallel: 1, Progress: p, Newer: NewerOverwrite})
		cancel()
		if p.Failed.Load() == 0 {
			t.Fatalf("%s: the copy was not cut off; the test needs a slower copy", tc.rel)
		}
		// The receiver cleans up after the connection drops: wait for its
		// temp file to go, then look.
		var temps []string
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if temps = rsyncTemps(t, tc.dir, tc.rel); len(temps) == 0 {
				break
			}
		}
		if len(temps) != 0 {
			t.Errorf("%s: the cut-off copy left %q", tc.rel, temps)
		}
		got, err := os.ReadFile(filepath.Join(tc.dir, tc.rel))
		if err != nil || !bytes.Equal(got, old) {
			t.Errorf("%s after a cut-off copy: %d bytes, %d of them new, err %v; want the old version", tc.rel, len(got), bytes.Count(got, []byte("n")), err)
		}
	}
}

// rsyncTemps lists the temp files rsync may have left for rel in dir:
// ".<rel>.<random>", not sc's own part file.
func rsyncTemps(t *testing.T, dir, rel string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var temps []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "."+rel+".") && !model.IsPartName(e.Name()) {
			temps = append(temps, e.Name())
		}
	}
	return temps
}
