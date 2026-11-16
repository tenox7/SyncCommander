package transfer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/transport"
)

// F10: a folder copy with checksum compare on, before the checksums are
// computed, rewrote every file on both sides. Now it leaves them out, says
// so in the log, and copies them only when the request says to.
func TestFolderCopyLeavesOutFilesNotComparedYet(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	// Same size and time, other bytes: only a checksum tells them apart.
	writeFile(t, filepath.Join(left, "d", "a.txt"), "left bytes")
	writeFile(t, filepath.Join(right, "d", "a.txt"), "right byte")
	for _, p := range []string{filepath.Join(left, "d", "a.txt"), filepath.Join(right, "d", "a.txt")} {
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	opts := scenarioOpts
	opts.Checksum = true
	for _, tc := range []struct {
		copyUnknown bool
		want        string
	}{{false, "right byte"}, {true, "left bytes"}} {
		s := newScenario(t, left, right)
		mark := len(transport.Log.Lines())
		p := &Progress{}
		Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: s.node(t, "d"), RelPath: "d",
			LeftToRight: true, Opts: opts, Parallel: 1, Progress: p, CopyUnknown: tc.copyUnknown})
		if got := readFile(t, filepath.Join(right, "d", "a.txt")); got != tc.want {
			t.Errorf("CopyUnknown=%v: d/a.txt holds %q, want %q", tc.copyUnknown, got, tc.want)
		}
		if said := logged(mark, "COPY d: left out 1 files on both sides that are not compared yet"); said == tc.copyUnknown {
			t.Errorf("CopyUnknown=%v: logged that it left the file out = %v, want %v", tc.copyUnknown, said, !tc.copyUnknown)
		}
	}
}
