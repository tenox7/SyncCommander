package transfer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/transport"
)

// F16: a copy put an older or smaller file over a newer or larger one on
// the destination without a word. Now it writes them only when the user
// chose to overwrite them, and leaves them as they are when the user chose
// to keep them. When nobody asked (the tree did not know them when the
// dialog counted), each one fails, and the next copy asks.
func TestNewerDestinationFiles(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	later := at.Add(10 * time.Minute)
	for _, tc := range []struct {
		rule        NewerRule
		failed      int64
		overwritten bool
		logs        []string
	}{
		{NewerRefuse, 2, false, []string{"COPY d/newer.txt: refused, it is newer on right", "COPY d/larger.txt: refused, it is larger on right"}},
		{NewerKeep, 0, false, []string{"COPY d: left out 2 files that are newer or larger on right"}},
		{NewerOverwrite, 0, true, nil},
	} {
		root := t.TempDir()
		left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
		for _, f := range []struct {
			path, body string
			mtime      time.Time
		}{
			{filepath.Join(left, "d", "newer.txt"), "source", at},
			{filepath.Join(right, "d", "newer.txt"), "dest!!", later},
			{filepath.Join(left, "d", "larger.txt"), "source", later},
			{filepath.Join(right, "d", "larger.txt"), "a longer destination", at},
			{filepath.Join(left, "d", "plain.txt"), "source", later},
			{filepath.Join(right, "d", "plain.txt"), "dest", at},
		} {
			writeFile(t, f.path, f.body)
			if err := os.Chtimes(f.path, f.mtime, f.mtime); err != nil {
				t.Fatal(err)
			}
		}
		s := scanned(t, left, right, transport.NewLocalBackend(left), transport.NewLocalBackend(right))
		mark := len(transport.Log.Lines())
		p := &Progress{}
		Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: s.node(t, "d"), RelPath: "d",
			LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: p, Newer: tc.rule})
		if got := p.Failed.Load(); got != tc.failed {
			t.Errorf("rule %d: %d failed, want %d", tc.rule, got, tc.failed)
		}
		if got := readFile(t, filepath.Join(right, "d", "plain.txt")); got != "source" {
			t.Errorf("rule %d: d/plain.txt, older and smaller on right, holds %q", tc.rule, got)
		}
		for _, f := range []string{"newer.txt", "larger.txt"} {
			if got := readFile(t, filepath.Join(right, "d", f)); (got == "source") != tc.overwritten {
				t.Errorf("rule %d: d/%s holds %q", tc.rule, f, got)
			}
		}
		for _, l := range tc.logs {
			if !logged(mark, l) {
				t.Errorf("rule %d: no log line %q", tc.rule, l)
			}
		}
	}
}
