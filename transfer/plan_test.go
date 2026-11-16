package transfer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/transport"
)

// D6: a mirror deletes an entry only while it is still the one the plan
// read. A file only on the destination that is a folder by the time the
// copy deletes is kept, with what it holds: the dialog showed a file. So is
// a file rewritten since, with the same size or with its old time put back.
func TestMirrorKeepsAnEntryThatChangedSinceThePlan(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, f := range []struct{ path, body string }{
		{filepath.Join(left, "d", "a.txt"), "on both"},
		{filepath.Join(right, "d", "a.txt"), "on both"},
		{filepath.Join(right, "d", "x"), "only on the right"},
		{filepath.Join(right, "d", "y"), "only on the right"},
		{filepath.Join(right, "d", "same-size"), "only on the right"},
		{filepath.Join(right, "d", "same-time"), "only on the right"},
	} {
		writeFile(t, f.path, f.body)
		if err := os.Chtimes(f.path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	s := newScenario(t, left, right)
	node := s.node(t, "d")
	plan := PlanCopy(s.sc, node, true, scenarioOpts)
	if files, dirs := DeleteCounts(plan.Deletes); files != 4 || dirs != 0 {
		t.Fatalf("the plan deletes %d files, %d folders, want 4 and 0: %+v", files, dirs, plan.Deletes)
	}
	plan.Mirror = true
	if err := os.Remove(filepath.Join(right, "d", "x")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(right, "d", "x", "new.txt"), "new work in a folder that took the file's name")
	writeFile(t, filepath.Join(right, "d", "same-size"), "ONLY ON THE RIGHT") // a later time
	writeFile(t, filepath.Join(right, "d", "same-time"), "rewritten, and its time put back")
	if err := os.Chtimes(filepath.Join(right, "d", "same-time"), at, at); err != nil {
		t.Fatal(err)
	}
	mark := len(transport.Log.Lines())
	Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: "d",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: &Progress{}, Plan: plan})
	if got := readFile(t, filepath.Join(right, "d", "x", "new.txt")); got != "new work in a folder that took the file's name" {
		t.Errorf("d/x/new.txt holds %q", got)
	}
	if _, err := os.Stat(filepath.Join(right, "d", "y")); !os.IsNotExist(err) {
		t.Errorf("d/y, still as the plan read it, was not deleted: %v", err)
	}
	for _, name := range []string{"same-size", "same-time"} {
		if _, err := os.Stat(filepath.Join(right, "d", name)); err != nil {
			t.Errorf("d/%s, rewritten since the plan, is gone: %v", name, err)
		}
	}
	for _, want := range []string{
		"mirror delete d/x skipped: it is not the file the dialog showed",
		"mirror delete d/same-size skipped: it changed since the dialog showed it",
		"mirror delete d/same-time skipped: it changed since the dialog showed it",
		"mirror delete: kept 3 on right",
	} {
		if !logged(mark, want) {
			t.Errorf("no log line %q", want)
		}
	}
}
