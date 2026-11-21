package transfer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// A part file only on the destination, of a file the copy writes, was a
// delete: the dialog asked to keep it or to delete it, and the copy wrote
// into it and renamed it over its file either way, so "keep" kept nothing.
// It is no delete now, and no data in what the destination holds. A part
// with no file to write (a file gone from the source, or the same on both
// sides) is a delete still, and says it is a part.
func TestPlanLeavesOutThePartsTheCopyWritesInto(t *testing.T) {
	root := t.TempDir()
	left, right := filepath.Join(root, "l"), filepath.Join(root, "r")
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, f := range []struct{ path, body string }{
		{filepath.Join(left, "d", "IMG.JPG"), "jpeg bytes, all of them"},
		{filepath.Join(right, "d", ".IMG.JPG.sc-part"), "jpeg bytes"}, // a cut-off copy of IMG.JPG
		{filepath.Join(right, "d", ".OLD.JPG.sc-part"), "old"},        // its file is gone from the source
		{filepath.Join(left, "d", "same.txt"), "on both"},
		{filepath.Join(right, "d", "same.txt"), "on both"},
		{filepath.Join(right, "d", ".same.txt.sc-part"), "on"}, // its file is not copied
	} {
		writeFile(t, f.path, f.body)
		if err := os.Chtimes(f.path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	s := newScenario(t, left, right)
	node := s.node(t, "d")
	plan := PlanCopy(s.sc, node, true, scenarioOpts)
	var deletes []string
	for _, d := range plan.Deletes {
		if !d.IsPart {
			t.Errorf("%s is not marked as a part", d.RelPath)
		}
		deletes = append(deletes, d.RelPath)
	}
	if want := []string{"d/.OLD.JPG.sc-part", "d/.same.txt.sc-part"}; !slices.Equal(deletes, want) {
		t.Fatalf("the plan deletes %q, want %q", deletes, want)
	}
	if plan.Dst.Files != 1 || plan.Dst.Size != int64(len("on both")) {
		t.Errorf("the destination holds %+v: parts count as data", plan.Dst)
	}
	plan.Mirror = true
	mark := len(transport.Log.Lines())
	Copy(context.Background(), Request{Src: s.lb, Dst: s.rb, Scanner: s.sc, Node: node, RelPath: "d",
		LeftToRight: true, Opts: scenarioOpts, Parallel: 1, Progress: &Progress{}, Plan: plan})
	if got := readFile(t, filepath.Join(right, "d", "IMG.JPG")); got != "jpeg bytes, all of them" {
		t.Errorf("d/IMG.JPG holds %q", got)
	}
	for _, name := range []string{".IMG.JPG.sc-part", ".OLD.JPG.sc-part", ".same.txt.sc-part"} {
		if _, err := os.Stat(filepath.Join(right, "d", name)); !os.IsNotExist(err) {
			t.Errorf("d/%s is still there: %v", name, err)
		}
	}
	for _, l := range transport.Log.Lines()[mark:] {
		if strings.Contains(l, "the dialog did not show") || strings.Contains(l, "mirror delete: kept") {
			t.Errorf("the log says the copy kept its own part: %s", l)
		}
	}
}
