package transport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// L17: vsftpd has no MFMT. It sets a time with "MDTM time path"
// (mdtm_write), which sc did not use, so SetTimes failed for every upload
// and every file copied there differed by mtime after the copy. Now it is
// used, and the time set is the time listed.
func TestFTPSetsTimesOnVsftpd(t *testing.T) {
	f := &fakeFTPD{mdtm: true, ls: map[string][]string{
		"/": {lsLine(".", true), lsLine("..", true), lsLine("f.txt", false)},
	}}
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	mt := time.Date(2026, 11, 18, 12, 34, 56, 0, time.UTC)
	if err := b.SetTimes(ctx, "f.txt", mt, time.Time{}, time.Time{}); err != nil {
		t.Fatalf("SetTimes on a server with MDTM and no MFMT: %v", err)
	}
	f.mu.Lock()
	set := append([]string(nil), f.setTimes...)
	f.mu.Unlock()
	if len(set) != 1 || !strings.HasPrefix(set[0], "MDTM 20261118123456 ") {
		t.Errorf("the commands that set a time: %q; want one MDTM 20261118123456", set)
	}
	entries, err := b.List(model.WithFreshListing(ctx), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "f.txt" && !e.ModTime.Equal(mt) {
			t.Errorf("f.txt lists %v, want the time set, %v", e.ModTime, mt)
		}
	}
}

// A server with MFMT still gets MFMT.
func TestFTPSetsTimesWithMFMTWhereItCan(t *testing.T) {
	f := &fakeFTPD{mdtm: true, mfmt: true, ls: map[string][]string{
		"/": {lsLine(".", true), lsLine("..", true), lsLine("f.txt", false)},
	}}
	b := openFakeFTP(t, f, "/")
	mt := time.Date(2026, 11, 18, 12, 34, 56, 0, time.UTC)
	if err := b.SetTimes(context.Background(), "f.txt", mt, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.setTimes) != 1 || !strings.HasPrefix(f.setTimes[0], "MFMT 20261118123456 ") {
		t.Errorf("the commands that set a time: %q; want one MFMT", f.setTimes)
	}
}

// L17: a server with neither MLSD nor MDTM lists times by LIST: to the
// minute, and to the day for a file more than six months old. They were
// compared to the second, so a file differed after every copy. Now the
// compare takes each time as exactly as it is listed; a time further off
// than that still differs.
func TestFTPListTimesCompareAsExactlyAsTheyAreListed(t *testing.T) {
	recent := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute).Add(37 * time.Second)
	old := time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)
	line := func(name string, at time.Time, layout string) string {
		return fmt.Sprintf("-rw-r--r--    1 1000     1000            5 %s %s", at.Format(layout), name)
	}
	f := &fakeFTPD{ls: map[string][]string{"/": {
		lsLine(".", true), lsLine("..", true),
		line("recent.txt", recent, "Jan _2 15:04"),
		line("old.txt", old, "Jan _2  2006"),
		line("moved.txt", recent, "Jan _2 15:04"),
	}}}
	right := openFakeFTP(t, f, "/")
	dir := t.TempDir()
	for name, mt := range map[string]time.Time{"recent.txt": recent, "old.txt": old, "moved.txt": recent.Add(-2 * time.Minute)} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("12345"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	opts := model.CompareOpts{Size: true, ModTime: true}
	s := model.NewScanner(NewLocalBackend(dir), right, 1, 1, true)
	s.Scan(context.Background(), opts)
	s.ReadTree(func(root *model.TreeNode) {
		for name, want := range map[string]model.AttrStatus{"recent.txt": model.AttrEqual, "old.txt": model.AttrEqual, "moved.txt": model.AttrDifferent} {
			n := childNamed(root, name)
			if n == nil {
				t.Errorf("%s: not in the tree", name)
				continue
			}
			if got := model.NodeStatus(n, &opts); got != want {
				t.Errorf("%s: %v, want %v", name, got, want)
				if l, r := n.Entries(); l != nil && r != nil {
					t.Logf("%s: local %v, listed %v", name, l.ModTime, r.ModTime)
				}
			}
		}
	})
}
