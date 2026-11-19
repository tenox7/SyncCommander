package ui

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// U16: a time option changed in Settings did nothing until a rescan. With
// the grace turned off, files a second apart are different at once, and
// equal again with it on.
func TestSettingsTimeOptionsApplyAtOnce(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local)
	for _, side := range []struct {
		dir string
		at  time.Time
	}{{left, at}, {right, at.Add(time.Second)}} {
		p := filepath.Join(side.dir, "f.txt")
		writeTestFile(t, p, "same")
		if err := os.Chtimes(p, side.at, side.at); err != nil {
			t.Fatal(err)
		}
	}
	m := scannedModel(t, left, right)
	if !m.cmpOpts.TimeGrace {
		t.Fatal("the test model has no grace on")
	}
	if got := m.cachedStats; got.FilesDiff != 0 || got.FilesEqual != 1 {
		t.Fatalf("with the grace on the rollup holds %d different and %d equal files, want 0 and 1", got.FilesDiff, got.FilesEqual)
	}
	press(m, "=")
	for range 8 {
		press(m, "down")
	}
	if !m.settings.IsOpen() || m.settings.options[m.settings.cursor].Value != &m.cmpOpts.TimeGrace {
		t.Fatal("the cursor is not on the grace option")
	}
	press(m, " ")
	if m.cmpOpts.TimeGrace {
		t.Fatal("space did not turn the grace off")
	}
	if got := m.cachedStats; got.FilesDiff != 1 {
		t.Errorf("with the grace off the rollup holds %d different files, want 1 at once", got.FilesDiff)
	}
	press(m, " ")
	if got := m.cachedStats; got.FilesDiff != 0 || got.FilesEqual != 1 {
		t.Errorf("with the grace on again the rollup holds %d different and %d equal files, want 0 and 1", got.FilesDiff, got.FilesEqual)
	}
}
