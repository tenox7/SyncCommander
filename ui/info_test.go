package ui

import (
	"strings"
	"testing"
)

// F22: the info dialog counts the files on both sides that are not compared
// yet on their own line; they were counted as equal.
func TestInfoCountsFilesNotCompared(t *testing.T) {
	d := NewInfoDialog()
	d.Open(TreeStats{FilesEqual: 3, FilesDiff: 2, FilesUnknown: 7}, "left", "right", "", nil, nil)
	for _, line := range strings.Split(d.View(120, 40), "\n") {
		if strings.Contains(line, "Not compared") {
			if !strings.Contains(line, "7") {
				t.Errorf("the line says %q, want the count 7", line)
			}
			return
		}
	}
	t.Error("the info dialog has no \"Not compared\" line")
}
