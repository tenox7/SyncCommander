package transport

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// setTemp points the temp folder at dir for the test, and removes this
// process's home there when it ends.
func setTemp(t *testing.T, dir string) {
	t.Helper()
	for _, v := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(v, dir)
	}
	t.Cleanup(RemoveTemp)
}

// leftInTemp lists what is in the temp folder tmp but this process's home
// and its lock: what a test's calls left there.
func leftInTemp(t *testing.T, tmp string) []string {
	t.Helper()
	var left []string
	err := filepath.WalkDir(tmp, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tmp, p)
		rel = filepath.ToSlash(rel)
		top, rest, _ := strings.Cut(rel, "/")
		home := strings.HasPrefix(top, tempHomePrefix)
		if rel == "." || home && (rest == "" && d.IsDir() || rest == tempLockName) {
			return nil
		}
		left = append(left, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return left
}

// RemoveTemp, as sc ends, removes this process's home in the temp folder
// and nothing else: the temp folder and what else is in it are not sc's.
// On Windows there is no home, and nothing is removed.
func TestRemoveTempKeepsTheTempFolder(t *testing.T) {
	tmp := t.TempDir()
	setTemp(t, tmp)
	writeTestFile(t, tmp, "theirs/file.txt", "not sc's")
	dir, err := tempDir("rsync-upload-*")
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, dir, "spool", "a spool")
	RemoveTemp()
	if !exists(tmp, "theirs/file.txt") {
		t.Fatal("RemoveTemp removed a file in the temp folder that is not sc's")
	}
	if home := filepath.Dir(dir); home != tmp {
		if _, err := os.Lstat(home); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("RemoveTemp left this process's home %s: %v", home, err)
		}
	}
	if dir, err := tempDir("rsync-upload-*"); err != nil {
		t.Errorf("a temp folder after RemoveTemp: %v", err)
	} else if _, err := os.Stat(dir); err != nil {
		t.Errorf("a temp folder after RemoveTemp: %v", err)
	}
}

// Every temp folder and temp file sc makes goes in its home, where a sweep
// finds what a killed sc left: one made straight in the temp folder stays
// there for good.
func TestTempFoldersGoInTheHome(t *testing.T) {
	// The test binary may run away from the source (in a container, in
	// /tmp): then ".." is no module, and nothing is walked.
	if mod, err := os.ReadFile("../go.mod"); err != nil || !strings.HasPrefix(string(mod), "module sc\n") {
		t.Skip("not in the source tree")
	}
	straight := regexp.MustCompile(`os\.(MkdirTemp|CreateTemp)\(""|os\.TempDir\(\)`)
	err := filepath.WalkDir("..", func(p string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir() && p != ".." && strings.HasPrefix(d.Name(), "."):
			return filepath.SkipDir // .git
		case d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.HasPrefix(d.Name(), "tempdir"):
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(src), "\n") {
			if code, _, _ := strings.Cut(line, "//"); straight.MatchString(code) {
				t.Errorf("%s:%d makes a temp folder straight in $TMPDIR, not with tempDir or tempFile: %s", p, i+1, strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
