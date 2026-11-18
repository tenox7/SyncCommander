package transport

import (
	"errors"
	"io/fs"
	"os"
	"sync"
)

// sc's temp folders. An upload to rsync spools the whole file in the temp
// folder, and a kill, a crash or a power cut left it there for good, or
// until the system's own cleaning came round (R16). Now each sc process
// keeps its temp folders and files in a home of its own, $TMPDIR/sc-tmp-*,
// and holds a lock on the sc.lock file in it while it runs: the system
// lets go of the lock when the process ends, however it ends. The first
// sc to need the temp folder removes the homes whose lock it can take.
// Where there is no such lock (Windows), the temp folders go straight in
// $TMPDIR, as before, and nothing is swept.

const (
	tempHomePrefix = "sc-tmp-"
	tempLockName   = "sc.lock"
)

var temps struct {
	sync.Mutex
	homes map[string]tempHome // by temp folder: os.TempDir()
}

// tempHome is this process's home in one temp folder.
type tempHome struct {
	dir  string
	lock *os.File // held open, and so locked, while the process runs
}

// tempDir makes a temp folder, as os.MkdirTemp("", pattern) did, in this
// process's home.
func tempDir(pattern string) (string, error) {
	var dir string
	err := inTempHome(func(home string) (err error) {
		dir, err = os.MkdirTemp(home, pattern)
		return err
	})
	return dir, err
}

// tempFile makes a temp file, as os.CreateTemp("", pattern) did, in this
// process's home.
func tempFile(pattern string) (*os.File, error) {
	var f *os.File
	err := inTempHome(func(home string) (err error) {
		f, err = os.CreateTemp(home, pattern)
		return err
	})
	return f, err
}

// inTempHome runs mk in this process's home. A home that went behind sc's
// back (a cleaner removes folders not used in days) is made again: every
// spool after it failed.
func inTempHome(mk func(home string) error) error {
	home, err := tempHomeDir("")
	if err != nil {
		return err
	}
	if err = mk(home); errors.Is(err, fs.ErrNotExist) {
		if home, err = tempHomeDir(home); err == nil {
			err = mk(home)
		}
	}
	return err
}

// tempHomeDir is this process's home in the temp folder, made the first
// time, after the homes of ended processes are swept. stale is a home that
// is gone: another one takes its place. Only a new home sweeps: a spool
// does not list the temp folder.
func tempHomeDir(stale string) (string, error) {
	base := os.TempDir()
	temps.Lock()
	defer temps.Unlock()
	if h, ok := temps.homes[base]; ok && h.dir != stale {
		return h.dir, nil
	}
	sweepTempHomes(base)
	dir, lock, err := makeTempHome(base)
	if err != nil {
		return "", err
	}
	if dir == "" {
		return base, nil // no lock on this system: straight in the temp folder
	}
	if old, ok := temps.homes[base]; ok {
		old.lock.Close()
	}
	if temps.homes == nil {
		temps.homes = map[string]tempHome{}
	}
	temps.homes[base] = tempHome{dir: dir, lock: lock}
	return dir, nil
}

// SweepTemp removes the homes that ended sc processes left in the temp
// folder. main runs it as sc starts, so the room comes back even when
// nothing spools.
func SweepTemp() {
	temps.Lock()
	defer temps.Unlock()
	sweepTempHomes(os.TempDir())
}

// RemoveTemp removes this process's home, with what is still in it: main
// runs it as sc ends. It never removes the temp folder itself.
func RemoveTemp() {
	base := os.TempDir()
	temps.Lock()
	defer temps.Unlock()
	h, ok := temps.homes[base]
	if !ok {
		return
	}
	delete(temps.homes, base)
	if h.dir != base {
		os.RemoveAll(h.dir)
	}
	h.lock.Close()
}
