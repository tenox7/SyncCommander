//go:build linux || darwin || freebsd

package transport

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// makeTempHome makes this process's home in base, and locks it. The lock
// file is made under another name, and named sc.lock once it is locked:
// a sweep never finds an sc.lock it can take in a live process's home. On
// a file system with no locks the home is used all the same, and is never
// swept.
func makeTempHome(base string) (string, *os.File, error) {
	dir, err := os.MkdirTemp(base, tempHomePrefix+"*")
	if err != nil {
		return "", nil, err
	}
	f, err := os.Create(filepath.Join(dir, tempLockName+".tmp"))
	if err != nil {
		return dir, nil, nil
	}
	if err := lockFile(f); err != nil {
		Log.Add("temp", DirWarn, "no lock on "+f.Name()+": "+err.Error()+"; an sc that ends this way leaves "+dir)
		return dir, f, nil
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, tempLockName)); err != nil {
		Log.Add("temp", DirWarn, "naming the lock in "+dir+": "+err.Error())
	}
	return dir, f, nil
}

// lockFile takes f's lock, or fails at once if a process holds it. The
// lock is the open file's: one taken through another open of the same
// file, even in this process, is another lock.
func lockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

// sweepTempHomes removes the homes in base whose lock it can take: the
// process that made each one has ended. A folder with no sc.lock, or one
// it cannot open, and anything not a folder (a link to one) are left
// alone; so are the temp folders an older sc made straight in base.
func sweepTempHomes(base string) {
	ents, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, e := range ents {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), tempHomePrefix) {
			continue
		}
		dir := filepath.Join(base, e.Name())
		f, err := os.Open(filepath.Join(dir, tempLockName))
		if err != nil {
			continue
		}
		if lockFile(f) == nil {
			if err := removeTempHome(dir); err != nil {
				Log.Add("temp", DirWarn, "removing "+dir+", left by an sc that ended: "+err.Error())
			} else {
				Log.Add("temp", DirIn, "removed "+dir+", left by an sc that ended")
			}
		}
		f.Close()
	}
}

// removeTempHome removes a home with its lock last: a sweep that stops
// half way (sc is killed, a file will not go) leaves a home the next one
// still finds.
func removeTempHome(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.Name() == tempLockName {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return os.RemoveAll(dir)
}
