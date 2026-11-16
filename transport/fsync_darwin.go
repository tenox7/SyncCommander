//go:build darwin

package transport

import (
	"os"
	"syscall"
)

// fsync hands f's bytes to the drive with fsync(2). os.File.Sync on macOS
// asks for F_FULLFSYNC, which also empties the drive's own cache: 5.5 ms a
// file against 0.02 ms on an Apple SSD, 40 times the whole copy of a small
// file, hours over a million files. fsync(2) is what SQLite does on macOS
// by default. A part's bytes reach the drive before the rename gives them
// the file's name, so a crash of the system cannot leave the name on bytes
// that were never written; a power cut can still lose what the drive held
// in its own cache.
func fsync(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	cerr := rc.Control(func(fd uintptr) {
		err = syscall.Fsync(int(fd))
		for err == syscall.EINTR {
			err = syscall.Fsync(int(fd))
		}
	})
	if cerr != nil {
		return cerr
	}
	if err != nil {
		return &os.PathError{Op: "fsync", Path: f.Name(), Err: err}
	}
	return nil
}
