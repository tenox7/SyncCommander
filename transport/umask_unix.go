//go:build unix

package transport

import (
	"os"
	"syscall"
)

// umask is the file creation mask sc started with. Setting the mask is the
// only way to read it, so this runs once at start, before anything creates
// a file.
var umask = func() os.FileMode {
	m := syscall.Umask(0)
	syscall.Umask(m)
	return os.FileMode(m)
}()
