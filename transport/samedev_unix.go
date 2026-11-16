//go:build unix

package transport

import (
	"os"
	"syscall"
)

// sameDevice reports whether a and b are on one file system.
func sameDevice(a, b os.FileInfo) bool {
	sa, ok := a.Sys().(*syscall.Stat_t)
	sb, ok2 := b.Sys().(*syscall.Stat_t)
	return ok && ok2 && sa.Dev == sb.Dev
}
