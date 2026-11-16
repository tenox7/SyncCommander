//go:build !unix

package transport

import "os"

// sameDevice cannot tell here (Windows), so the names above a root are not
// tried; the names in it are.
func sameDevice(a, b os.FileInfo) bool { return false }
