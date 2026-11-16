//go:build !darwin

package transport

import "os"

// fsync is os.File.Sync: fsync(2) on Linux and the BSDs, FlushFileBuffers
// on Windows.
func fsync(f *os.File) error { return f.Sync() }
