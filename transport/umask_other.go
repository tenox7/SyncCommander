//go:build !unix

package transport

import "os"

// umask: Windows has no creation mask, and Chmod only sets read-only.
const umask os.FileMode = 0
