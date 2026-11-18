//go:build !linux && !darwin && !freebsd && !windows

package transport

import "errors"

// diskFree does not know what this system has free: nothing is refused.
func diskFree(string) (int64, error) { return 0, errors.ErrUnsupported }
