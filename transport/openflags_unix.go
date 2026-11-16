//go:build unix

package transport

import "syscall"

// oNoFollow makes a local write fail on a link at the final name instead of
// writing into the link's target (L2).
const oNoFollow = syscall.O_NOFOLLOW
