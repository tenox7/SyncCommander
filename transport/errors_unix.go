//go:build !windows

package transport

import "syscall"

// connLostErrnos: the peer or the network cut an open connection.
var connLostErrnos = []error{syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED, syscall.ETIMEDOUT}

// noSpaceErrnos: the file system takes no more data.
var noSpaceErrnos = []error{syscall.ENOSPC, syscall.EDQUOT, syscall.EROFS}
