package transport

import "syscall"

// connLostErrnos: the peer or the network cut an open connection. Windows
// reports a reset socket under its own numbers, not as ECONNRESET.
var connLostErrnos = []error{
	syscall.WSAECONNRESET,
	syscall.WSAECONNABORTED,
	syscall.ERROR_NETNAME_DELETED, // a reset seen through overlapped I/O
	syscall.Errno(10060),          // WSAETIMEDOUT
}

// noSpaceErrnos: the file system takes no more data.
var noSpaceErrnos = []error{
	syscall.Errno(112),  // ERROR_DISK_FULL
	syscall.Errno(39),   // ERROR_HANDLE_DISK_FULL
	syscall.Errno(1295), // ERROR_DISK_QUOTA_EXCEEDED
	syscall.Errno(19),   // ERROR_WRITE_PROTECT
}
