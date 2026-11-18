package transport

import "os"

// unixFileMode is the os.FileMode of a Unix mode's permission and special
// bits, as find's %m, perl's lstat and an rsync file list give them.
// setuid, setgid and sticky become Go's bits, as os.Lstat and pkg/sftp give
// them. find and perl gave them as the Unix bits, and an rsync file list
// left them out: one file had three modes, and -mode called a file
// different from its own copy (S25). The type is the caller's to add.
func unixFileMode(m uint32) os.FileMode {
	mode := os.FileMode(m) & os.ModePerm
	if m&0o4000 != 0 {
		mode |= os.ModeSetuid
	}
	if m&0o2000 != 0 {
		mode |= os.ModeSetgid
	}
	if m&0o1000 != 0 {
		mode |= os.ModeSticky
	}
	return mode
}
