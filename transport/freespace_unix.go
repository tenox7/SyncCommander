//go:build linux || darwin || freebsd

package transport

import "golang.org/x/sys/unix"

// diskFree is what dir's file system has free for this user.
func diskFree(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
