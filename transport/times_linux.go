package transport

import (
	"syscall"
	"time"

	"sc/model"
)

func fillTimes(entry *model.FileEntry, path string) {
	var st syscall.Stat_t
	if syscall.Lstat(path, &st) != nil {
		return
	}
	entry.ATime = time.Unix(st.Atim.Sec, st.Atim.Nsec)
	entry.CTime = time.Unix(st.Ctim.Sec, st.Ctim.Nsec)
}

func setTimes(path string, mtime, atime, btime time.Time) error {
	utimes := [2]syscall.Timespec{
		{Sec: atime.Unix(), Nsec: int64(atime.Nanosecond())},
		{Sec: mtime.Unix(), Nsec: int64(mtime.Nanosecond())},
	}
	return syscall.UtimesNano(path, utimes[:])
}
