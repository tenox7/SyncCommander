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
	entry.ATime = time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec)
	entry.CTime = time.Unix(st.Ctimespec.Sec, st.Ctimespec.Nsec)
	entry.BirthTime = time.Unix(st.Birthtimespec.Sec, st.Birthtimespec.Nsec)
}

func setTimes(path string, mtime, atime, btime time.Time) error {
	utimes := [2]syscall.Timespec{
		{Sec: atime.Unix(), Nsec: int64(atime.Nanosecond())},
		{Sec: mtime.Unix(), Nsec: int64(mtime.Nanosecond())},
	}
	return syscall.UtimesNano(path, utimes[:])
}
