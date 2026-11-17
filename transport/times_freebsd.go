package transport

import (
	"os"
	"syscall"
	"time"

	"sc/model"
)

func fillTimes(entry *model.FileEntry, info os.FileInfo) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	entry.ATime = model.NanoOf(time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec))
	entry.CTime = model.NanoOf(time.Unix(st.Ctimespec.Sec, st.Ctimespec.Nsec))
	entry.BirthTime = model.NanoOf(time.Unix(st.Birthtimespec.Sec, st.Birthtimespec.Nsec))
}

func setTimes(path string, mtime, atime, _ time.Time) error {
	utimes := [2]syscall.Timespec{
		{Sec: atime.Unix(), Nsec: int64(atime.Nanosecond())},
		{Sec: mtime.Unix(), Nsec: int64(mtime.Nanosecond())},
	}
	return syscall.UtimesNano(path, utimes[:])
}
