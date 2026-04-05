package main

import (
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func fillTimes(entry *FileEntry, path string) {
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
	if err := syscall.UtimesNano(path, utimes[:]); err != nil {
		return err
	}
	if !btime.IsZero() {
		ts := unix.Timespec{Sec: btime.Unix(), Nsec: int64(btime.Nanosecond())}
		attrList := unix.Attrlist{
			Bitmapcount: unix.ATTR_BIT_MAP_COUNT,
			Commonattr:  unix.ATTR_CMN_CRTIME,
		}
		attrBuf := (*[unsafe.Sizeof(unix.Timespec{})]byte)(unsafe.Pointer(&ts))[:]
		_ = unix.Setattrlist(path, &attrList, attrBuf, 0)
	}
	return nil
}
