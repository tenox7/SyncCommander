package transport

import (
	"os"
	"syscall"
	"time"

	"sc/model"
)

func fillTimes(entry *model.FileEntry, path string) {
	info, err := os.Lstat(path)
	if err != nil {
		return
	}
	d := info.Sys().(*syscall.Win32FileAttributeData)
	entry.ATime = time.Unix(0, d.LastAccessTime.Nanoseconds())
	entry.CTime = time.Unix(0, d.CreationTime.Nanoseconds())
	entry.BirthTime = time.Unix(0, d.CreationTime.Nanoseconds())
}

func setTimes(path string, mtime, atime, btime time.Time) error {
	h, err := syscall.CreateFile(
		syscall.StringToUTF16Ptr(path),
		syscall.FILE_WRITE_ATTRIBUTES,
		syscall.FILE_SHARE_WRITE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(h)

	mt := syscall.NsecToFiletime(mtime.UnixNano())
	at := syscall.NsecToFiletime(atime.UnixNano())
	ct := &at
	if !btime.IsZero() {
		bt := syscall.NsecToFiletime(btime.UnixNano())
		ct = &bt
	}
	return syscall.SetFileTime(h, ct, &at, &mt)
}
