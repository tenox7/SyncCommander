package model

import "time"

// TimeRes is how exact a listed mtime is: how much of the time a copy set
// the backend keeps and lists again. The zero value is exact. Two mtimes
// closer than the coarser side's step are equal, as in rclone's modify
// window, and a side whose mtime is not the file's (TimeNone) leaves the
// mtime out of the compare. A time listed to the minute, or an object
// store's upload time, was "different" after every copy, and the next copy
// sent everything again (L17).
type TimeRes uint8

const (
	TimeExact  TimeRes = iota // to the nanosecond: as exact as the compare asks
	TimeMicro                 // to the microsecond, or 100 ns: NTFS
	TimeMilli                 // to the millisecond
	TimeSecond                // to the second: sftp, rsync, FTP MLSD and MDTM
	Time2Sec                  // to two seconds: FAT
	TimeMinute                // to the minute: an FTP LIST line with a time
	TimeDay                   // to the day: an FTP LIST line with a year
	TimeNone                  // not the file's time: an upload time, or none
)

var timeSteps = [...]time.Duration{
	TimeMicro:  time.Microsecond,
	TimeMilli:  time.Millisecond,
	TimeSecond: time.Second,
	Time2Sec:   2 * time.Second,
	TimeMinute: time.Minute,
	TimeDay:    24 * time.Hour,
}

// Step is the window r keeps a time to: 0 for TimeExact and for TimeNone.
func (r TimeRes) Step() time.Duration {
	if int(r) < len(timeSteps) {
		return timeSteps[r]
	}
	return 0
}

// TimeResOf is the TimeRes of a time kept to step: the finest whose window
// takes step in. 0 or less, or more than a day, is TimeNone: no time kept,
// as an sc/1 server's timeres=0 or rclone's ModTimeNotSupported (100
// years) says.
func TimeResOf(step time.Duration) TimeRes {
	if step <= 0 {
		return TimeNone
	}
	for r := TimeExact; r < TimeNone; r++ {
		if step <= max(r.Step(), time.Nanosecond) {
			return r
		}
	}
	return TimeNone
}
