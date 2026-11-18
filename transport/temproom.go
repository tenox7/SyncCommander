package transport

import (
	"errors"
	"fmt"
	"io"
)

// ErrTempRoom: the temp folder has no room for a file that rsync must send
// from disk. It fails that file at once, before a byte is read, and never
// tries it again: the folder will not grow. TMPDIR names another one.
var ErrTempRoom = errors.New("no room in the temp folder")

// tempMargin is the room a file written to the temp folder leaves free.
const tempMargin = 64 << 20

// freeSpace is what dir's file system has free; tests set it.
var freeSpace = diskFree

// roomFor fails with ErrTempRoom when dir has no room for size bytes. A
// spool found out when the disk was full, after it had read most of the
// file, and left the disk full for all else (R16). A size not known, or a
// disk that does not say what it has free, is let through.
func roomFor(dir string, size int64) error {
	if size <= 0 {
		return nil
	}
	free, err := freeSpace(dir)
	if err != nil || size+tempMargin <= free {
		return nil
	}
	return fmt.Errorf("%w: %s has %d bytes free, a file of %d needs %d more", ErrTempRoom, dir, free, size, size+tempMargin-free)
}

// spoolWriter writes a file into the temp folder and keeps the error a
// write got, so a temp disk that filled is told from a source that failed.
type spoolWriter struct {
	w   io.Writer
	err error
}

func (w *spoolWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

// tempFull is the error of a write to the temp folder that found its disk
// full: that disk is the local one, not the destination's. It stopped the
// whole copy as a full destination does, though another file may fit, and
// this one when other copies give their room back: it is tried again.
func tempFull(err error) error {
	if cannotWrite(err) {
		return fmt.Errorf("the temp folder is full: %v", err)
	}
	return err
}
