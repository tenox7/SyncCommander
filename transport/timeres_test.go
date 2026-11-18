package transport

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	rsyncpkg "github.com/gokrazy/rsync"
	"github.com/rclone/rclone/fs"

	"sc/model"
)

// L17: with UseServerModTime on S3 or Swift, a listed mtime is the upload
// time, which no copy sets; it is left out of the compare. Otherwise a
// remote's mtimes are as exact as rclone says it keeps them.
func TestRcloneUploadTimeIsNoMtime(t *testing.T) {
	for _, c := range []struct {
		upload    bool
		precision time.Duration
		want      model.TimeRes
	}{
		{true, time.Nanosecond, model.TimeNone},
		{false, fs.ModTimeNotSupported, model.TimeNone},
		{false, time.Second, model.TimeSecond},
		{false, time.Millisecond, model.TimeMilli},
		{false, time.Nanosecond, model.TimeExact},
	} {
		if got := rcloneTimeRes(c.upload, c.precision); got != c.want {
			t.Errorf("rcloneTimeRes(upload %v, %v) = %d, want %d", c.upload, c.precision, got, c.want)
		}
	}
	// The memory remote is one per process: a bucket of its own.
	b, err := NewRcloneBackend(context.Background(), "rclone://:memory:timeres", false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.timeRes = model.TimeMinute
	if err := b.CopyFrom(context.Background(), "a.txt", strings.NewReader("abc"), model.PutParams{Size: 3}); err != nil {
		t.Fatal(err)
	}
	entries, err := b.List(model.WithFreshListing(context.Background()), "")
	if err != nil || len(entries) != 1 || entries[0].Name != "a.txt" || entries[0].TimeRes != model.TimeMinute {
		t.Errorf("List = %+v, %v; want a.txt, with the remote's TimeRes", entries, err)
	}
}

// sftp and rsync send whole seconds: their mtimes say so, and a sub-second
// compare takes them to the second.
func TestSecondTimesSaySo(t *testing.T) {
	info := fakeFileInfo{name: "a", size: 1, mtime: time.Unix(1700000000, 0)}
	if e := sftpEntry("a", info); e.TimeRes != model.TimeSecond {
		t.Errorf("sftp entry: TimeRes %d, want TimeSecond", e.TimeRes)
	}
	e, ok := fileInfoEntry("", rsyncpkg.FileInfo{Name: "a", Length: 1, ModTime: time.Unix(1700000000, 0), Mode: 0o100644})
	if !ok || e.TimeRes != model.TimeSecond {
		t.Errorf("rsync entry: TimeRes %d (%v), want TimeSecond", e.TimeRes, ok)
	}
}

type fakeFileInfo struct {
	name  string
	size  int64
	mtime time.Time
}

func (i fakeFileInfo) Name() string       { return i.name }
func (i fakeFileInfo) Size() int64        { return i.size }
func (i fakeFileInfo) Mode() os.FileMode  { return 0o644 }
func (i fakeFileInfo) ModTime() time.Time { return i.mtime }
func (i fakeFileInfo) IsDir() bool        { return false }
func (i fakeFileInfo) Sys() any           { return nil }
