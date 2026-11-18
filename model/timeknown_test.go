package model

import (
	"testing"
	"time"
)

// R22: an rsync file list has no atime or ctime, and no FTP or WebDAV
// listing has a birth time. With that time on in the compare, every file of
// such a side was different from its copy anywhere else, and stayed so
// after a copy. A side without the time now leaves it out.
func TestATimeCTimeBirthTimeASideDoesNotHave(t *testing.T) {
	mt := time.Unix(1700000000, 0)
	at := NanoOf(mt.Add(-time.Hour))
	full := &FileEntry{RelPath: "f", Name: "f", Size: 3, ModTime: mt, ATime: at, CTime: at, BirthTime: at}
	bare := &FileEntry{RelPath: "f", Name: "f", Size: 3, ModTime: mt}
	opts := CompareOpts{Size: true, ModTime: true, ATime: true, CTime: true, BirthTime: true}
	n := &TreeNode{Name: "f", RelPath: "f"}
	n.Sides[SideLeft].Entry, n.Sides[SideRight].Entry = full, bare
	compareNode(n, opts)
	if c := n.Compare; c.ATime != AttrNA || c.CTime != AttrNA || c.BirthTime != AttrNA {
		t.Errorf("atime, ctime, birth time = %v, %v, %v; want each left out (AttrNA)", c.ATime, c.CTime, c.BirthTime)
	}
	if s := NodeStatus(n, &opts); s != AttrEqual {
		t.Errorf("status = %v, want equal: size and mtime match", s)
	}
	// Both sides have them: compared as before.
	other := *full
	other.ATime = NanoOf(mt)
	n.Sides[SideRight].Entry = &other
	compareNode(n, opts)
	if c := n.Compare; c.ATime != AttrDifferent || c.CTime != AttrEqual || c.BirthTime != AttrEqual {
		t.Errorf("with both sides: atime, ctime, birth time = %v, %v, %v; want different, equal, equal", c.ATime, c.CTime, c.BirthTime)
	}
}
