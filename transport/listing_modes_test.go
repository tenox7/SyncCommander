package transport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sc/model"
)

// S25: setuid, setgid and sticky came back as Go's bits from os.Lstat and
// READDIR, as the Unix bits from find and perl, and not at all from an
// rsync file list. One file had another mode through each lister, and
// -mode called it different from its own copy. Every lister now gives
// Go's bits.
func TestListersAgreeOnSpecialModeBits(t *testing.T) {
	root := t.TempDir()
	names := map[string]os.FileMode{
		"setuid":        0o755 | os.ModeSetuid,
		"setgid":        0o755 | os.ModeSetgid,
		"sticky folder": 0o755 | os.ModeDir | os.ModeSetgid | os.ModeSticky,
	}
	want := map[string]os.FileMode{}
	for name, mode := range names {
		p := filepath.Join(root, name)
		var err error
		if mode.IsDir() {
			err = os.Mkdir(p, 0o755)
		} else {
			err = os.WriteFile(p, []byte("x"), 0o644)
		}
		if err == nil {
			err = os.Chmod(p, mode)
		}
		if err != nil {
			t.Fatal(err)
		}
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode() != mode {
			t.Skipf("%s: this disk keeps %v for %v", name, fi.Mode(), mode)
		}
		want[name] = mode
	}
	ctx := context.Background()
	listers := []struct {
		name string
		list func(t *testing.T) ([]model.FileEntry, error)
	}{
		{"local", func(t *testing.T) ([]model.FileEntry, error) { return NewLocalBackend(root).List(ctx, "") }},
		{"scp, find", func(t *testing.T) ([]model.FileEntry, error) {
			b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { b.Close() })
			return b.List(ctx, "")
		}},
		{"scp, stat", func(t *testing.T) ([]model.FileEntry, error) {
			b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { b.Close() })
			var out []model.FileEntry
			for name := range want {
				e, err := b.Stat(ctx, name)
				if err != nil {
					return nil, err
				}
				out = append(out, e)
			}
			return out, nil
		}},
		{"sftp, READDIR", func(t *testing.T) ([]model.FileEntry, error) {
			b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+startShellServer(t, true)+root, true, 1)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { b.Close() })
			return b.List(ctx, "")
		}},
		{"sftp, find", func(t *testing.T) ([]model.FileEntry, error) {
			b, err := NewSFTPBackend(ctx, "sftp://x:pw@"+startShellServer(t, true)+root, true, 1)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { b.Close() })
			if err := b.PreloadRecursive(ctx, ""); err != nil {
				return nil, err
			}
			return b.List(ctx, "")
		}},
		{"rsync+ssh, find", func(t *testing.T) ([]model.FileEntry, error) {
			if _, err := exec.LookPath("rsync"); err != nil {
				t.Skip("no rsync binary")
			}
			b, err := NewRsyncSSHBackend(ctx, "rsync+ssh://x:pw@"+startExecSSHServer(t)+root, true, 1)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { b.Close() })
			return b.List(ctx, "")
		}},
		{"rsync+ssh, file list", func(t *testing.T) ([]model.FileEntry, error) {
			if _, err := exec.LookPath("rsync"); err != nil {
				t.Skip("no rsync binary")
			}
			b, err := NewRsyncSSHBackend(ctx, "rsync+ssh://x:pw@"+startExecSSHServer(t)+root, true, 1)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { b.Close() })
			mark := len(Log.Lines())
			if err := b.PreloadRecursive(ctx, ""); err != nil {
				return nil, err
			}
			entries, err := b.List(ctx, "")
			if !loggedSince(mark, "RLIST") {
				t.Error("the listing was not rsync's file list")
			}
			return entries, err
		}},
	}
	for _, l := range listers {
		t.Run(l.name, func(t *testing.T) {
			entries, err := l.list(t)
			if err != nil {
				t.Fatal(err)
			}
			seen := 0
			for _, e := range entries {
				w, ok := want[e.Name]
				if !ok {
					continue
				}
				seen++
				if e.Mode != w {
					t.Errorf("%s: mode %v (%#o), want %v (%#o), as os.Lstat gives it", e.Name, e.Mode, uint32(e.Mode), w, uint32(w))
				}
			}
			if seen != len(want) {
				t.Errorf("listed %d of the %d names", seen, len(want))
			}
		})
	}
}

// S25: the Unix bits read as Go's, whatever the type.
func TestUnixModeBits(t *testing.T) {
	for _, tc := range []struct {
		rec  string
		want os.FileMode
	}{
		{"1\t0\t0\t0\t4755\tf\tsu", 0o755 | os.ModeSetuid},
		{"1\t0\t0\t0\t2711\tf\tsg", 0o711 | os.ModeSetgid},
		{"1\t0\t0\t0\t1777\td\ttmp", 0o777 | os.ModeDir | os.ModeSticky},
		{"1\t0\t0\t0\t7000\tf\tall", os.ModeSetuid | os.ModeSetgid | os.ModeSticky},
		{"1\t0\t0\t0\t644\tf\tplain", 0o644},
	} {
		r, ok := parseListRecord(tc.rec)
		if !ok || r.entry.Mode != tc.want {
			t.Errorf("%q: mode %v, %v; want %v", tc.rec, r.entry.Mode, ok, tc.want)
		}
	}
	const sIFREG, sIFDIR = 0o100000, 0o040000
	for m, want := range map[int32]os.FileMode{
		sIFREG | 0o4755: 0o755 | os.ModeSetuid,
		sIFREG | 0o2755: 0o755 | os.ModeSetgid,
		sIFDIR | 0o1777: 0o777 | os.ModeDir | os.ModeSticky,
		sIFREG | 0o640:  0o640,
	} {
		if got := rsyncModeToFileMode(m); got != want {
			t.Errorf("rsync mode %#o: %v, want %v", m, got, want)
		}
	}
}
