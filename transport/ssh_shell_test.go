package transport

import (
	"errors"
	"os"
	"slices"
	"testing"

	"sc/model"
)

// Names may hold tabs and newlines; the record puts the name last and ends in
// NUL so nothing inside it can be mistaken for a separator.
func TestParseFindRecordKeepsTabsAndNewlinesInNames(t *testing.T) {
	r, ok := parseListRecord("12\t1700000000.5\t1700000001\t1700000002\t644\tf\tweird\tname\nhere")
	if !ok || r.name != "weird\tname\nhere" {
		t.Fatalf("name = %q ok=%v", r.name, ok)
	}
	if e := r.entry; e.Size != 12 || e.IsDir || e.Mode != 0o644 || e.ModTime.Unix() != 1700000000 || e.ModTime.Nanosecond() != 500000000 {
		t.Errorf("entry = %+v", e)
	}
	// L2, L9: links, pipes, sockets and devices are listed with their type,
	// never as files.
	for letter, want := range map[string]os.FileMode{"l": os.ModeSymlink, "p": os.ModeNamedPipe, "s": os.ModeSocket, "b": os.ModeDevice, "c": os.ModeDevice | os.ModeCharDevice, "o": os.ModeIrregular} {
		e, ok := parseListRecord("1\t2\t3\t4\t777\t" + letter + "\tx")
		if !ok || e.entry.IsDir || e.entry.Mode.Type() != want || !e.entry.IsSpecial() {
			t.Errorf("record of type %s = %+v ok=%v; want mode type %v, special", letter, e.entry, ok, want)
		}
	}
	if d, ok := parseListRecord("0\t2\t3\t4\t755\td\tdir"); !ok || !d.entry.IsDir || !d.entry.Mode.IsDir() || d.unread {
		t.Errorf("dir record = %+v ok=%v", d, ok)
	}
	if d, ok := parseListRecord("0\t2\t3\t4\t700\tD\tlocked"); !ok || !d.entry.IsDir || !d.unread {
		t.Errorf("unreadable dir record = %+v ok=%v", d, ok)
	}
	if e, ok := parseListRecord("Permission denied\t0\t0\t0\t0\t!\t/x/secret"); !ok || e.err != "Permission denied" || e.name != "/x/secret" {
		t.Errorf("error record = %+v ok=%v", e, ok)
	}
}

// S2: when the home cannot be found the connect fails; it used to fall
// back to "/" (or take a broken answer as the base), and the tree on
// screen then mapped onto another folder.
func TestExpandHome(t *testing.T) {
	answer := func(h string) func() (string, error) { return func() (string, error) { return h, nil } }
	home := answer("/home/u")
	broken := func() (string, error) { return "", errors.New("no shell") }
	for _, tc := range []struct {
		in, want string
		home     func() (string, error)
	}{
		{"", "/home/u", home}, {"/", "/home/u", home}, {"/~", "/home/u", home},
		{"/~/x/y", "/home/u/x/y", home}, {"/srv", "/srv", home},
		{"/srv", "/srv", broken}, {"/srv/~/x", "/srv/~/x", broken},
		{"/", "", broken}, {"", "", broken}, {"/~", "", broken}, {"/~/x", "", broken},
		{"/", "", answer("")}, {"/", "", answer("$HOME")},
		{"/~/x", "", answer("Welcome to nas\n/root")},
	} {
		got, err := expandHome(tc.in, tc.home)
		if got != tc.want || (err != nil) != (tc.want == "") {
			t.Errorf("expandHome(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestShellQuoteAndTruncateCmd(t *testing.T) {
	if got := shellQuote("it's"); got != `'it'\''s'` {
		t.Errorf("shellQuote = %s", got)
	}
	if got := truncateCmd("/a b", 42); got != "dd if=/dev/null of='/a b' bs=1 seek=42 2>/dev/null" {
		t.Errorf("truncateCmd = %s", got)
	}
}

func listerRec(typ, p string) string { return "0\t1\t1\t1\t755\t" + typ + "\t" + p }
func perlErrRec(p string) string     { return "Permission denied\t0\t0\t0\t0\t!\t" + p }

// S1: the shell listers report what they cannot read in order with the
// entries. Only the folder a report makes short is held back and dropped at
// the end; the others are still released as the stream leaves them.
func TestShellListingSkipsWhatItCannotRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		recs   []string
		early  []string // released before the run ended
		unread []string // never released; the cache drops them
		whole  bool     // the whole run fails
	}{
		{name: "nothing wrong",
			recs:  []string{listerRec("d", "/r/a"), listerRec("f", "/r/a/1"), listerRec("d", "/r/b"), listerRec("f", "/r/b/2")},
			early: []string{"a"}},
		{name: "find: a folder it cannot list",
			recs:   []string{listerRec("d", "/r/a"), listerRec("f", "/r/a/1"), listerRec("D", "/r/b"), listerRec("d", "/r/c"), listerRec("f", "/r/c/2"), listerRec("f", "/r/z")},
			early:  []string{"a", "c"},
			unread: []string{"b"}},
		{name: "perl: a folder it cannot open",
			recs:   []string{listerRec("d", "/r/a"), listerRec("f", "/r/a/1"), listerRec("d", "/r/b"), perlErrRec("/r/b"), listerRec("d", "/r/c"), listerRec("f", "/r/c/2"), listerRec("f", "/r/z")},
			early:  []string{"a", "c"},
			unread: []string{"b"}},
		{name: "perl: an entry it cannot stat",
			recs:   []string{listerRec("d", "/r/a"), listerRec("d", "/r/a/b"), listerRec("f", "/r/a/b/1"), perlErrRec("/r/a/x"), listerRec("f", "/r/a/y"), listerRec("d", "/r/c"), listerRec("f", "/r/c/2")},
			early:  []string{"a/b"},
			unread: []string{"a"}},
		{name: "perl: the base itself",
			recs:   []string{perlErrRec("/r")},
			unread: []string{""}},
		{name: "a report naming nothing listed",
			recs:  []string{listerRec("d", "/r/a"), perlErrRec("/r/q/z"), listerRec("f", "/r/a/1"), listerRec("d", "/r/b"), listerRec("f", "/r/b/2")},
			whole: true},
		{name: "a path outside the base",
			recs:  []string{listerRec("d", "/elsewhere"), listerRec("d", "/r/a"), listerRec("d", "/r/b")},
			whole: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var completed []string
			l := newShellListing("", "/r", func(string, []model.FileEntry) {}, func(d string) { completed = append(completed, d) })
			for _, rec := range tc.recs {
				l.add(rec, "")
			}
			if early := slices.Clone(completed); !slices.Equal(early, tc.early) {
				t.Errorf("released during the run: %q, want %q", early, tc.early)
			}
			err := l.finish("test")
			var unread *unreadDirs
			switch {
			case tc.whole:
				if !errors.Is(err, ErrIncompleteListing) || errors.As(err, &unread) {
					t.Errorf("err = %v, want the whole run to fail", err)
				}
			case tc.unread == nil:
				if err != nil {
					t.Errorf("err = %v", err)
				}
			case !errors.As(err, &unread) || !slices.Equal(unread.dirs, tc.unread):
				t.Errorf("err = %v, want unread folders %q", err, tc.unread)
			}
			for _, d := range tc.unread {
				if slices.Contains(completed, d) {
					t.Errorf("%q was released although the lister could not read it", d)
				}
			}
		})
	}
}
