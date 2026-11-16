package transfer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sc/model"
	"sc/transport"
)

// namesAs is a destination that matches names by rule, whatever the
// backend under it does.
type namesAs struct {
	model.Backend
	rule model.NameRule
}

func (n namesAs) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Names: n.rule}, nil
}

// twinSource holds names that are one name where letter case or Unicode
// forms fold, as a Linux disk can: the fake backend matches names byte for
// byte.
func twinSource(t *testing.T) model.Backend {
	t.Helper()
	ctx := context.Background()
	src, err := transport.NewFakeBackend("fake://src?dirs=0&files=0")
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range twinFiles {
		if err := src.Mkdir(ctx, model.DirOf(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := src.CopyFrom(ctx, name, strings.NewReader(body), model.PutParams{Size: int64(len(body)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	return src
}

var twinFiles = map[string]string{
	"d/A.txt":     "upper case",
	"d/a.txt":     "lower case",
	"d/b.txt":     "no twin",
	"d/café.txt":  "composed",
	"d/café.txt": "decomposed",
	"d/Sub/x.txt": "in Sub",
	"d/sub/y.txt": "in sub",
}

var (
	caseTwins = []string{"d/A.txt", "d/a.txt", "d/Sub/x.txt", "d/sub/y.txt"}
	formTwins = []string{"d/café.txt", "d/café.txt"}
)

// holds returns what b holds at name, looked up by its name: the fake
// backend lists no folder it was not told to make.
func holds(t *testing.T, b model.Backend, name string) (string, bool) {
	t.Helper()
	ctx := context.Background()
	if _, err := b.Stat(ctx, name); err != nil {
		return "", false
	}
	r, err := b.Open(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(body), true
}

// filesIn lists the files under b's root.
func filesIn(t *testing.T, b model.Backend, dir string) []string {
	t.Helper()
	ents, err := b.List(model.WithFreshListing(context.Background()), dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		rel := strings.TrimPrefix(dir+"/"+e.Name, "/")
		if e.IsDir {
			out = append(out, filesIn(t, b, rel)...)
		} else {
			out = append(out, rel)
		}
	}
	slices.Sort(out)
	return out
}

// D10: a destination that folds letter case or Unicode forms takes two
// source names for one. The copy wrote that one file twice, the second
// over the first, and logged both "OK": one of the two files was gone from
// the destination, and nothing said so. Now both names are refused, as a
// failure that says why, and the rest is copied. Letter case is folded only
// where the destination is known to fold it; Unicode forms unless it is
// known to keep them apart.
func TestCopyRefusesNamesTheDestinationTakesForOne(t *testing.T) {
	for _, c := range []struct {
		what    string
		rule    model.NameRule
		refused []string
	}{
		{"case and forms fold", model.NameRule{Case: model.Folded, Forms: model.Folded}, append(slices.Clone(caseTwins), formTwins...)},
		{"case folds, forms apart", model.NameRule{Case: model.Folded, Forms: model.Apart}, caseTwins},
		{"case-sensitive, forms apart", model.NameRule{Case: model.Apart, Forms: model.Apart}, nil},
		{"not known, as for a remote", model.NameRule{}, formTwins},
	} {
		t.Run(c.what, func(t *testing.T) {
			src := twinSource(t)
			fake, err := transport.NewFakeBackend("fake://dst?dirs=0&files=0")
			if err != nil {
				t.Fatal(err)
			}
			dst := namesAs{Backend: fake, rule: c.rule}
			s := scanned(t, "", "", src, dst)
			mark := len(transport.Log.Lines())
			p := s.copy(t, "d", true, false)

			for name, body := range twinFiles {
				got, ok := holds(t, fake, name)
				switch refused := slices.Contains(c.refused, name); {
				case refused && ok:
					t.Errorf("%s was written, holding %q; it was to be refused", name, got)
				case !refused && got != body:
					t.Errorf("%s holds %q (there: %v), want %q", name, got, ok, body)
				}
			}
			if got := p.Failed.Load(); got != int64(len(c.refused)) {
				t.Errorf("failed=%d, want %d (one for each refused name)", got, len(c.refused))
			}
			for _, name := range c.refused {
				if !logged(mark, "COPY "+name+": refused, the destination ") {
					t.Errorf("no refusal logged for %s", name)
				}
			}
			if slices.Contains(c.refused, "d/A.txt") && !logged(mark, "COPY d/A.txt: refused, the destination takes d/A.txt and d/a.txt for one name") {
				t.Error("the refusal of d/A.txt does not name d/a.txt")
			}
			if slices.Contains(c.refused, "d/sub/y.txt") && !logged(mark, "COPY d/sub/y.txt: refused, the destination takes d/sub and d/Sub for one name") {
				t.Error("the refusal of d/sub/y.txt does not name the folders")
			}
		})
	}
}

// D10, on this disk: a local destination finds out its own rule, and the
// copy keeps what it cannot hold apart off it. On a Mac letter case folds,
// on Linux it does not; an empty destination has no name to tell its
// Unicode forms by, so the two forms are refused on both.
func TestCopyToThisDiskKeepsTwinsApart(t *testing.T) {
	truth := t.TempDir()
	writeFile(t, filepath.Join(truth, "probe"), "x")
	_, err := os.Lstat(filepath.Join(truth, "PROBE"))
	foldsCase := err == nil
	t.Logf("this disk folds letter case: %v", foldsCase)

	right := t.TempDir()
	dst := transport.NewLocalBackend(right)
	s := scanned(t, "", right, twinSource(t), dst)
	p := s.copy(t, "d", true, false)

	refused := slices.Clone(formTwins)
	if foldsCase {
		refused = append(refused, caseTwins...)
	}
	var want []string
	for name := range twinFiles {
		if !slices.Contains(refused, name) {
			want = append(want, name)
			if got := readFile(t, filepath.Join(right, filepath.FromSlash(name))); got != twinFiles[name] {
				t.Errorf("%s holds %q, want %q", name, got, twinFiles[name])
			}
		}
	}
	slices.Sort(want)
	if got := filesIn(t, dst, ""); !slices.Equal(got, want) {
		t.Errorf("the destination holds %q, want %q", got, want)
	}
	if got := p.Failed.Load(); got != int64(len(refused)) {
		t.Errorf("failed=%d, want %d", got, len(refused))
	}
}
