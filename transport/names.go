package transport

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/text/unicode/norm"

	"sc/model"
)

// probeNames finds how the file system under root matches names (D10),
// writing nothing: it looks a name up again in another spelling and sees
// whether the same file answers. The names tried are those in root, then
// the name of root and of each folder above it, while those are on root's
// file system: a name is looked up by the file system of its folder. What
// no name there has another spelling for stays unknown: Unicode forms, in
// a tree of plain ASCII names.
func probeNames(root string) model.NameRule {
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		return model.NameRule{}
	}
	var p nameProbe
	if f, err := os.Open(real); err == nil {
		for seen := 0; seen < 4096 && !p.done(); {
			ents, err := f.ReadDir(256)
			for _, e := range ents {
				p.try(real, e.Name())
			}
			seen += len(ents)
			if err != nil {
				break
			}
		}
		f.Close()
	}
	rootInfo, err := os.Stat(real)
	for dir := real; err == nil && !p.done(); {
		parent := filepath.Dir(dir)
		info, perr := os.Stat(parent)
		if parent == dir || perr != nil || !sameDevice(rootInfo, info) {
			break
		}
		p.try(parent, filepath.Base(dir))
		dir = parent
	}
	return p.rule
}

type nameProbe struct{ rule model.NameRule }

func (p *nameProbe) done() bool {
	return p.rule.Case != model.FoldUnknown && p.rule.Forms != model.FoldUnknown
}

// try looks name, in dir, up in the other letter case and the other
// Unicode form, for what is not known yet.
func (p *nameProbe) try(dir, name string) {
	if p.rule.Case == model.FoldUnknown {
		if alt := otherCase(name); alt != "" {
			p.rule.Case = oneName(dir, name, alt)
		}
	}
	if p.rule.Forms == model.FoldUnknown {
		if alt := otherForm(name); alt != "" {
			p.rule.Forms = oneName(dir, name, alt)
		}
	}
}

// oneName reports whether alt, in dir, is the file name is.
func oneName(dir, name, alt string) model.Fold {
	a, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return model.FoldUnknown
	}
	b, err := os.Lstat(filepath.Join(dir, alt))
	switch {
	case err == nil && os.SameFile(a, b):
		return model.Folded
	case err == nil, errors.Is(err, fs.ErrNotExist):
		return model.Apart
	}
	return model.FoldUnknown
}

// otherCase is name with the case of each ASCII letter swapped, "" when it
// has none. Every file system that folds case folds ASCII; beyond it they
// differ.
func otherCase(name string) string {
	b := []byte(name)
	swapped := false
	for i, c := range b {
		switch {
		case 'a' <= c && c <= 'z':
			b[i], swapped = c-'a'+'A', true
		case 'A' <= c && c <= 'Z':
			b[i], swapped = c-'A'+'a', true
		}
	}
	if !swapped {
		return ""
	}
	return string(b)
}

// otherForm is name in the other Unicode form, "" when both forms are the
// same bytes.
func otherForm(name string) string {
	if d := norm.NFD.String(name); d != name {
		return d
	}
	if c := norm.NFC.String(name); c != name {
		return c
	}
	return ""
}
