package model

import "testing"

// D10: names with the same Key are one name on the file system of the
// rule; what the rule does not know is not folded.
func TestNameRuleKey(t *testing.T) {
	nfc, nfd := "café", "café"
	unknown := NameRule{}
	apart := NameRule{Case: Apart, Forms: Apart}
	caseFolds := NameRule{Case: Folded, Forms: FoldUnknown}
	formsFold := NameRule{Case: Apart, Forms: Folded}
	for _, c := range []struct {
		what  string
		rule  NameRule
		a, b  string
		folds bool
		one   bool
	}{
		{"unknown, case", unknown, "README", "readme", false, false},
		{"unknown, forms", unknown, nfc, nfd, false, false},
		{"apart, case", apart, "README", "readme", false, false},
		{"apart, forms", apart, nfc, nfd, false, false},
		{"case folded", caseFolds, "README", "readme", true, true},
		{"case folded, beyond ASCII", caseFolds, "ÄRGER.txt", "ärger.txt", true, true},
		{"case folded, other names", caseFolds, "a.txt", "b.txt", true, false},
		{"case folded, forms not known", caseFolds, nfc, nfd, true, false},
		{"forms folded", formsFold, nfc, nfd, true, true},
		{"forms folded, case apart", formsFold, "README", "readme", true, false},
		{"both folded", NameRule{Case: Folded, Forms: Folded}, "CAFÉ", nfd, true, true},
	} {
		if got := c.rule.Folds(); got != c.folds {
			t.Errorf("%s: Folds = %v, want %v", c.what, got, c.folds)
		}
		if one := c.rule.Key(c.a) == c.rule.Key(c.b); one != c.one {
			t.Errorf("%s: %q and %q one name = %v, want %v", c.what, c.a, c.b, one, c.one)
		}
	}
}
