package ui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"sc/model"
	"sc/transport"
)

// namedSumsBackend is a local root that gives sha256 sums alone and says
// they are its objects' names, as restic does.
type namedSumsBackend struct{ model.Backend }

func (b namedSumsBackend) ChecksumAlgos(context.Context) ([]string, error) {
	return []string{"sha256"}, nil
}

func (b namedSumsBackend) Capabilities(ctx context.Context) (model.Caps, error) {
	caps, err := b.Backend.Capabilities(ctx)
	caps.NamedSums = true
	return caps, err
}

// checksumOption is the Settings line of the checksum option.
func checksumOption(m *Model) string {
	for _, o := range m.settings.options {
		if strings.HasPrefix(o.Label, "Checksum") {
			return o.Label
		}
	}
	return ""
}

// W-M3: a compare with a side whose sums are its objects' names (restic)
// checks the names, not the content. The status bar, the Settings line and
// the info dialog showed it as a SHA check of the content. Two sides that
// read their content show no names.
func TestNamedSumsAreShownAsNames(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	for _, dir := range []string{left, right} {
		writeTestFile(t, filepath.Join(dir, "f.txt"), "same")
	}
	for _, c := range []struct {
		name  string
		right model.Backend
		crc   string
		named bool
	}{
		{"names on the right", namedSumsBackend{transport.NewLocalBackend(right)}, "CRC:NAME", true},
		{"content on both sides", transport.NewLocalBackend(right), "CRC:XXH3", false},
	} {
		m := newTestModel(t, transport.NewLocalBackend(left), c.right)
		m.cmpOpts.Checksum = true
		m.scanner.Scan(context.Background(), *m.cmpOpts)
		m.refreshTreeNow()
		if m.scanner.ChecksumAlgo() == "" {
			t.Fatalf("%s: the sides agreed on no checksum", c.name)
		}
		if bar := RenderStatusBar(m.buildStatus(m.scanner.Progress()), 300); !strings.Contains(bar, c.crc) {
			t.Errorf("%s: the status bar says %q; want %s", c.name, bar, c.crc)
		}
		press(m, "=")
		if label := checksumOption(m); strings.Contains(label, "R: names") != c.named {
			t.Errorf("%s: the Settings line says %q; names on the right: %v", c.name, label, c.named)
		}
		m.settings.Close()
		press(m, "i")
		if view := m.info.View(160, 40); strings.Contains(view, "R: the sums are the object names") != c.named {
			t.Errorf("%s: the info dialog, names on the right: %v:\n%s", c.name, c.named, view)
		}
	}
}
