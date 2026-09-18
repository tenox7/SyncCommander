package transport

import "testing"

// gorsync parses the rsync:// form with net/url, so a name holding '#', '?'
// or '%' must be escaped per segment or the path is cut or corrupted.
func TestRemoteURLEscapesPathSegments(t *testing.T) {
	b, err := NewRsyncBackend("rsync://u:p%40ss@host:8730/mod/base dir")
	if err != nil {
		t.Fatal(err)
	}
	got := b.remoteURL("a#1/b?c/100%.txt")
	want := "rsync://u:p%2540ss@host:8730/mod/base%20dir/a%231/b%3Fc/100%25.txt"
	if got != want {
		t.Errorf("remoteURL =\n %s\nwant\n %s", got, want)
	}
	if got := b.modulePath("x", true); got != "mod/base dir/x/" {
		t.Errorf("modulePath = %q", got)
	}
}

func TestDaemonProtocolGreeting(t *testing.T) {
	for in, want := range map[string]int{
		"@RSYNCD: 27": 27,
		"@RSYNCD: 31.0 sha512 sha256 sha1 md5 md4": 31,
	} {
		if got, err := daemonProtocol(in); err != nil || got != want {
			t.Errorf("daemonProtocol(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "hello", "@RSYNCD: x"} {
		if _, err := daemonProtocol(in); err == nil {
			t.Errorf("daemonProtocol(%q) accepted", in)
		}
	}
}
