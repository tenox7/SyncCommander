package transport

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
)

// L25: ftp:// to an IPv6 literal, as ftp://[::1]/, joined host and port as
// "::1:21", which no dial takes: the connect failed. A listing, a checksum
// and a download now reach [::1], their data connections too.
func TestFTPToAnIPv6Literal(t *testing.T) {
	if l, err := net.Listen("tcp", "[::1]:0"); err != nil {
		t.Skipf("no IPv6 loopback here: %v", err)
	} else {
		l.Close()
	}
	f := hashFTPD()
	f.host = "::1"
	f.files = map[string]fakeFTPFile{"/a.txt": {"alpha", "226 Transfer complete."}}
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	ents, err := b.List(ctx, "")
	if err != nil || len(ents) != 3 {
		t.Fatalf("listing of [::1]: %d entries, %v; want 3", len(ents), err)
	}
	if sum, err := b.Checksum(ctx, "a.txt", "sha256"); err != nil || sum != sha256Hex("alpha") {
		t.Errorf("sha256 of a.txt on [::1]: %q, %v; want %s", sum, err, sha256Hex("alpha"))
	}
	r, err := b.Open(ctx, "a.txt")
	if err != nil {
		t.Fatalf("download from [::1]: %v", err)
	}
	body, err := io.ReadAll(r)
	if cerr := r.Close(); err == nil {
		err = cerr
	}
	if err != nil || string(body) != "alpha" {
		t.Errorf("download from [::1]: %q, %v; want alpha", body, err)
	}
}

// L25: the checksum connection sent a name with a line break as it was.
// The server read the rest as a command of its own, and from then on each
// reply was taken for the one before's: a.txt got the sum b.txt's
// command asked for. Such a name is refused now, with nothing sent, and
// the connection goes on as it was.
func TestFTPChecksumOfANameWithALineBreak(t *testing.T) {
	f := hashFTPD()
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	conns := f.connCount()
	if sum, err := b.Checksum(ctx, "x.txt\r\nXSHA256 /b.txt", "sha256"); err == nil {
		t.Errorf("a name with a line break got the sum %q", sum)
	}
	if sum, err := b.Checksum(ctx, "x.txt\nXSHA256 /b.txt", "sha256"); err == nil {
		t.Errorf("a name with a line feed got the sum %q", sum)
	}
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if sum, err := b.Checksum(ctx, name, "sha256"); err != nil || sum != sha256Hex(hashBodies[name]) {
			t.Errorf("sha256 of %s after names with line breaks: %q, %v; want %s", name, sum, err, sha256Hex(hashBodies[name]))
		}
	}
	if n := f.connCount() - conns; n != 0 {
		t.Errorf("%d connections dialed; want none: nothing was sent, the connection is as it was", n)
	}
	for _, line := range f.heardOn(2) {
		if strings.Contains(line, "x.txt") {
			t.Errorf("the server heard %q", line)
		}
	}
}

// L25: the main connection says OPTS UTF8 ON when FEAT names UTF8 (the
// library does); the checksum connection did not. On a server that starts
// in another code page a name beyond ASCII was not found there.
func TestFTPChecksumConnectionSpeaksUTF8(t *testing.T) {
	f := hashFTPD()
	f.utf8 = true
	f.store["/café.txt"] = []byte("crème")
	b := openFakeFTP(t, f, "/")
	if sum, err := b.Checksum(context.Background(), "café.txt", "sha256"); err != nil || sum != sha256Hex("crème") {
		t.Errorf("sha256 of café.txt: %q, %v; want %s", sum, err, sha256Hex("crème"))
	}
}

// hashOnlyFTPD is hashFTPD with HASH in place of XSHA256, SHA-1 its
// first choice.
func hashOnlyFTPD() *fakeFTPD {
	f := hashFTPD()
	f.hashes = false
	f.hashFeat = "HASH SHA-1*;SHA-256;MD5"
	return f
}

func sumOf(algo, body string) string {
	switch algo {
	case "sha1":
		sum := sha1.Sum([]byte(body))
		return hex.EncodeToString(sum[:])
	case "md5":
		sum := md5.Sum([]byte(body))
		return hex.EncodeToString(sum[:])
	}
	return sha256Hex(body)
}

// L25: with HASH, the checksum connection said OPTS HASH before each file:
// a round trip each. The choice holds for the connection, so it is made
// once for each change of algorithm.
func TestFTPHashChoosesTheAlgorithmOnce(t *testing.T) {
	f := hashOnlyFTPD()
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	for _, algo := range []string{"sha256", "sha256", "md5", "md5", "sha256"} {
		for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
			if sum, err := b.Checksum(ctx, name, algo); err != nil || sum != sumOf(algo, hashBodies[name]) {
				t.Errorf("%s of %s: %q, %v; want %s", algo, name, sum, err, sumOf(algo, hashBodies[name]))
			}
		}
	}
	var opts []string
	for _, line := range f.heardOn(2) {
		if strings.HasPrefix(line, "OPTS HASH") {
			opts = append(opts, line)
		}
	}
	if want := []string{"OPTS HASH SHA-256", "OPTS HASH MD5", "OPTS HASH SHA-256"}; !slices.Equal(opts, want) {
		t.Errorf("the checksum connection said %q; want %q", opts, want)
	}
}

// L25: a sum is taken only when it is one of the algorithm asked for. A
// server that refuses OPTS HASH answers HASH with its own choice, one may
// answer with another algorithm than chosen, and a 213 reply may hold
// something else than a sum: each was taken for a sum of the algorithm
// asked, and the file compared as different from its copy.
func TestFTPChecksumIsOfTheAlgorithmAsked(t *testing.T) {
	for _, c := range []struct {
		what string
		set  func(*fakeFTPD)
	}{
		{"OPTS HASH refused", func(f *fakeFTPD) { f.hashRefuses = true }},
		{"HASH answers with another algorithm", func(f *fakeFTPD) { f.hashSays = "SHA-1" }},
		{"HASH answers no hex", func(f *fakeFTPD) { f.sumReply = strings.Repeat("z", 64) }},
		{"HASH answers too few digits", func(f *fakeFTPD) { f.sumReply = "abcd" }},
		{"XSHA256 answers no hex", func(f *fakeFTPD) { f.hashFeat, f.hashes, f.sumReply = "", true, "Done." }},
		{"XSHA256 answers too many digits", func(f *fakeFTPD) { f.hashFeat, f.hashes, f.sumReply = "", true, strings.Repeat("ab", 40) }},
	} {
		t.Run(c.what, func(t *testing.T) {
			f := hashOnlyFTPD()
			c.set(f)
			b := openFakeFTP(t, f, "/")
			for range 2 { // and the next ask too
				if sum, err := b.Checksum(context.Background(), "a.txt", "sha256"); err == nil {
					t.Errorf("sha256 of a.txt: %q, no error", sum)
				}
			}
		})
	}
}
