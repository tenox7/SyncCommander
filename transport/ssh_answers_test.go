package transport

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/crypto/ssh"

	"sc/internal/sshtest"
)

const testBanner = "Welcome to the test host\n"

func always(s string) func() string { return func() string { return s } }

// answerFiles writes the files the S10 tests list and sum.
func answerFiles(t *testing.T) (dir string, bodies map[string]string) {
	t.Helper()
	dir = t.TempDir()
	bodies = map[string]string{"a.txt": "hello", "b.txt": "a longer file"}
	for name, body := range bodies {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir, bodies
}

func trueSum(algo, body string) string {
	switch algo {
	case "sha256":
		s := sha256.Sum256([]byte(body))
		return hex.EncodeToString(s[:])
	case "sha1":
		s := sha1.Sum([]byte(body))
		return hex.EncodeToString(s[:])
	case "md5":
		s := md5.Sum([]byte(body))
		return hex.EncodeToString(s[:])
	}
	return "no such algorithm: " + algo
}

// S10: scp and rsync+ssh read every listing, stat and sum from what
// commands print. Behind a login script that prints, the script's first
// word was every file's sum, and the first entry of each listing had size
// 0. The connect now stops and says why.
func TestSSHShellBehindABannerDoesNotConnect(t *testing.T) {
	dir, _ := answerFiles(t)
	addr := sshtest.StartWith(t, sshtest.Options{Banner: always(testBanner)})
	for _, scheme := range []string{"scp", "rsync+ssh"} {
		b, err := OpenBackend(scheme+"://x:pw@"+addr+dir, true, 1)
		if err == nil {
			sum, serr := b.Checksum(context.Background(), "a.txt", "sha256")
			entries, lerr := b.List(context.Background(), "")
			CloseBackend(b)
			t.Errorf("%s connected behind a login script that prints: sha256 of a.txt = %q, %v; listing %+v, %v", scheme, sum, serr, entries, lerr)
			continue
		}
		if !strings.Contains(err.Error(), "login script") {
			t.Errorf("%s: %v, want it to name the login script", scheme, err)
		}
	}
}

// S10: a login script can start to print after the connect, as one that
// prints a warning only now and then. Its text ahead of a listing was left
// out, and the first entry read as size 0. The listing now fails and says
// what the server printed.
func TestSSHShellListingBehindABannerFails(t *testing.T) {
	dir, _ := answerFiles(t)
	var banner atomic.Pointer[string]
	addr := sshtest.StartWith(t, sshtest.Options{Banner: func() string {
		if p := banner.Load(); p != nil {
			return *p
		}
		return ""
	}})
	b, err := OpenBackend("scp://x:pw@"+addr+dir, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	text := testBanner
	banner.Store(&text)
	entries, err := b.List(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "Welcome") {
		t.Errorf("listing behind a banner: %+v, %v; want an error that shows the banner", entries, err)
	}
}

// S10: a sum the server prints is checked even when the command passed its
// probe: it must be the algorithm's number of hex digits.
func TestSSHShellChecksumIsASum(t *testing.T) {
	dir, _ := answerFiles(t)
	b, err := OpenBackend("scp://x:pw@"+sshtest.Start(t, false)+dir, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	s := b.(*SCPBackend)
	s.cksumMu.Lock()
	s.cksumCmds = map[string]string{"sha256": "echo Welcome"}
	s.cksumMu.Unlock()
	ctx := context.Background()
	if sum, err := s.Checksum(ctx, "a.txt", "sha256"); err == nil {
		t.Errorf("Checksum = %q from a command that printed Welcome; want an error", sum)
	}
	if sum, err := s.ChecksumRange(ctx, "a.txt", "sha256", 0, 3); err == nil {
		t.Errorf("ChecksumRange = %q from a command that printed Welcome; want an error", sum)
	}
}

// S10: on an account limited to sftp (ForceCommand internal-sftp) every
// command prints nothing and exits 0. rsync+ssh, which lists through the
// shell, connected, and every folder listed as empty: a mirror from it
// deletes the other side.
func TestSSHShellOnAnSFTPOnlyAccountDoesNotConnect(t *testing.T) {
	dir, _ := answerFiles(t)
	b, err := OpenBackend("rsync+ssh://x:pw@"+sshtest.StartWith(t, sshtest.Options{OnlySFTP: true})+dir, true, 1)
	if err == nil {
		entries, lerr := b.List(context.Background(), "")
		CloseBackend(b)
		t.Fatalf("rsync+ssh connected to an account that runs no command: listing %+v, %v", entries, lerr)
	}
	if !strings.Contains(err.Error(), "sftp only") {
		t.Errorf("%v, want it to say the account may allow sftp only", err)
	}
}

// S10: sftp lists through sftp but sums with shell commands. On an account
// limited to sftp every probe "passed" with exit 0, and then every checksum
// failed after its retries. A probe now counts only when it gives the
// right sum of a test word.
func TestSFTPOnAnSFTPOnlyAccountOffersNoServerSums(t *testing.T) {
	dir, bodies := answerFiles(t)
	b, err := OpenBackend("sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{OnlySFTP: true})+dir, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	if entries, err := b.List(ctx, ""); err != nil || len(entries) != len(bodies) {
		t.Fatalf("listing over sftp: %+v, %v", entries, err)
	}
	if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) != 0 {
		t.Errorf("ChecksumAlgos = %v, %v on an account that runs no command, want none", algos, err)
	}
}

// S10: behind a login script that prints, the script's first word was the
// sum of every file, so every file compared equal to every other. Each sum
// the server now offers is the file's.
func TestSFTPBehindABannerGivesNoWrongSums(t *testing.T) {
	dir, bodies := answerFiles(t)
	b, err := OpenBackend("sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{SFTP: true, Banner: always(testBanner)})+dir, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	algos, err := b.ChecksumAlgos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, algo := range algos {
		for name, body := range bodies {
			if got, err := b.Checksum(ctx, name, algo); err != nil || got != trueSum(algo, body) {
				t.Errorf("%s of %s = %q, %v; want %s", algo, name, got, err, trueSum(algo, body))
			}
		}
	}
}

// S10: over sftp a scan's deep listing comes from find in the shell. Behind
// a login script that prints, the first record had the banner ahead of its
// size, which read as 0. That listing is now refused, and the folder lists
// through sftp, with its sizes.
func TestSFTPDeepListingBehindABannerKeepsSizes(t *testing.T) {
	dir, bodies := answerFiles(t)
	b, err := OpenBackend("sftp://x:pw@"+sshtest.StartWith(t, sshtest.Options{SFTP: true, Banner: always(testBanner)})+dir, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	b.(interface {
		PreloadRecursive(context.Context, string) error
	}).PreloadRecursive(ctx, "")
	entries, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	sizes, want := map[string]int64{}, map[string]int64{}
	for _, e := range entries {
		sizes[e.Name] = e.Size
	}
	for name, body := range bodies {
		want[name] = int64(len(body))
	}
	if !maps.Equal(sizes, want) {
		t.Errorf("after the deep listing the sizes are %v, want %v", sizes, want)
	}
}

// S10: a record whose numbers do not read is no record.
func TestParseListRecordRefusesTextThatIsNotARecord(t *testing.T) {
	for _, rec := range []string{
		testBanner + "12\t1700000000\t1\t2\t644\tf\ta.txt",
		testBanner,
		"12\tsoon\t1\t2\t644\tf\ta.txt",
		"12\t1700000000\tsoon\t2\t644\tf\ta.txt",
		"12\t1700000000\t1\tsoon\t644\tf\ta.txt",
		"12\t1700000000.5x\t1\t2\t644\tf\ta.txt",
		"12\t1700000000\t1\t2\trw-\tf\ta.txt",
		"-12\t1700000000\t1\t2\t644\tf\ta.txt",
	} {
		if r, ok := parseListRecord(rec); ok {
			t.Errorf("parseListRecord(%q) = %+v, want no record", rec, r)
		}
	}
	if r, ok := parseListRecord("12\t1700000000.0123456789\t1\t2\t644\tf\ta.txt"); !ok || r.entry.ModTime.Nanosecond() != 12345678 {
		t.Errorf("a record with find's ten-digit fraction: %+v, %v", r, ok)
	}
}

// S10: a sum is the first word of what the command printed, in hex, as
// long as the algorithm's sums are.
func TestSumInReadsOnlyASum(t *testing.T) {
	s := testSums["sha256"]
	for out, want := range map[string]string{
		s + "  -\n":                  s,
		s + "  /x/a.txt\n":           s,
		`\` + s + `  /x/a\nb` + "\n": s,
		strings.ToUpper(s) + "\n":    s,
		testBanner + s + "  -\n":     "",
		"":                           "",
		s[:63] + "  -\n":             "",
		"zz" + s[2:] + "  -\n":       "",
	} {
		if got, ok := sumIn(out, "sha256"); got != want || ok != (want != "") {
			t.Errorf("sumIn(%q) = %q, %v; want %q", out, got, ok, want)
		}
	}
}

// S10: a probe that exits 0 counts only when it printed the right sum of
// the test word; one that exits with a status has no such command.
func TestProbeSSHChecksumsTakesOnlyTheRightSum(t *testing.T) {
	answers := map[string]string{
		"printf test | sha256sum":     "", // exits 0, prints nothing
		"printf test | shasum -a 256": testSums["sha256"] + "  -\n",
		"printf test | sha1sum":       strings.Repeat("0", 40) + "  -\n", // a sum, not of test
		"printf test | shasum":        testBanner + testSums["sha1"] + "  -\n",
		"printf test | md5sum":        testSums["md5"] + "  -\n",
	}
	var wrong []string
	cmds, err := probeSSHChecksums(func(cmd string) (string, error) {
		out, ok := answers[cmd]
		if !ok {
			return "", &ssh.ExitError{}
		}
		return out, nil
	}, func(cmd, _ string) { wrong = append(wrong, cmd) })
	if want := map[string]string{"sha256": "shasum -a 256", "md5": "md5sum"}; err != nil || !maps.Equal(cmds, want) {
		t.Errorf("probe found %v, %v; want %v", cmds, err, want)
	}
	if want := []string{"sha256sum", "sha1sum", "shasum"}; !slices.Equal(wrong, want) {
		t.Errorf("wrong answers from %q, want %q", wrong, want)
	}
	for algo, sum := range testSums {
		if sum != trueSum(algo, "test") {
			t.Errorf("testSums[%s] = %s, want %s", algo, sum, trueSum(algo, "test"))
		}
	}
}
