package transport

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// sentSince is the commands proto sent since the log held mark lines.
func sentSince(mark int, proto string) []string {
	var sent []string
	for _, line := range Log.Lines()[mark:] {
		if strings.Contains(line, " "+proto+" >>> ") {
			sent = append(sent, line)
		}
	}
	return sent
}

// S19: the shell summed each file with a command of its own, a round trip
// for each: hours for a million small files at 50 ms. Many files are summed
// with one command, sh, which gets the script on its input: the names,
// with spaces, quotes, a newline and a leading dash as they are, stay out
// of the command line and the log. A link, a folder, a named pipe (which
// would hold the command up), a file that cannot be read and one that is
// gone get no sum.
func TestShellSumsManyFilesInOneCommand(t *testing.T) {
	root := t.TempDir()
	bodies := map[string]string{"a b.txt": "a", "it's.txt": "q", "new\nline.txt": "n", "-dash.txt": "d", "deep/er/x.bin": "deep", "secret": "s"}
	for p, body := range bodies {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("a b.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("mkfifo", filepath.Join(root, "pipe")).Run(); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	unreadable := os.Getuid() != 0
	if unreadable {
		secret := filepath.Join(root, "secret")
		if err := os.Chmod(secret, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(secret, 0o644) })
	}
	b, err := OpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if caps, err := b.Capabilities(ctx); err != nil || !caps.BatchSums {
		t.Errorf("scp's caps = %+v, %v; want BatchSums", caps, err)
	}
	cb, ok := b.(model.ChecksumBatcher)
	if !ok {
		t.Fatal("scp sums no batch of files")
	}
	algos, err := b.ChecksumAlgos(ctx)
	if err != nil || len(algos) == 0 {
		t.Skipf("the shell here has no checksum command: %v", err)
	}
	rels := []string{"a b.txt", "it's.txt", "new\nline.txt", "-dash.txt", "deep/er/x.bin", "secret", "link", "dir", "pipe", "gone.txt"}
	for _, algo := range algos {
		t.Run(algo, func(t *testing.T) {
			want := map[string]string{}
			for name, body := range bodies {
				if name != "secret" || !unreadable {
					want[name] = sumOf(algo, body)
				}
			}
			mark := len(Log.Lines())
			got, err := cb.ChecksumFiles(ctx, rels, algo)
			if err != nil || !maps.Equal(got, want) {
				t.Errorf("sums = %q, %v; want %q", got, err, want)
			}
			if sent := sentSince(mark, "scp"); len(sent) != 1 || !strings.HasSuffix(sent[0], " >>> sh") {
				t.Errorf("sent %q; want one command, sh, with no names in it", sent)
			}
		})
	}
}

// An answer that is not the one asked for, as from a checksum command that
// prints more than its line, or nothing, or a shell that fails on the
// script, is not read on: the sums before it stand, the rest are left to
// the caller, and the connection sums file by file from then on, sending
// no batch.
func TestShellSumsStopAtAnAnswerNotAskedFor(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{"a.txt": "A", "b.txt": "B", "c.txt": "C"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	addr := startExecSSHServer(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name, wrap string
		want       []string
	}{
		{"a line more", "sh -c '%s; echo one more'", []string{"a.txt"}},
		{"no line", "sh -c '%s | tr -d \"\\n\"'", nil},
		{"nothing", "sh -c 'cat >/dev/null'%.0s", nil},
		{"a script the shell cannot read", "%.0s)", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := OpenBackend("scp://x:pw@"+addr+root, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			sh := &b.(*SCPBackend).sshShell
			cmds, err := sh.checksumCmds(ctx)
			if err != nil || len(cmds) == 0 {
				t.Skipf("the shell here has no checksum command: %v", err)
			}
			algo := ""
			for _, a := range sshChecksumOrder {
				if cmds[a] != "" {
					algo = a
					break
				}
			}
			sh.cksumMu.Lock()
			sh.cksumCmds = map[string]string{algo: fmt.Sprintf(tc.wrap, cmds[algo])}
			sh.cksumMu.Unlock()
			want := map[string]string{}
			for _, name := range tc.want {
				want[name] = sumOf(algo, strings.ToUpper(strings.TrimSuffix(name, ".txt")))
			}
			got, err := sh.ChecksumFiles(ctx, []string{"a.txt", "b.txt", "c.txt"}, algo)
			if !errors.Is(err, ErrUnsupported) || !maps.Equal(got, want) {
				t.Errorf("sums = %q, %v; want %q and ErrUnsupported", got, err, want)
			}
			mark := len(Log.Lines())
			if got, err := sh.ChecksumFiles(ctx, []string{"a.txt"}, algo); !errors.Is(err, ErrUnsupported) || len(got) != 0 {
				t.Errorf("the next batch gave %q, %v; want none and ErrUnsupported", got, err)
			}
			if sent := sentSince(mark, "scp"); len(sent) != 0 {
				t.Errorf("the next batch sent %q; want nothing", sent)
			}
		})
	}
}

// A batch in an algorithm the shell has no command for, as rsync's md4, is
// no failure to try again: it is ErrUnsupported at once, with no command
// sent, through the wrapper that holds every remote too.
func TestShellSumsNoBatchInAnAlgorithmItLacks(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "A")
	b, err := TryOpenBackend("scp://x:pw@"+startExecSSHServer(t)+root, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) == 0 {
		t.Skipf("the shell here has no checksum command: %v", err)
	}
	cb, ok := b.(model.ChecksumBatcher)
	if !ok {
		t.Fatal("the wrapper sums no batch of files")
	}
	mark := len(Log.Lines())
	start := time.Now()
	got, err := cb.ChecksumFiles(ctx, []string{"a.txt"}, "md4")
	if took := time.Since(start); !errors.Is(err, ErrUnsupported) || len(got) != 0 || took > time.Second {
		t.Errorf("an md4 batch gave %q, %v after %v; want none and ErrUnsupported at once", got, err, took)
	}
	if sent := sentSince(mark, "scp"); len(sent) != 0 {
		t.Errorf("an md4 batch sent %q; want nothing", sent)
	}
}

// S19, end to end: a scan with checksums over scp, through the wrapper that
// holds every remote, sums the remote side's files with one command, where
// it sent one for each file.
func TestScanOverSCPSumsInOneCommand(t *testing.T) {
	ctx := context.Background()
	left, right := t.TempDir(), t.TempDir()
	var files []string
	for i := range 40 {
		files = append(files, fmt.Sprintf("d/f%02d.txt", i))
	}
	for _, rel := range files {
		writeFile(t, filepath.Join(left, rel), "body of "+rel)
		writeFile(t, filepath.Join(right, rel), "body of "+rel)
	}
	remote, err := TryOpenBackend("scp://x:pw@"+startExecSSHServer(t)+right, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(remote)
	if algos, err := remote.ChecksumAlgos(ctx); err != nil || len(algos) == 0 {
		t.Skipf("the shell here has no checksum command: %v", err)
	}
	s := model.NewScanner(NewLocalBackend(left), remote, 4, 1, true)
	mark := len(Log.Lines())
	s.Scan(ctx, model.CompareOpts{Size: true, ModTime: true, Checksum: true})
	var batches, perFile []string
	for _, line := range sentSince(mark, "scp") {
		switch {
		case strings.HasSuffix(line, " >>> sh"):
			batches = append(batches, line)
		case strings.Contains(line, "/d/f"):
			perFile = append(perFile, line)
		}
	}
	if len(batches) != 1 || len(perFile) != 0 {
		t.Errorf("the scan sent %d batches and %d commands that name a file: %q; want 1 batch", len(batches), len(perFile), perFile)
	}
	s.ReadTree(func(root *model.TreeNode) {
		var walk func(n *model.TreeNode)
		walk = func(n *model.TreeNode) {
			if !n.IsDir && n.Compare.Checksum != model.AttrEqual {
				t.Errorf("%s by checksum: %v, want equal", n.RelPath, n.Compare.Checksum)
			}
			for _, c := range n.Children {
				walk(c)
			}
		}
		walk(root)
	})
}
