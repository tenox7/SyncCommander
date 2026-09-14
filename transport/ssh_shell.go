package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

// sshShell is the shell layer the ssh-family backends share: metadata ops run
// as remote commands and file bodies stream through cat and tail. SCP and
// rsync+ssh embed it whole; SFTP uses it for the checksum commands only.
type sshShell struct {
	client    *ssh.Client
	proto     string
	base      string
	display   string
	cksumAlgo string
	cksumCmds map[string]string
	listCache *listCache // nil when the backend has no recursive preload
	usePerl   atomic.Bool
}

func (s *sshShell) BasePath() string { return s.display }

func (s *sshShell) abs(relPath string) string { return path.Join(s.base, relPath) }

// exec runs cmd on client with the given stdin and stdout. A failure carries
// the command's stderr, which is what "exited with status 1" hides.
func (s *sshShell) exec(ctx context.Context, client *ssh.Client, cmd string, stdin io.Reader, stdout io.Writer) error {
	Log.Add(s.proto, ">>>", cmd)
	session, err := client.NewSession()
	if err != nil {
		Log.Add(s.proto, "ERR", err.Error())
		return err
	}
	defer session.Close()
	defer CancelCloser(ctx, session)()
	var stderr bytes.Buffer
	session.Stdin, session.Stdout, session.Stderr = stdin, stdout, &stderr
	if err := session.Run(cmd); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			err = errors.New(msg)
		}
		Log.Add(s.proto, "ERR", err.Error())
		return err
	}
	return nil
}

// run executes cmd on the primary connection and returns its stdout.
func (s *sshShell) run(ctx context.Context, cmd string) (string, error) {
	var out bytes.Buffer
	if err := s.exec(ctx, s.client, cmd, nil, &out); err != nil {
		return "", err
	}
	if t := strings.TrimRight(out.String(), "\n"); t != "" && !strings.ContainsAny(t, "\n\x00") {
		Log.Add(s.proto, "<<<", t)
	}
	return out.String(), nil
}

func (s *sshShell) home() (string, error) {
	out, err := s.run(context.Background(), "echo $HOME")
	return strings.TrimSpace(out), err
}

// expandHome resolves the URL spellings that mean the login home: "", "/"
// and "/~" are the home itself, "/~/x" is x below it.
func expandHome(remotePath string, home func() (string, error)) string {
	switch {
	case remotePath == "" || remotePath == "/" || remotePath == "/~":
		if h, err := home(); err == nil {
			return h
		}
		return "/"
	case strings.HasPrefix(remotePath, "/~/"):
		if h, err := home(); err == nil {
			return path.Join(h, remotePath[3:])
		}
	}
	return remotePath
}

// findRecord lays out size, mtime, atime, ctime, octal mode and type letter;
// the name comes last and the record ends in NUL, so tabs and newlines in
// names survive.
const findRecord = `%s\t%T@\t%A@\t%C@\t%m\t%y\t`

// perlLister prints findRecord-shaped records on hosts whose find has no
// -printf (BSD, macOS). Its arguments are the directory and 1 to recurse; the
// name is the bare entry at one level and the full path when recursing,
// matching find's %f and %p.
const perlLister = `perl -e 'sub w{my($d,$r)=@_;opendir(my $h,$d)or return;for(readdir $h){next if $_ eq "."||$_ eq "..";my $p="$d/$_";my @s=lstat($p)or next;my $t=-l _?"l":-d _?"d":"f";printf "%d\t%d\t%d\t%d\t%o\t%s\t%s\0",$s[7],$s[9],$s[8],$s[10],$s[2]&07777,$t,$r?$p:$_;w($p,$r)if $r&&$t eq "d"}}w($ARGV[0],$ARGV[1])'`

func (s *sshShell) listCmd(dir string, recursive bool) string {
	if s.usePerl.Load() {
		flag := "0"
		if recursive {
			flag = "1"
		}
		return perlLister + " " + shellQuote(dir) + " " + flag
	}
	depth, name := " -maxdepth 1", "%f"
	if recursive {
		depth, name = "", "%p"
	}
	return fmt.Sprintf(`find %s%s -mindepth 1 -printf '%s%s\0'`, shellQuote(dir), depth, findRecord, name)
}

// list runs the directory lister into stdout, switching to the perl form for
// good once find rejects -printf.
func (s *sshShell) list(ctx context.Context, dir string, recursive bool, stdout io.Writer) error {
	err := s.exec(ctx, s.client, s.listCmd(dir, recursive), nil, stdout)
	if err == nil || s.usePerl.Load() || !strings.Contains(err.Error(), "printf") {
		return err
	}
	s.usePerl.Store(true)
	return s.exec(ctx, s.client, s.listCmd(dir, recursive), nil, stdout)
}

func (s *sshShell) findList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	var out bytes.Buffer
	if err := s.list(ctx, s.abs(relDir), false, &out); err != nil {
		return nil, err
	}
	var result []model.FileEntry
	for _, rec := range strings.Split(out.String(), "\x00") {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		name, e, ok := parseFindRecord(rec)
		if !ok {
			continue
		}
		e.RelPath, e.Name = path.Join(relDir, name), name
		result = append(result, e)
	}
	return result, nil
}

// findRecursive streams every entry under scope into emit as the listing
// arrives, grouped by parent.
func (s *sshShell) findRecursive(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	absPrefix := s.abs(scope)
	g := &emitGrouper{emit: emit}
	w := newRecordWriter(0, func(rec string) {
		full, e, ok := parseFindRecord(rec)
		rel := strings.TrimPrefix(strings.TrimPrefix(full, absPrefix), "/")
		if !ok || rel == "" {
			return
		}
		e.RelPath, e.Name = path.Join(scope, rel), path.Base(rel)
		g.add(e)
	})
	err := s.list(ctx, absPrefix, true, w)
	w.flush()
	g.finish()
	return err
}

// parseFindRecord decodes one lister record; the trailing name field may
// contain anything but NUL. Symlinks are skipped like on every backend.
func parseFindRecord(rec string) (name string, e model.FileEntry, ok bool) {
	f := strings.SplitN(rec, "\t", 7)
	if len(f) < 7 || f[5] == "l" {
		return "", model.FileEntry{}, false
	}
	size, _ := strconv.ParseInt(f[0], 10, 64)
	modeVal, _ := strconv.ParseUint(f[4], 8, 32)
	e = model.FileEntry{
		Size:    size,
		ModTime: parseEpoch(f[1]),
		ATime:   parseEpoch(f[2]),
		CTime:   parseEpoch(f[3]),
		IsDir:   f[5] == "d",
		Mode:    os.FileMode(modeVal),
	}
	if e.IsDir {
		e.Mode |= os.ModeDir
	}
	return f[6], e, true
}

func parseEpoch(s string) time.Time {
	sec, frac, _ := strings.Cut(s, ".")
	secs, _ := strconv.ParseInt(sec, 10, 64)
	if frac == "" {
		return time.Unix(secs, 0)
	}
	for len(frac) < 9 {
		frac += "0"
	}
	nsec, _ := strconv.ParseInt(frac[:9], 10, 64)
	return time.Unix(secs, nsec)
}

// recordWriter buffers a stream and calls cb for each complete sep-terminated
// record, so listings are parsed as they arrive rather than buffered whole.
type recordWriter struct {
	sep byte
	buf []byte
	cb  func(string)
}

func newRecordWriter(sep byte, cb func(string)) *recordWriter { return &recordWriter{sep: sep, cb: cb} }

func (w *recordWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, w.sep)
		if i < 0 {
			return len(p), nil
		}
		w.cb(string(w.buf[:i]))
		w.buf = w.buf[i+1:]
	}
}

func (w *recordWriter) flush() {
	if len(w.buf) > 0 {
		w.cb(string(w.buf))
		w.buf = nil
	}
}

var cksumProbes = []struct{ algo, test, cmd string }{
	{"sha256", "echo -n test | sha256sum >/dev/null 2>&1", "sha256sum"},
	{"sha256", "echo -n test | shasum -a 256 >/dev/null 2>&1", "shasum -a 256"},
	{"sha1", "echo -n test | sha1sum >/dev/null 2>&1", "sha1sum"},
	{"sha1", "echo -n test | shasum >/dev/null 2>&1", "shasum"},
	{"md5", "echo -n test | md5sum >/dev/null 2>&1", "md5sum"},
	{"md5", "md5 -q -s test >/dev/null 2>&1", "md5 -q"},
}

var sshChecksumOrder = []string{"sha256", "sha1", "md5"}

// probeSSHChecksums finds a working command per algorithm; the first probe
// that succeeds wins.
func probeSSHChecksums(run func(string) error) map[string]string {
	cmds := make(map[string]string)
	for _, p := range cksumProbes {
		if cmds[p.algo] == "" && run(p.test) == nil {
			cmds[p.algo] = p.cmd
		}
	}
	return cmds
}

func (s *sshShell) ProbeChecksums() []string {
	if s.cksumCmds == nil {
		s.cksumCmds = probeSSHChecksums(func(cmd string) error {
			_, err := s.run(context.Background(), cmd)
			return err
		})
	}
	var algos []string
	for _, a := range sshChecksumOrder {
		if s.cksumCmds[a] != "" {
			algos = append(algos, a)
		}
	}
	return algos
}

func (s *sshShell) SetChecksumAlgo(algo string) { s.cksumAlgo = algo }

func (s *sshShell) Checksum(ctx context.Context, relPath string) (string, error) {
	cmd := s.cksumCmds[s.cksumAlgo]
	if cmd == "" {
		return "", fmt.Errorf("%s: no %q checksum command on the remote", s.proto, s.cksumAlgo)
	}
	out, err := s.run(ctx, cmd+" "+shellQuote(s.abs(relPath)))
	if err != nil {
		return "", err
	}
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return "", errors.New("empty checksum output")
	}
	return strings.TrimPrefix(fields[0], "\\"), nil
}

const touchLayout = "2006-01-02T15:04:05.000000000Z"

func (s *sshShell) SetTimes(ctx context.Context, relPath string, mtime, atime, _ time.Time) error {
	if atime.IsZero() {
		atime = mtime
	}
	fp := shellQuote(s.abs(relPath))
	_, err := s.run(ctx, fmt.Sprintf("touch -m -d %s %s && touch -a -d %s %s",
		shellQuote(mtime.UTC().Format(touchLayout)), fp, shellQuote(atime.UTC().Format(touchLayout)), fp))
	s.listCache.invalidate(parentDir(relPath))
	return err
}

func (s *sshShell) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	fp := shellQuote(s.abs(relPath))
	cmd := "mkdir -p " + fp
	if mode != 0 {
		cmd += fmt.Sprintf(" && chmod %04o %s", mode.Perm(), fp)
	}
	_, err := s.run(ctx, cmd)
	s.listCache.invalidateAncestors(relPath)
	return err
}

// Rename refuses an existing directory at the destination, where mv would
// silently move the source inside it.
func (s *sshShell) Rename(ctx context.Context, oldRel, newRel string) error {
	src, dst := shellQuote(s.abs(oldRel)), shellQuote(s.abs(newRel))
	_, err := s.run(ctx, fmt.Sprintf("if [ -d %s ]; then echo destination is a directory >&2; exit 1; fi; mv %s %s", dst, src, dst))
	s.listCache.invalidateTree(oldRel)
	s.listCache.invalidate(parentDir(oldRel))
	s.listCache.invalidate(parentDir(newRel))
	return err
}

func (s *sshShell) Remove(ctx context.Context, relPath string) error {
	_, err := s.run(ctx, "rm "+shellQuote(s.abs(relPath)))
	s.listCache.invalidate(parentDir(relPath))
	return err
}

func (s *sshShell) RemoveAll(ctx context.Context, relPath string) error {
	if isBaseRel(relPath) {
		return fmt.Errorf("%s: refusing to remove the base directory", s.proto)
	}
	_, err := s.run(ctx, "rm -rf "+shellQuote(s.abs(relPath)))
	s.listCache.invalidateTree(relPath)
	s.listCache.invalidate(parentDir(relPath))
	return err
}

// isBaseRel reports whether relPath names the backend base itself.
func isBaseRel(relPath string) bool {
	c := strings.Trim(path.Clean(relPath), "/")
	return c == "" || c == "."
}

func (s *sshShell) tailCmd(relPath string, offset int64) string {
	return fmt.Sprintf("tail -c +%d %s", offset+1, shellQuote(s.abs(relPath)))
}

// truncateCmd sizes path to n bytes portably: dd with a seek and no data
// truncates or extends the output file, where truncate(1) is GNU-only.
func truncateCmd(path string, n int64) string {
	return fmt.Sprintf("dd if=/dev/null of=%s bs=1 seek=%d 2>/dev/null", shellQuote(path), n)
}

// stream starts cmd on client and returns its stdout as a reader; release
// runs when the reader is closed. EOF alone is not success: the exit status
// only arrives through Wait, so the reader surfaces it (with stderr) once the
// pipe drains, or a cat that failed with EACCES looks like an empty file.
func (s *sshShell) stream(ctx context.Context, client *ssh.Client, cmd string, release func()) (io.ReadCloser, error) {
	Log.Add(s.proto, ">>>", cmd)
	session, err := client.NewSession()
	if err != nil {
		release()
		return nil, err
	}
	r := &sshReadCloser{session: session, release: release}
	session.Stderr = &r.stderr
	rd, err := session.StdoutPipe()
	if err == nil {
		err = session.Start(cmd)
	}
	if err != nil {
		session.Close()
		release()
		return nil, err
	}
	r.Reader = rd
	r.stop = CancelCloser(ctx, session)
	return r, nil
}

type sshReadCloser struct {
	io.Reader
	session *ssh.Session
	stderr  bytes.Buffer
	stop    func()
	release func()
	waited  bool
	waitErr error
}

func (r *sshReadCloser) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != io.EOF {
		return n, err
	}
	if werr := r.wait(); werr != nil {
		return n, werr
	}
	return n, io.EOF
}

func (r *sshReadCloser) wait() error {
	if r.waited {
		return r.waitErr
	}
	r.waited = true
	if err := r.session.Wait(); err != nil {
		r.waitErr = err
		if msg := strings.TrimSpace(r.stderr.String()); msg != "" {
			r.waitErr = fmt.Errorf("%v: %s", err, msg)
		}
	}
	return r.waitErr
}

func (r *sshReadCloser) Close() error {
	r.stop()
	err := r.session.Close()
	r.release()
	if r.waited || err == io.EOF {
		return nil
	}
	return err
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
