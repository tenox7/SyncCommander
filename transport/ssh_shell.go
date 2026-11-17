package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

// sshShell is the shell layer the ssh-family backends share: metadata ops run
// as remote commands and file bodies stream through cat and tail. SCP and
// rsync+ssh embed it whole; SFTP uses it for the checksum commands only.
type sshShell struct {
	client  *ssh.Client
	proto   string
	base    string
	display string
	// The checksum command of each algorithm the server has, found on the
	// first ask and kept for the connection; nil until then.
	cksumMu   sync.Mutex
	cksumCmds map[string]string
	listCache *listCache // nil when the backend has no recursive preload
	usePerl   atomic.Bool
}

func (s *sshShell) BasePath() string { return s.display }

func (s *sshShell) abs(relPath string) string { return path.Join(s.base, cleanRel(relPath)) }

// exec runs cmd on client with the given stdin and stdout. A failure carries
// the command's stderr, which is what "exited with status 1" hides.
func (s *sshShell) exec(ctx context.Context, client *ssh.Client, cmd string, stdin io.Reader, stdout io.Writer) error {
	Log.Add(s.proto, DirOut, cmd)
	session, err := client.NewSession()
	if err != nil {
		Log.Add(s.proto, DirErr, err.Error())
		return err
	}
	defer session.Close()
	defer CancelCloser(ctx, session)()
	var stderr bytes.Buffer
	session.Stdin, session.Stdout, session.Stderr = stdin, stdout, &stderr
	if err := session.Run(cmd); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			err = &remoteError{msg: msg, err: err}
		}
		Log.Add(s.proto, DirErr, err.Error())
		return err
	}
	return nil
}

// remoteError is what a remote command wrote on stderr, which says more than
// its exit status. The session's own error stays under it, so a connection
// that died in the middle is still known by its type (D3).
type remoteError struct {
	msg string
	err error
}

func (e *remoteError) Error() string { return e.msg }
func (e *remoteError) Unwrap() error { return e.err }

// run executes cmd on the primary connection and returns its stdout.
func (s *sshShell) run(ctx context.Context, cmd string) (string, error) {
	var out bytes.Buffer
	if err := s.exec(ctx, s.client, cmd, nil, &out); err != nil {
		return "", err
	}
	if t := strings.TrimRight(out.String(), "\n"); t != "" && !strings.ContainsAny(t, "\n\x00") {
		Log.Add(s.proto, DirIn, t)
	}
	return out.String(), nil
}

// home asks the login shell for $HOME during setup, where no caller context
// exists; the deadline keeps a stuck server from hanging the connect.
func (s *sshShell) home() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sshKeepaliveTimeout)
	defer cancel()
	out, err := s.run(ctx, "echo $HOME")
	return strings.TrimSpace(out), err
}

// shellWord is the word checkShell has the login shell echo.
const shellWord = "sc-shell-ok"

// checkShell has the login shell echo a known word. scp and rsync+ssh read
// every listing, stat and sum from what commands print, so any other answer
// stops the connect (S10). Text around the word is a login script that
// prints: read as file data, it made the first entry of each listing size 0
// and its own first word every file's sum. No answer at all is an account
// that runs one fixed command, such as ForceCommand internal-sftp: every
// command prints nothing and exits 0, so every folder listed as empty.
func (s *sshShell) checkShell() error {
	ctx, cancel := context.WithTimeout(context.Background(), sshKeepaliveTimeout)
	defer cancel()
	var out bytes.Buffer
	if err := s.exec(ctx, s.client, "echo "+shellWord, nil, &out); err != nil {
		return fmt.Errorf("the login shell cannot run echo: %w", err)
	}
	switch got := strings.TrimRight(out.String(), "\r\n"); {
	case got == shellWord:
		return nil
	case strings.TrimSpace(got) == "":
		return errors.New("the server ran no command for this login (echo printed nothing): it may allow sftp only; use sftp://")
	default:
		return fmt.Errorf("the login shell printed %s for echo %s: a login script prints text, which would be read as file data; make it print only in interactive shells", clip(out.String()), shellWord)
	}
}

// clip quotes up to 80 bytes of what a server printed, for an error.
func clip(s string) string {
	if len(s) > 80 {
		return strconv.Quote(s[:80]) + "..."
	}
	return strconv.Quote(s)
}

// expandHome resolves the URL spellings that mean the login home: "", "/"
// and "/~" are the home itself, "/~/x" is x below it. When the server does
// not give the home as one absolute path, it fails, and so does the
// connect, which is then retried. It used to fall back to "/", on a
// reconnect too, and the tree on screen then mapped onto "/".
func expandHome(remotePath string, home func() (string, error)) (string, error) {
	rest := ""
	switch {
	case remotePath == "" || remotePath == "/" || remotePath == "/~":
	case strings.HasPrefix(remotePath, "/~/"):
		rest = remotePath[3:]
	default:
		return remotePath, nil
	}
	h, err := home()
	if err == nil && (!path.IsAbs(h) || strings.ContainsAny(h, "\r\n")) {
		err = fmt.Errorf("the server answered %q", h)
	}
	if err != nil {
		return "", fmt.Errorf("cannot find the login home folder (%v); write the full remote path in the URL", err)
	}
	return path.Join(h, rest), nil
}

// findRecord lays out size, mtime, atime, ctime, octal mode and type letter;
// the name comes last and the record ends in NUL, so tabs and newlines in
// names survive.
const findRecord = `%s\t%T@\t%A@\t%C@\t%m\t%y\t`

// unreadRecord is findRecord for a folder find cannot list (not readable or
// not searchable), with type letter D. find prunes it instead of reporting
// it on stderr, so the report arrives in order with the entries and find
// exits 0 when nothing else went wrong.
const unreadRecord = `%s\t%T@\t%A@\t%C@\t%m\tD\t`

// perlLister prints findRecord-shaped records on hosts whose find has no
// -printf (BSD, macOS). Its arguments are the directory and 1 to recurse; the
// name is the bare entry at one level and the full path when recursing,
// matching find's %f and %p. A path it cannot read, a folder it cannot open
// or an entry it cannot lstat, becomes a record of type ! holding the error
// in the first field and the full path last. A file deleted while it lists
// is skipped: leaving it out is right.
const perlLister = `perl -e 'sub u{my $e="$!";$e=~s/[\t\0]/ /g;printf "%s\t0\t0\t0\t0\t!\t%s\0",$e,$_[0]}sub w{my($d,$r)=@_;my $h;unless(opendir($h,$d)){u($d);return}for(readdir $h){next if $_ eq "."||$_ eq "..";my $p="$d/$_";my @s=lstat($p);unless(@s){u($p)unless $!{ENOENT};next}my $t=-l _?"l":-d _?"d":-f _?"f":-p _?"p":-S _?"s":-b _?"b":-c _?"c":"o";printf "%d\t%d\t%d\t%d\t%o\t%s\t%s\0",$s[7],$s[9],$s[8],$s[10],$s[2]&07777,$t,$r?$p:$_;w($p,$r)if $r&&$t eq "d"}}w($ARGV[0],$ARGV[1])'`

// exitNotThere is the exit status of a stat that finds nothing at its path.
const exitNotThere = 78

// perlStat prints the findRecord of its one argument, lstat'ed, the full
// path as the name, for hosts whose find has no -printf. It exits
// exitNotThere when nothing is there.
const perlStat = `perl -e 'my @s=lstat($ARGV[0]);unless(@s){exit 78 if $!{ENOENT};my $e="$!";$e=~s/[\t\0]/ /g;printf "%s\t0\t0\t0\t0\t!\t%s\0",$e,$ARGV[0];exit 0}my $t=-l _?"l":-d _?"d":-f _?"f":-p _?"p":-S _?"s":-b _?"b":-c _?"c":"o";printf "%d\t%d\t%d\t%d\t%o\t%s\t%s\0",$s[7],$s[9],$s[8],$s[10],$s[2]&07777,$t,$ARGV[0]'`

// statCmd prints the list record of p itself, a link as a link, or exits
// exitNotThere when nothing is there. find starts at p and, with
// -maxdepth 0, prints only p, which it does not follow.
func (s *sshShell) statCmd(p string) string {
	q := shellQuote(p)
	if s.usePerl.Load() {
		return perlStat + " " + q
	}
	return fmt.Sprintf(`[ -e %[1]s ] || [ -L %[1]s ] || exit %[2]d; find %[1]s -maxdepth 0 -printf '%[3]s%[4]s\0'`,
		q, exitNotThere, findRecord, "%f")
}

// Stat prints relPath's own list record on the server: one command, no
// listing of its folder. A folder above relPath that the login cannot
// search makes relPath "not there" too: the shell's test cannot tell the
// two apart.
func (s *sshShell) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	p := s.abs(relPath)
	out, err := s.run(ctx, s.statCmd(p))
	if err != nil && !s.usePerl.Load() && findLacksPrimary(err) {
		s.usePerl.Store(true)
		out, err = s.run(ctx, s.statCmd(p))
	}
	switch {
	case exitCode(err) == exitNotThere:
		return model.FileEntry{}, fmt.Errorf("%s: %s: %w", s.proto, relPath, os.ErrNotExist)
	case err != nil:
		return model.FileEntry{}, err
	}
	r, ok := statRecord(out)
	switch {
	case !ok:
		return model.FileEntry{}, fmt.Errorf("%s: stat %s: the server answered %q", s.proto, relPath, out)
	case r.err != "":
		return model.FileEntry{}, fmt.Errorf("%s: stat %s: %s", s.proto, relPath, r.err)
	}
	e := r.entry
	e.RelPath, e.Name = relPath, path.Base(relPath)
	if relPath == "" {
		e.Name = ""
	}
	return e, nil
}

// statRecord reads the one record a stat prints.
func statRecord(out string) (listRecord, bool) {
	return parseListRecord(strings.TrimSuffix(out, "\x00"))
}

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
	return fmt.Sprintf(`find %s%s -mindepth 1 \( -type d \( ! -readable -o ! -executable \) -printf '%s%s\0' -prune \) -o -printf '%s%s\0'`,
		shellQuote(findStart(dir)), depth, unreadRecord, name, findRecord, name)
}

// findStart names dir for find as dir/. : find does not follow a start
// folder that is a symlink, it prints nothing and exits 0, but it follows
// the link to reach the "." inside it. A dir that is a file then fails with
// "Not a directory" instead of listing as empty. The paths find prints
// carry the "/.", which relUnder cleans away. perl's opendir follows links
// and fails on files already.
func findStart(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir + "."
	}
	return dir + "/."
}

// list runs the directory lister into stdout, switching to the perl form for
// good once find rejects -printf, -readable or -executable (BSD, busybox and
// pre-2007 GNU find).
func (s *sshShell) list(ctx context.Context, dir string, recursive bool, stdout io.Writer) error {
	err := s.exec(ctx, s.client, s.listCmd(dir, recursive), nil, stdout)
	if err == nil || s.usePerl.Load() || !findLacksPrimary(err) {
		return err
	}
	s.usePerl.Store(true)
	return s.exec(ctx, s.client, s.listCmd(dir, recursive), nil, stdout)
}

func findLacksPrimary(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "printf") || strings.Contains(msg, "readable") || strings.Contains(msg, "executable")
}

func (s *sshShell) findList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	var out bytes.Buffer
	if err := s.list(ctx, s.abs(relDir), false, &out); err != nil {
		return nil, err
	}
	text := strings.TrimSuffix(out.String(), "\x00")
	if text == "" {
		return nil, nil
	}
	var result []model.FileEntry
	for _, rec := range strings.Split(text, "\x00") {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		r, ok := parseListRecord(rec)
		switch {
		case !ok:
			// Text that is not a record, such as what a login script
			// prints ahead of the listing, was left out, and the rest
			// was taken for the folder (S10).
			err := fmt.Errorf("%s: listing %s: %w: the server answered %s", s.proto, s.abs(relDir), ErrIncompleteListing, clip(rec))
			Log.Add(s.proto, DirErr, err.Error())
			return nil, err
		case r.err != "":
			// perl could not open the folder or stat an entry: the listing
			// is short, and exit status 0 says nothing.
			err := fmt.Errorf("cannot read %s: %s", path.Clean(r.name), r.err)
			Log.Add(s.proto, DirErr, err.Error())
			return nil, err
		}
		r.entry.RelPath, r.entry.Name = path.Join(relDir, r.name), r.name
		result = append(result, r.entry)
	}
	return result, nil
}

// findRecursive streams every entry under scope into emit as the listing
// arrives, grouped by parent; see shellListing.
func (s *sshShell) findRecursive(ctx context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
	absPrefix := s.abs(scope)
	l := newShellListing(scope, absPrefix, emit, complete)
	w := newRecordWriter(0, l.add)
	err := s.list(ctx, absPrefix, true, w)
	w.flush()
	if err == nil {
		err = l.finish(s.proto + ": listing " + absPrefix)
	}
	if err != nil {
		Log.Add(s.proto, DirWarn, err.Error())
	}
	return err
}

// shellListing is one recursive run of find or the perl lister. Both report
// what they cannot read in the same stream as the entries and in order: a D
// record is a folder find cannot list, a ! record a path perl could not
// read. So the folder a report makes short is still open when it arrives;
// it is never released, and at the end the cache drops it, to be listed
// live where the error shows. Everything else is served. A failed run, or a
// report that cannot be pinned to a folder of the listing, drops all.
type shellListing struct {
	scope, absPrefix string
	g                *emitGrouper
	dirs             map[string]bool // every folder listed, and the scope
	unread           map[string]bool // the grouper's skip set
	reports          []string
	bad              bool
	junk             string // the first text that was not a record
}

func newShellListing(scope, absPrefix string, emit func(string, []model.FileEntry), complete func(string)) *shellListing {
	l := &shellListing{scope: scope, absPrefix: absPrefix, dirs: map[string]bool{scope: true}, unread: map[string]bool{}}
	l.g = &emitGrouper{emit: emit, complete: complete, skip: l.unread, hold: func() bool { return l.bad }}
	return l
}

func (l *shellListing) add(rec string) {
	r, ok := parseListRecord(rec)
	if !ok {
		// See findList.
		if !l.bad {
			l.junk = rec
		}
		l.bad = true
		return
	}
	rel, ok := relUnder(l.absPrefix, r.name)
	if !ok {
		l.bad = true
		return
	}
	relPath := path.Join(l.scope, rel)
	if r.err != "" {
		l.reports = append(l.reports, path.Clean(r.name)+": "+r.err)
		l.unreadBy(relPath)
		return
	}
	if rel == "" {
		return
	}
	r.entry.RelPath, r.entry.Name = relPath, path.Base(relPath)
	if r.entry.IsDir {
		l.dirs[relPath] = true
	}
	if r.unread {
		l.reports = append(l.reports, path.Clean(r.name)+": cannot be read")
		l.unread[relPath] = true
	}
	l.g.add(r.entry)
}

// unreadBy marks the folder a path perl could not read makes short: the
// path itself when it is a folder of the listing (opendir failed), else its
// parent, which lacks the entry (lstat failed).
func (l *shellListing) unreadBy(relPath string) {
	switch parent := parentDir(relPath); {
	case l.dirs[relPath]:
		l.unread[relPath] = true
	case relPath != l.scope && l.dirs[parent]:
		l.unread[parent] = true
	default:
		l.bad = true
	}
}

func (l *shellListing) finish(label string) error {
	switch {
	case l.bad && l.junk != "":
		return fmt.Errorf("%s: %w: the server answered %s", label, ErrIncompleteListing, clip(l.junk))
	case l.bad:
		return fmt.Errorf("%s: %w", label, ErrIncompleteListing)
	case len(l.unread) == 0:
		l.g.finish()
		return nil
	}
	l.g.finishExcept(l.unread)
	return &unreadDirs{
		dirs: slices.Sorted(maps.Keys(l.unread)),
		why:  fmt.Sprintf("%s: %v: %s", label, ErrIncompleteListing, strings.Join(l.reports[:min(len(l.reports), 3)], "; ")),
	}
}

// listRecord is one record of a shell lister.
type listRecord struct {
	name   string
	entry  model.FileEntry
	unread bool   // D: a folder find cannot list
	err    string // !: the lister could not read name; no entry
}

// parseListRecord decodes one lister record; the trailing name field may
// contain anything but NUL. A link, pipe or device keeps its type (find's
// %y letter) in the mode: it is listed, never read or written through (L2,
// L9). One whose numbers do not read is no record, such as the first
// record with a login banner ahead of it, which was taken as size 0 (S10).
func parseListRecord(rec string) (listRecord, bool) {
	f := strings.SplitN(rec, "\t", 7)
	if len(f) < 7 {
		return listRecord{}, false
	}
	if f[5] == "!" {
		return listRecord{name: f[6], err: f[0]}, true
	}
	size, serr := strconv.ParseInt(f[0], 10, 64)
	modeVal, merr := strconv.ParseUint(f[4], 8, 32)
	mtime, mok := parseEpoch(f[1])
	atime, aok := parseEpoch(f[2])
	ctime, cok := parseEpoch(f[3])
	if serr != nil || size < 0 || merr != nil || !mok || !aok || !cok {
		return listRecord{}, false
	}
	e := model.FileEntry{
		Size:    size,
		ModTime: mtime,
		ATime:   model.NanoOf(atime),
		CTime:   model.NanoOf(ctime),
		IsDir:   f[5] == "d" || f[5] == "D",
		Mode:    os.FileMode(modeVal),
	}
	switch f[5] {
	case "d", "D":
		e.Mode |= os.ModeDir
	case "f":
	case "l":
		e.Mode |= os.ModeSymlink
	case "p":
		e.Mode |= os.ModeNamedPipe
	case "s":
		e.Mode |= os.ModeSocket
	case "b":
		e.Mode |= os.ModeDevice
	case "c":
		e.Mode |= os.ModeDevice | os.ModeCharDevice
	default:
		e.Mode |= os.ModeIrregular
	}
	return listRecord{name: f[6], entry: e, unread: f[5] == "D"}, true
}

// parseEpoch reads seconds since 1970 with an optional fraction, as find's
// %T@ and perl print them.
func parseEpoch(s string) (time.Time, bool) {
	sec, frac, _ := strings.Cut(s, ".")
	secs, err := strconv.ParseInt(sec, 10, 64)
	if err != nil || strings.Trim(frac, "0123456789") != "" {
		return time.Time{}, false
	}
	if frac == "" {
		return time.Unix(secs, 0), true
	}
	for len(frac) < 9 {
		frac += "0"
	}
	nsec, _ := strconv.ParseInt(frac[:9], 10, 64)
	return time.Unix(secs, nsec), true
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

// cksumProbes are the commands that may give each algorithm's sum; the
// first that works for an algorithm is used for it.
var cksumProbes = []struct{ algo, cmd string }{
	{"sha256", "sha256sum"},
	{"sha256", "shasum -a 256"},
	{"sha1", "sha1sum"},
	{"sha1", "shasum"},
	{"md5", "md5sum"},
	{"md5", "md5 -q"},
}

// testSums are the sums of the word test, which each probe is asked for.
var testSums = map[string]string{
	"sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
	"sha1":   "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
	"md5":    "098f6bcd4621d373cade4e832627b4f6",
}

var sshChecksumOrder = []string{"sha256", "sha1", "md5"}

// probeSSHChecksums finds a working command per algorithm: one that gives
// the right sum of the word test. That it exits 0 proves nothing (S10): on
// an account limited to sftp every command prints nothing and exits 0, and
// behind a login script that prints, the script's first word was every
// file's sum. wrong is told each answer that is not the sum. A probe that
// exits with a status has no such command; any other failure, a
// connection that died or ctx, ends the probe with it: taken for a missing
// command, it left the connection without checksums.
func probeSSHChecksums(run func(string) (string, error), wrong func(cmd, out string)) (map[string]string, error) {
	cmds := make(map[string]string)
	for _, p := range cksumProbes {
		if cmds[p.algo] != "" {
			continue
		}
		out, err := run("printf test | " + p.cmd)
		var exit *ssh.ExitError
		switch {
		case err == nil:
			if sum, ok := sumIn(out, p.algo); ok && sum == testSums[p.algo] {
				cmds[p.algo] = p.cmd
			} else {
				wrong(p.cmd, out)
			}
		case !errors.As(err, &exit):
			return nil, err
		}
	}
	return cmds, nil
}

// sumLen is how many hex digits each algorithm's sum has.
var sumLen = map[string]int{"sha256": 64, "sha1": 40, "md5": 32}

// sumIn reads the sum a checksum command printed: its first word, which
// must be algo's number of hex digits. GNU tools put a \ ahead of the sum
// of a name they escape. Anything else, such as the first word of a login
// banner ahead of the sum, is no sum (S10).
func sumIn(out, algo string) (string, bool) {
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return "", false
	}
	sum := strings.ToLower(strings.TrimPrefix(fields[0], "\\"))
	if len(sum) != sumLen[algo] || strings.Trim(sum, "0123456789abcdef") != "" {
		return "", false
	}
	return sum, true
}

// checksumCmds returns the server's checksum command of each algorithm it
// has, asking the server on the first call of the connection. An ask that
// failed keeps nothing, so the next one asks again.
func (s *sshShell) checksumCmds(ctx context.Context) (map[string]string, error) {
	s.cksumMu.Lock()
	defer s.cksumMu.Unlock()
	if s.cksumCmds != nil {
		return s.cksumCmds, nil
	}
	cmds, err := probeSSHChecksums(func(cmd string) (string, error) {
		var out bytes.Buffer
		err := s.exec(ctx, s.client, cmd, nil, &out)
		return out.String(), err
	}, func(cmd, out string) {
		Log.Add(s.proto, DirWarn, fmt.Sprintf("%s did not give the sum of a test word (it printed %s): not used", cmd, clip(out)))
	})
	if ctx.Err() != nil {
		// A command cut short can still end with an exit status, which
		// reads as a missing command: what a stopped probe found is not
		// kept, and the stop is known as one, so it is not logged.
		err = ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("%s: asking for checksum commands: %w", s.proto, err)
	}
	s.cksumCmds = cmds
	return cmds, nil
}

// checksumCmd returns the server's command for algo.
func (s *sshShell) checksumCmd(ctx context.Context, algo string) (string, error) {
	cmds, err := s.checksumCmds(ctx)
	if err != nil {
		return "", err
	}
	if cmd := cmds[algo]; cmd != "" {
		return cmd, nil
	}
	return "", fmt.Errorf("%s: no %q checksum command on the remote", s.proto, algo)
}

func (s *sshShell) ChecksumAlgos(ctx context.Context) ([]string, error) {
	cmds, err := s.checksumCmds(ctx)
	if err != nil {
		return nil, err
	}
	var algos []string
	for _, a := range sshChecksumOrder {
		if cmds[a] != "" {
			algos = append(algos, a)
		}
	}
	return algos, nil
}

func (s *sshShell) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	cmd, err := s.checksumCmd(ctx, algo)
	if err != nil {
		return "", err
	}
	out, err := s.run(ctx, cmd+" "+shellQuote(s.abs(relPath)))
	if err != nil {
		return "", err
	}
	sum, ok := sumIn(out, algo)
	if !ok {
		return "", fmt.Errorf("%s: %s of %s: the server answered %s, not a sum", s.proto, algo, relPath, clip(out))
	}
	return sum, nil
}

// ChecksumRange sums n bytes of relPath from off on the server, the
// negotiated command reading them from a pipe (model.RegionChecksummer). A
// file that ends early gives another sum, never a match.
func (s *sshShell) ChecksumRange(ctx context.Context, relPath, algo string, off, n int64) (string, error) {
	cmd, err := s.checksumCmd(ctx, algo)
	if err != nil {
		return "", err
	}
	fp := shellQuote(s.abs(relPath))
	read := fmt.Sprintf("head -c %d %s", n, fp)
	if off > 0 {
		read = fmt.Sprintf("tail -c +%d %s | head -c %d", off+1, fp, n)
	}
	out, err := s.run(ctx, fmt.Sprintf("test -f %s && %s | %s", fp, read, cmd))
	if err != nil {
		return "", err
	}
	sum, ok := sumIn(out, algo)
	if !ok {
		return "", fmt.Errorf("%s: %s of %d bytes of %s: the server answered %s, not a sum", s.proto, algo, n, relPath, clip(out))
	}
	return sum, nil
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

// renameCmd moves src to dst unless dst is taken: mv would replace a file
// there, or move src into a folder there. A dst that is src itself (a
// case-only rename on a case-insensitive disk) is not taken, unless it is a
// folder, where mv would move it into itself.
func renameCmd(src, dst string) string {
	src, dst = shellQuote(src), shellQuote(dst)
	return fmt.Sprintf("if [ -d %[2]s ] || { { [ -e %[2]s ] || [ -L %[2]s ]; } && ! [ %[1]s -ef %[2]s ]; }; then echo destination already exists >&2; exit 1; fi; mv %[1]s %[2]s", src, dst)
}

// Rename refuses a taken destination; see renameCmd.
func (s *sshShell) Rename(ctx context.Context, oldRel, newRel string) error {
	if err := renameBase(s.proto, oldRel, newRel); err != nil {
		return err
	}
	_, err := s.run(ctx, renameCmd(s.abs(oldRel), s.abs(newRel)))
	s.listCache.forgetRenamed(oldRel, newRel)
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
	s.listCache.forgetRemoved(relPath)
	return err
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
	Log.Add(s.proto, DirOut, cmd)
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

	// mu guards waited and waitErr: a cancel closes the reader from the
	// watcher's goroutine while the copy may be reading.
	mu      sync.Mutex
	waited  bool
	waitErr error

	closeOnce sync.Once
	closeErr  error
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.waited {
		return r.waitErr
	}
	r.waited = true
	if err := r.session.Wait(); err != nil {
		r.waitErr = err
		if msg := strings.TrimSpace(r.stderr.String()); msg != "" {
			r.waitErr = fmt.Errorf("%w: %s", err, msg)
		}
	}
	return r.waitErr
}

// Close may run more than once, and on two goroutines at once: a copy
// closes its reader, and a watcher closes it on a cancel or stall
// (CancelCloser). Only the first Close does anything, so the pooled
// connection goes back once. A second one used to close a closed channel,
// a panic that took sc down.
func (r *sshReadCloser) Close() error {
	r.closeOnce.Do(func() {
		r.stop()
		err := r.session.Close() // ends a session.Wait that wait() holds mu for
		r.release()
		r.mu.Lock()
		waited := r.waited
		r.mu.Unlock()
		if !waited && err != io.EOF {
			r.closeErr = err
		}
	})
	return r.closeErr
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
