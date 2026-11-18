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
	gone    <-chan struct{} // closed once client's connection has ended; nil: not known
	proto   string
	base    string
	display string
	// The checksum command of each algorithm the server has, found on the
	// first ask and kept for the connection; nil until then.
	cksumMu   sync.Mutex
	cksumCmds map[string]string
	listCache *listCache   // nil when the backend has no recursive preload
	form      atomic.Int32 // the lister: formUnknown until a listing or stat finds it
	// noBatch: an answer to ChecksumFiles was not the one asked for, so
	// the connection sums file by file.
	noBatch atomic.Bool
	// dirs holds the folders a Mkdir or a send made or went into, and the
	// folders above them: a send into one runs no mkdir (sawDir).
	dirs sync.Map
}

// sawDir notes relDir, and each folder above it up to the base, as there.
// rsync+ssh ran mkdir -p in an exec of its own before each file it sent:
// a round trip per file, 14 hours for 1M files at 50 ms (R16).
func (s *sshShell) sawDir(relDir string) {
	for p := cleanRel(relDir); ; p = parentDir(p) {
		s.dirs.Store(p, true)
		if p == "" {
			return
		}
	}
}

// knowsDir reports whether relDir is a folder sawDir noted.
func (s *sshShell) knowsDir(relDir string) bool {
	_, ok := s.dirs.Load(cleanRel(relDir))
	return ok
}

// forgetDirs drops the folders noted, after a remove or a rename of relPath
// when it was one of them: those under it are not there now. All go, so
// the cost does not grow with the folders noted: a send into each runs
// one mkdir again. A folder not noted has none noted under it.
func (s *sshShell) forgetDirs(relPath string) {
	if s.knowsDir(relPath) {
		s.dirs.Clear()
	}
}

func (s *sshShell) BasePath() string { return s.display }

func (s *sshShell) heldListCache() *listCache { return s.listCache }

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
	session.Stdout, session.Stderr = stdout, &stderr
	if err := runFed(session, cmd, stdin); err != nil {
		if client == s.client {
			err = s.lostIf(ctx, err)
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			err = &remoteError{msg: msg, err: err}
		}
		Log.Add(s.proto, DirErr, err.Error())
		return err
	}
	return nil
}

// runFed runs cmd in session and writes stdin to it from this goroutine.
// x/crypto copies Session.Stdin in a goroutine of its own, which Run does
// not wait for: a command that failed before it read its input left that
// goroutine reading the source after exec had returned. The lazy wrapper
// tries a copy again on the same source while none of it has been read,
// and the read in flight took up to 32 KiB the second try never sent: the
// file came out short, with every byte of the source counted (S24).
// A heldOpen input is written nothing and stays open until the command ends.
func runFed(session *ssh.Session, cmd string, stdin io.Reader) error {
	if stdin == nil {
		return session.Run(cmd)
	}
	w, err := session.StdinPipe()
	if err != nil {
		return err
	}
	if err := session.Start(cmd); err != nil {
		return err
	}
	if _, ok := stdin.(heldOpen); ok {
		return session.Wait()
	}
	// A command that ended closes the channel, and the copy stops on it.
	_, copyErr := io.Copy(w, stdin)
	if cerr := w.Close(); copyErr == nil && cerr != io.EOF {
		copyErr = cerr
	}
	if err := session.Wait(); err != nil {
		return err
	}
	return copyErr
}

// connGoneWait is how long lostIf waits to see the connection end:
// x/crypto/ssh closes the channels of a connection that ended just before
// it says the connection is gone.
const connGoneWait = time.Second

// lostIf is err, how a command on the shell's connection ended, as a lost
// connection when the command ended with no exit status because the
// connection ended under it. sc took that for a failure on the server's
// answer: a recursive listing cut off so kept nothing it had listed in
// full (S18). A command its ctx stopped is not looked at.
func (s *sshShell) lostIf(ctx context.Context, err error) error {
	var missing *ssh.ExitMissingError
	if s.gone == nil || ctx.Err() != nil || !errors.As(err, &missing) {
		return err
	}
	t := time.NewTimer(connGoneWait)
	defer t.Stop()
	select {
	case <-s.gone:
		return fmt.Errorf("%w: %w", model.ErrConnLost, err)
	case <-t.C:
		return err
	}
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

// shellCheckTimeout is how long a check the connect runs on the login
// shell may take: its echo, its $HOME, rsync+ssh's rsync. A shell whose
// PATH or home is on a hung mount answers none of them, while the server
// still answers keepalives (R17).
var shellCheckTimeout = sshKeepaliveTimeout

// errShellSilent is the error of a check the shell did not answer in time.
var errShellSilent = errors.New("the login shell did not answer")

// shellCheckCtx is the ctx of such a check: the connect's own, so a cancel
// stops it, with shellCheckTimeout on top.
func shellCheckCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, shellCheckTimeout, fmt.Errorf("%w in %v", errShellSilent, shellCheckTimeout))
}

// shellCheckErr is err of a check run under ctx. When ctx ended the check,
// it is why: the cancel, or that the shell did not answer.
func shellCheckErr(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return err
}

// home asks the login shell for $HOME during the connect.
func (s *sshShell) home(ctx context.Context) (string, error) {
	ctx, cancel := shellCheckCtx(ctx)
	defer cancel()
	out, err := s.run(ctx, "echo $HOME")
	return strings.TrimSpace(out), shellCheckErr(ctx, err)
}

// errRunsNoCommand is a login that runs no command, as ForceCommand
// internal-sftp makes it.
var errRunsNoCommand = errors.New("the server ran no command for this login (echo printed nothing): it may allow sftp only; use sftp://")

// shellWord is the word checkShell has the login shell echo.
const shellWord = "sc-shell-ok"

// checkShell has the login shell echo a known word. scp and rsync+ssh read
// every listing, stat and sum from what commands print, so any other answer
// stops the connect (S10). Text around the word is a login script that
// prints: read as file data, it made the first entry of each listing size 0
// and its own first word every file's sum. No answer at all is an account
// that runs one fixed command, such as ForceCommand internal-sftp: every
// command prints nothing and exits 0, so every folder listed as empty.
func (s *sshShell) checkShell(ctx context.Context) error {
	ctx, cancel := shellCheckCtx(ctx)
	defer cancel()
	var out bytes.Buffer
	if err := s.exec(ctx, s.client, "echo "+shellWord, nil, &out); err != nil {
		return fmt.Errorf("the login shell cannot run echo: %w", shellCheckErr(ctx, err))
	}
	switch got := strings.TrimRight(out.String(), "\r\n"); {
	case got == shellWord:
		return nil
	case strings.TrimSpace(got) == "":
		return errRunsNoCommand
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
		return "", fmt.Errorf("cannot find the login home folder (%w); write the full remote path in the URL", err)
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

// The lister of a shell backend: GNU find, or perl where find lacks what
// the find form uses. It is unknown until the first listing or stat, whose
// command asks find first (probed).
const (
	formUnknown int32 = iota
	formFind
	formPerl
)

// exitNoGNUFind is the exit status of the probe when find lacks -printf,
// -readable, -executable or -ignore_readdir_race (BSD, macOS, busybox, old
// GNU find), or there is no find.
const exitNoGNUFind = 74

// probed puts the probe ahead of cmd while the form is unknown. The probe
// names no path but /, which -maxdepth 0 does not list, so its answer is
// about find alone. sc read the error of the listing instead, and a path
// with "printf", "readable" or "executable" in it, such as a folder named
// unreadable that was not there, switched it to perl for good (S18).
func probed(form int32, cmd string) string {
	if form != formUnknown {
		return cmd
	}
	return fmt.Sprintf(`find / -maxdepth 0 -ignore_readdir_race -readable -executable -printf '' 2>/dev/null || exit %d; `, exitNoGNUFind) + cmd
}

// learnForm notes what a command sent in form says of find, from how it
// ended (err): in the unknown form the probe ran first, and exitNoGNUFind
// is perl from now on, the command to run again in it (true); any other
// end is a find that has what the find form uses. A command that did not
// end, as on a lost connection, says nothing.
func (s *sshShell) learnForm(form int32, err error) bool {
	if form != formUnknown {
		return false
	}
	switch code := exitCode(err); {
	case code == exitNoGNUFind:
		s.form.Store(formPerl)
		return true
	case err == nil || code >= 0:
		s.form.CompareAndSwap(formUnknown, formFind)
	}
	return false
}

// perlStat prints the findRecord of its one argument, lstat'ed, the full
// path as the name, for hosts whose find has no -printf. It exits
// exitNotThere when nothing is there.
const perlStat = `perl -e 'my @s=lstat($ARGV[0]);unless(@s){exit 78 if $!{ENOENT};my $e="$!";$e=~s/[\t\0]/ /g;printf "%s\t0\t0\t0\t0\t!\t%s\0",$e,$ARGV[0];exit 0}my $t=-l _?"l":-d _?"d":-f _?"f":-p _?"p":-S _?"s":-b _?"b":-c _?"c":"o";printf "%d\t%d\t%d\t%d\t%o\t%s\t%s\0",$s[7],$s[9],$s[8],$s[10],$s[2]&07777,$t,$ARGV[0]'`

// statCmd prints the list record of p itself, a link as a link, or exits
// exitNotThere when nothing is there. find starts at p and, with
// -maxdepth 0, prints only p, which it does not follow.
func (s *sshShell) statCmd(p string, form int32) string {
	q := shellQuote(p)
	if form == formPerl {
		return perlStat + " " + q
	}
	return probed(form, fmt.Sprintf(`[ -e %[1]s ] || [ -L %[1]s ] || exit %[2]d; find %[1]s -maxdepth 0 -printf '%[3]s%[4]s\0'`,
		q, exitNotThere, findRecord, "%f"))
}

// statAt is Stat of the full path p, which errors call name.
func (s *sshShell) statAt(ctx context.Context, p, name string) (model.FileEntry, error) {
	form := s.form.Load()
	out, err := s.run(ctx, s.statCmd(p, form))
	if s.learnForm(form, err) {
		out, err = s.run(ctx, s.statCmd(p, formPerl))
	}
	switch {
	case exitCode(err) == exitNotThere:
		return model.FileEntry{}, fmt.Errorf("%s: %s: %w", s.proto, name, os.ErrNotExist)
	case err != nil:
		return model.FileEntry{}, err
	}
	r, ok := statRecord(out)
	switch {
	case !ok:
		return model.FileEntry{}, fmt.Errorf("%s: stat %s: the server answered %q", s.proto, name, out)
	case r.err != "":
		return model.FileEntry{}, fmt.Errorf("%s: stat %s: %s", s.proto, name, r.err)
	}
	return r.entry, nil
}

// Stat prints relPath's own list record on the server: one command, no
// listing of its folder. A folder above relPath that the login cannot
// search makes relPath "not there" too: the shell's test cannot tell the
// two apart.
func (s *sshShell) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	e, err := s.statAt(ctx, s.abs(relPath), relPath)
	if err != nil {
		return model.FileEntry{}, err
	}
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

// listCmd lists dir in form, one level or all under it. find leaves out a
// file deleted while it lists, as perl does (-ignore_readdir_race): it said
// "No such file or directory" and exited 1, and one temporary file that
// went away threw the whole recursive listing away (S18). A start folder
// that is not there still fails (GNU find 4.5.11 to 4.10.0).
func (s *sshShell) listCmd(dir string, recursive bool, form int32) string {
	if form == formPerl {
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
	return probed(form, fmt.Sprintf(`find %s -ignore_readdir_race%s -mindepth 1 \( -type d \( ! -readable -o ! -executable \) -printf '%s%s\0' -prune \) -o -printf '%s%s\0'`,
		shellQuote(findStart(dir)), depth, unreadRecord, name, findRecord, name))
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

// list runs the directory lister into stdout. The first listing or stat
// finds the lister (learnForm): perl when find lacks what the find form
// uses, and the probe then printed nothing, so the listing runs again into
// the same stdout.
func (s *sshShell) list(ctx context.Context, dir string, recursive bool, stdout io.Writer) error {
	form := s.form.Load()
	err := s.exec(ctx, s.client, s.listCmd(dir, recursive, form), nil, stdout)
	if !s.learnForm(form, err) {
		return err
	}
	return s.exec(ctx, s.client, s.listCmd(dir, recursive, formPerl), nil, stdout)
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
// live where the error shows. Everything else is served. A report that
// cannot be pinned to a folder of the listing drops all, and so does a
// failed run, but for the folders released before its connection was cut.
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
		Mode:    unixFileMode(uint32(modeVal)),
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

// ChecksumFiles sums the files rels names with one command
// (model.ChecksumBatcher): a command for each file was a round trip for
// each, hours for a million small files at 50 ms (S19). The command is sh
// and the script comes on its input, so no command line limits the names,
// and they stay out of the log. For each file the script prints its index
// in rels, a space, and what the server's command for algo prints with the
// file as its input, as the probe ran it; or "!" for a file that is not a
// file now or cannot be read. That file, and one the answer does not
// reach, gets no sum. An answer that is not the one asked for is not read
// on, and the connection sums file by file from then on.
func (s *sshShell) ChecksumFiles(ctx context.Context, rels []string, algo string) (map[string]string, error) {
	if s.noBatch.Load() {
		return nil, fmt.Errorf("%s: summing files in one command: %w", s.proto, ErrUnsupported)
	}
	cmds, err := s.checksumCmds(ctx)
	if err != nil {
		return nil, err
	}
	// An algorithm the shell has no command for, as rsync's md4, is no
	// failure to try again.
	cmd := cmds[algo]
	if cmd == "" {
		return nil, fmt.Errorf("%s: no %q checksum command on the remote: %w", s.proto, algo, ErrUnsupported)
	}
	var script bytes.Buffer
	fmt.Fprintf(&script, `sc_sum() { printf '%%s ' "$1"; if [ -f "$2" ] && [ ! -L "$2" ]; then { %s < "$2"; } 2>/dev/null || echo '!'; else echo '!'; fi; }`+"\n", cmd)
	for i, rel := range rels {
		fmt.Fprintf(&script, "sc_sum %d %s\n", i, shellQuote(s.abs(rel)))
	}
	w := &batchSums{rels: rels, algo: algo, sums: make(map[string]string, len(rels))}
	err = s.exec(ctx, s.client, "sh", &script, w)
	// A command that did not run, or did not end by itself, says nothing
	// of how the server answers: a lost connection is tried again.
	var exit *ssh.ExitError
	if err != nil && !errors.As(err, &exit) || ctx.Err() != nil {
		return w.sums, err
	}
	if err == nil && w.bad == "" && len(w.buf) == 0 && w.next == len(rels) {
		return w.sums, nil
	}
	s.noBatch.Store(true)
	what := fmt.Sprintf("the answer ended after %d of %d files", w.next, len(rels))
	if w.bad != "" {
		what = "the server answered " + clip(w.bad)
	}
	if err != nil {
		what += ", " + err.Error()
	}
	err = fmt.Errorf("%s: summing files in one command: %s; the files are summed one by one: %w", s.proto, what, ErrUnsupported)
	Log.Add(s.proto, DirWarn, err.Error())
	return w.sums, err
}

// batchSums reads the answer to ChecksumFiles's script, a line for each
// file: its index, a space, and what its checksum command printed. It
// reads on only while each line is the next one asked for; it never fails
// a write, which would hold the command up.
type batchSums struct {
	rels []string
	algo string
	sums map[string]string
	buf  []byte
	next int    // the index the next line has
	bad  string // the first line that is not the next one asked for
}

func (w *batchSums) Write(p []byte) (int, error) {
	if w.bad != "" {
		return len(p), nil
	}
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := string(w.buf[:i])
		w.buf = w.buf[i+1:]
		idx, rest, ok := strings.Cut(line, " ")
		if !ok || w.next >= len(w.rels) || idx != strconv.Itoa(w.next) {
			w.bad, w.buf = line, nil
			break
		}
		if sum, ok := sumIn(rest, w.algo); ok {
			w.sums[w.rels[w.next]] = sum
		}
		w.next++
	}
	return len(p), nil
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

// touchPOSIX is a time as touch -t takes it, POSIX's form: to the second,
// in the zone TZ names.
const touchPOSIX = "200601021504.05"

// SetTimes sets relPath's times with touch -d, to the nanosecond. A touch
// that does not take -d so (busybox, older Unix) gets -t in UTC, to the
// second: the command failed, the times stayed those of the copy, and the
// next compare found every file different (S20).
func (s *sshShell) SetTimes(ctx context.Context, relPath string, mtime, atime, _ time.Time) error {
	if atime.IsZero() {
		atime = mtime
	}
	fp := shellQuote(s.abs(relPath))
	m, a := mtime.UTC(), atime.UTC()
	_, err := s.run(ctx, fmt.Sprintf("{ touch -m -d %s %s && touch -a -d %s %s; } 2>/dev/null || { TZ=UTC0 touch -m -t %s %s && TZ=UTC0 touch -a -t %s %s; }",
		shellQuote(m.Format(touchLayout)), fp, shellQuote(a.Format(touchLayout)), fp, m.Format(touchPOSIX), fp, a.Format(touchPOSIX), fp))
	s.listCache.invalidate(parentDir(relPath))
	return err
}

func (s *sshShell) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	fp := shellQuote(s.abs(relPath))
	cmd := "mkdir -p " + fp
	if mode != 0 {
		cmd += fmt.Sprintf(" && { chmod %04o %s || exit %d; }", mode.Perm(), fp, exitModeNotSet)
	}
	_, err := s.run(ctx, cmd)
	s.listCache.invalidateAncestors(relPath)
	if exitCode(err) == exitModeNotSet {
		// The folder is made: a mode the server will not take is no
		// failure, as over SFTP (S20).
		Log.Add(s.proto, DirWarn, fmt.Sprintf("MKDIR %s: chmod %04o: %v", relPath, mode.Perm(), err))
		err = nil
	}
	if err == nil {
		s.sawDir(relPath)
	}
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
	s.forgetDirs(oldRel)
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
	err := s.rmRF(ctx, relPath)
	s.listCache.forgetRemoved(relPath)
	s.forgetDirs(relPath)
	return err
}

// errNoShell is a command the server did not run as it was sent: it runs
// something else for every command, as ForceCommand internal-sftp does, or
// it has no such program.
var errNoShell = errors.New("the server did not run the command")

// removedWord is the last line rmRF's command prints when rm went through.
const removedWord = "sc-removed"

// rmRF runs rm -rf on relPath, made to stop on the server when ctx stops
// it. sshd leaves a command that reads and writes nothing running when its
// client goes: a cancelled delete went on removing (S19). rm runs in the
// background, and a read of the session's input, which the client holds
// open until the command ends, stops it when that input ends, as it does
// when the session closes. The command says when rm went through, so a
// server that ran something else is not taken for one that removed.
func (s *sshShell) rmRF(ctx context.Context, relPath string) error {
	cmd := "exec 3<&0; rm -rf " + shellQuote(s.abs(relPath)) +
		" & p=$!; { read -r x <&3; kill $p; } >/dev/null 2>&1 & w=$!; exec 3<&-; wait $p; s=$?; kill $w 2>/dev/null; [ $s = 0 ] && echo " + removedWord + "; exit $s"
	var out bytes.Buffer
	err := s.exec(ctx, s.client, cmd, heldOpen{}, &out)
	var exit *ssh.ExitError
	switch {
	case errors.As(err, &exit) && exit.ExitStatus() == 127:
		return fmt.Errorf("%s: rm -rf %s: %w: %w", s.proto, relPath, errNoShell, err)
	case err == nil && !strings.HasSuffix(strings.TrimRight(out.String(), "\r\n"), removedWord):
		return fmt.Errorf("%s: rm -rf %s: the server answered %s: %w", s.proto, relPath, clip(out.String()), errNoShell)
	}
	return err
}

// heldOpen is the input of a command that reads it only to see its
// session end, as rm -rf's: runFed writes nothing to it and keeps it open
// until the command ends. The input this replaced gave nothing until it
// was closed, after the call: read by the call itself, it held the call
// for good (S24).
type heldOpen struct{}

// Read is not called: runFed reads no heldOpen.
func (heldOpen) Read([]byte) (int, error) {
	return 0, errors.New("a held input has nothing to read")
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
