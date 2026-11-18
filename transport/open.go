package transport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

const fakeScheme = "fake://"

// An opener connects; ctx bounds the connect, the backend outlives it.
type opener func(ctx context.Context, arg string, insecure bool, parallel int) (model.Backend, error)

// schemes maps URL prefixes to constructors; IsRemote and OpenBackend both
// read it, so a new protocol is one line here.
var schemes = []struct {
	prefix string
	open   opener
}{
	{"sftp://", openSFTP},
	{"ssh://", openSSHBackend},
	{"scp://", openSSHBackend},
	{"ftp://", openFTP},
	{"ftps://", openFTP},
	{"ftpes://", openFTP},
	{"rsync+ssh://", openRsyncSSH},
	{"rsync://", openRsync},
	{"webdav://", openWebDAV},
	{"webdavs://", openWebDAV},
	{"restic://", openRestic},
	{"restics://", openRestic},
	{rcloneScheme, openRclone},
	{fakeScheme, openFake},
}

// Every opener that connects stops when ctx is cancelled (D1).
func openSFTP(ctx context.Context, a string, i bool, p int) (model.Backend, error) {
	return NewSFTPBackend(ctx, a, i, p)
}
func openFTP(ctx context.Context, a string, i bool, p int) (model.Backend, error) {
	return NewFTPBackend(ctx, a, i, p)
}
func openRsyncSSH(ctx context.Context, a string, i bool, p int) (model.Backend, error) {
	return NewRsyncSSHBackend(ctx, a, i, p)
}

// openRsync makes no connection: every call dials the daemon itself.
func openRsync(_ context.Context, a string, _ bool, _ int) (model.Backend, error) {
	return NewRsyncBackend(a)
}
func openWebDAV(ctx context.Context, a string, i bool, p int) (model.Backend, error) {
	return NewWebDAVBackend(ctx, a, i, p)
}
func openRestic(ctx context.Context, a string, i bool, p int) (model.Backend, error) {
	return NewResticBackend(ctx, a, i, p)
}
func openRclone(ctx context.Context, a string, i bool, _ int) (model.Backend, error) {
	return NewRcloneBackend(ctx, a, i)
}
func openFake(_ context.Context, a string, _ bool, _ int) (model.Backend, error) {
	return NewFakeBackend(a)
}

func lookupScheme(arg string) opener {
	for _, s := range schemes {
		if strings.HasPrefix(arg, s.prefix) {
			return s.open
		}
	}
	return nil
}

func IsRemote(arg string) bool { return lookupScheme(arg) != nil }

// urlMask is what MaskURLPassword shows for a secret.
const urlMask = "xxxxx"

// MaskURLPassword hides the secrets of an open argument, for the title bar,
// the log and the open dialog: the password of a URL, the option values of
// an rclone spec (replaceRcloneSecrets).
func MaskURLPassword(rawURL string) string {
	return replaceSecrets(rawURL, func(string) string { return urlMask })
}

// replaceSecrets is arg with each secret s in it replaced by repl(s): the
// password of a URL, the option values of an rclone spec but those that
// say where the remote is. One reading of where the secrets are, to hide
// them and to find a mask left in (holdsMask).
func replaceSecrets(arg string, repl func(string) string) string {
	if spec, ok := strings.CutPrefix(arg, rcloneScheme); ok {
		return rcloneScheme + replaceRcloneSecrets(spec, repl)
	}
	return replaceURLPassword(arg, repl)
}

// replaceURLPassword is rawURL with its password p replaced by repl(p):
// what follows the user name up to the @ before the host, cut as
// parseRemoteURL cuts it. A password with a / in it ended the authority
// early: the mask found no @ there and showed the URL, password and all
// (F26). An authority user:X, with X no port and an @ further on, is now
// taken for such a password, up to the last @. A password that starts with
// a port number and a / still shows: user:22/x@host reads as the host user
// on port 22.
func replaceURLPassword(rawURL string, repl func(string) string) string {
	idx := strings.Index(rawURL, "://")
	if idx < 0 {
		return rawURL
	}
	head, rest := rawURL[:idx+3], rawURL[idx+3:]
	authority, _, _ := strings.Cut(rest, "/")
	if at := strings.LastIndexByte(authority, '@'); at >= 0 {
		user, pw, ok := strings.Cut(authority[:at], ":")
		if !ok {
			return rawURL // a user, no password
		}
		return head + user + ":" + repl(pw) + rest[at:]
	}
	user, x, ok := strings.Cut(authority, ":")
	at := strings.LastIndexByte(rest, '@')
	if !ok || at < 0 || strings.HasPrefix(authority, "[") || isPortNumber(x) {
		return rawURL
	}
	return head + user + ":" + repl(rest[len(user)+1:at]) + rest[at:]
}

// errMaskForAnotherPlace refuses a secret left as the mask in an open
// argument for another place than the one it was given for.
var errMaskForAnotherPlace = errors.New("a password or secret shown as xxxxx is not sent to another place: type it again")

// Unmask puts the secrets MaskURLPassword hid in original back into edited,
// the masked text the open dialog showed, as the user left it. The dialog
// gave the secrets back only for text left as it was: any edit sent the
// mask as the password, and the login failed (U19). They go back only
// where edited is for the same place as original, all before the path the
// same, so a secret never goes where it was not given for. A secret left
// as the mask in an argument for another place is refused.
func Unmask(original, edited string) (string, error) {
	masked := MaskURLPassword(original)
	if masked == original {
		return edited, nil // nothing was hidden
	}
	mPlace, mPath := splitPlace(masked)
	ePlace, ePath := splitPlace(edited)
	if oPlace, ok := strings.CutSuffix(original, mPath); ok && ePlace == mPlace {
		return oPlace + ePath, nil
	}
	if holdsMask(edited) {
		return "", errMaskForAnotherPlace
	}
	return edited, nil
}

// holdsMask reports a secret of arg that is the mask: left as the dialog
// showed it. A password with xxxxx in it, or a host, is no mask.
func holdsMask(arg string) bool {
	found := false
	replaceSecrets(arg, func(s string) string {
		found = found || s == urlMask
		return s
	})
	return found
}

// splitPlace cuts an open argument into its place and its path: for a URL
// the scheme, user and host, then the path; for an rclone spec the remote
// and its options, then the :path.
func splitPlace(arg string) (place, rest string) {
	if spec, ok := strings.CutPrefix(arg, rcloneScheme); ok {
		opts, tail := splitRcloneOpts(spec)
		return rcloneScheme + opts, tail
	}
	idx := strings.Index(arg, "://")
	if idx < 0 {
		return "", arg
	}
	if si := strings.IndexByte(arg[idx+3:], '/'); si >= 0 {
		return arg[:idx+3+si], arg[idx+3+si:]
	}
	return arg, ""
}

// isPortNumber reports a port: 1 to 5 digits.
func isPortNumber(s string) bool {
	if s == "" || len(s) > 5 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// OpenBackend connects synchronously; anything without a known scheme is a
// local directory.
func OpenBackend(arg string, insecure bool, parallel int) (model.Backend, error) {
	return openBackend(context.Background(), arg, insecure, parallel)
}

func openBackend(ctx context.Context, arg string, insecure bool, parallel int) (model.Backend, error) {
	if open := lookupScheme(arg); open != nil {
		return open(ctx, arg, insecure, parallel)
	}
	return NewLocalBackend(arg), nil
}

// openSSHBackend dials SSH once, logs the server version, probes the SFTP
// subsystem, and returns the SFTPBackend when available, otherwise the
// shell-and-cat SCPBackend. Used for ssh:// and scp:// URLs; sftp:// always
// goes straight to SFTP without a fallback. parallel sizes the lazy
// connection pool used for parallel transfers (1 = no extras).
func openSSHBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (model.Backend, error) {
	conn, err := dialSSH(ctx, rawURL, insecure)
	if err != nil {
		return nil, err
	}
	ver := strings.TrimRight(string(conn.client.ServerVersion()), "\r\n")
	if !probeSFTP(conn.client) {
		Log.Add("ssh", DirIn, "SFTP unavailable, falling back to shell backend ("+ver+")")
		b, err := newSCPBackend(ctx, conn, rawURL, insecure, parallel)
		if err != nil {
			conn.client.Close()
			return nil, err
		}
		return b, nil
	}
	Log.Add("ssh", DirIn, "SFTP available, using SFTP backend ("+ver+")")
	b, err := newSFTPBackend(ctx, conn, rawURL, insecure, parallel)
	if err != nil {
		conn.client.Close()
		return nil, err
	}
	return b, nil
}

// probeSFTP returns true when the remote sshd accepts the "sftp" subsystem
// request. Cheaper and more reliable than parsing client.ServerVersion(): a
// server can run OpenSSH 9.x with Subsystem sftp disabled (some hardened
// configs), or run a non-OpenSSH server that still exposes SFTP.
func probeSFTP(client *ssh.Client) bool {
	sess, err := client.NewSession()
	if err != nil {
		return false
	}
	defer sess.Close()
	return sess.RequestSubsystem("sftp") == nil
}

// TryOpenBackend validates the argument and returns a backend. Remote schemes
// connect lazily on first use; fake:// is built eagerly since it has no
// connection to defer and the lazy wrapper's retry plumbing would only add
// noise to profiles.
func TryOpenBackend(arg string, insecure bool, parallel int) (model.Backend, error) {
	if strings.HasPrefix(arg, fakeScheme) {
		return NewFakeBackend(arg)
	}
	if !IsRemote(arg) {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", arg, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s: not a directory", arg)
		}
		return NewLocalBackend(arg), nil
	}
	if strings.HasPrefix(arg, rcloneScheme) {
		// rclone's config file is read here, before the TUI holds the
		// terminal when main calls this: a password command runs on a free
		// terminal, and a file that cannot be read is the argument's error
		// (L13).
		if err := rcloneSetup(); err != nil {
			return nil, err
		}
	}
	return NewLazyBackend(MaskURLPassword(arg), func(ctx context.Context) (model.Backend, error) {
		return openBackend(ctx, arg, insecure, parallel)
	}), nil
}

func ParentPath(p string) string {
	if idx := strings.Index(p, "://"); idx >= 0 {
		rest := p[idx+3:]
		si := strings.IndexByte(rest, '/')
		if si < 0 {
			return p
		}
		authority := rest[:si]
		pathPart := path.Clean(rest[si:])
		parent := path.Dir(pathPart)
		if parent == pathPart {
			return p
		}
		return p[:idx+3] + authority + parent
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		abs = p
	}
	parent := filepath.Dir(abs)
	if parent == abs {
		return abs
	}
	return parent
}

// CloseBackend closes b, if any, for a defer or a goroutine that has no one
// to tell about an error.
func CloseBackend(b model.Backend) {
	if b != nil {
		b.Close()
	}
}

// ParallelSetter is a backend that keeps one connection per parallel copy
// and can resize that pool while copies run.
type ParallelSetter interface {
	SetParallel(n int)
}

// SetParallel tells b that up to n copies may now run at once. Backends
// without a per-transfer connection pool ignore it.
func SetParallel(b model.Backend, n int) {
	if s, ok := b.(ParallelSetter); ok {
		s.SetParallel(max(n, 1))
	}
}
