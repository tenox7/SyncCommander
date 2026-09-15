package transport

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

const fakeScheme = "fake://"

type opener func(arg string, insecure bool, parallel int) (model.Backend, error)

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

func openSFTP(a string, i bool, p int) (model.Backend, error)     { return NewSFTPBackend(a, i, p) }
func openFTP(a string, i bool, p int) (model.Backend, error)      { return NewFTPBackend(a, i, p) }
func openRsyncSSH(a string, i bool, p int) (model.Backend, error) { return NewRsyncSSHBackend(a, i, p) }
func openRsync(a string, _ bool, _ int) (model.Backend, error)    { return NewRsyncBackend(a) }
func openWebDAV(a string, i bool, p int) (model.Backend, error)   { return NewWebDAVBackend(a, i, p) }
func openRestic(a string, i bool, p int) (model.Backend, error)   { return NewResticBackend(a, i, p) }
func openRclone(a string, i bool, _ int) (model.Backend, error)   { return NewRcloneBackend(a, i) }
func openFake(a string, _ bool, _ int) (model.Backend, error)     { return NewFakeBackend(a) }

func lookupScheme(arg string) opener {
	for _, s := range schemes {
		if strings.HasPrefix(arg, s.prefix) {
			return s.open
		}
	}
	return nil
}

func IsRemote(arg string) bool { return lookupScheme(arg) != nil }

func MaskURLPassword(rawURL string) string {
	if strings.HasPrefix(rawURL, rcloneScheme) {
		return rcloneScheme + maskRcloneSecrets(strings.TrimPrefix(rawURL, rcloneScheme))
	}
	idx := strings.Index(rawURL, "://")
	if idx < 0 {
		return rawURL
	}
	rest := rawURL[idx+3:]
	authority := rest
	tail := ""
	if si := strings.IndexByte(rest, '/'); si >= 0 {
		authority = rest[:si]
		tail = rest[si:]
	}
	ai := strings.LastIndexByte(authority, '@')
	if ai < 0 {
		return rawURL
	}
	creds := authority[:ai]
	ci := strings.IndexByte(creds, ':')
	if ci < 0 {
		return rawURL
	}
	return rawURL[:idx+3] + creds[:ci] + ":xxxxx@" + authority[ai+1:] + tail
}

// OpenBackend connects synchronously; anything without a known scheme is a
// local directory.
func OpenBackend(arg string, insecure bool, parallel int) (model.Backend, error) {
	if open := lookupScheme(arg); open != nil {
		return open(arg, insecure, parallel)
	}
	return NewLocalBackend(arg), nil
}

// openSSHBackend dials SSH once, logs the server version, probes the SFTP
// subsystem, and returns the SFTPBackend when available, otherwise the
// shell-and-cat SCPBackend. Used for ssh:// and scp:// URLs; sftp:// always
// goes straight to SFTP without a fallback. parallel sizes the lazy
// connection pool used for parallel transfers (1 = no extras).
func openSSHBackend(rawURL string, insecure bool, parallel int) (model.Backend, error) {
	conn, err := dialSSH(rawURL, insecure)
	if err != nil {
		return nil, err
	}
	ver := strings.TrimRight(string(conn.client.ServerVersion()), "\r\n")
	if !probeSFTP(conn.client) {
		Log.Add("ssh", DirIn, "SFTP unavailable, falling back to shell backend ("+ver+")")
		return newSCPBackend(conn, rawURL, insecure, parallel), nil
	}
	Log.Add("ssh", DirIn, "SFTP available, using SFTP backend ("+ver+")")
	b, err := newSFTPBackend(conn, rawURL, insecure, parallel)
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
	return NewLazyBackend(MaskURLPassword(arg), func() (model.Backend, error) {
		return OpenBackend(arg, insecure, parallel)
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

func CloseBackend(b model.Backend) {
	if c, ok := b.(interface{ Close() error }); ok {
		c.Close()
	}
}
