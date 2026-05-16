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

func IsRemote(arg string) bool {
	for _, p := range []string{"sftp://", "ssh://", "scp://", "ftp://", "ftps://", "ftpes://", "rsync+ssh://", "rsync://"} {
		if strings.HasPrefix(arg, p) {
			return true
		}
	}
	return false
}

func MaskURLPassword(rawURL string) string {
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

func OpenBackend(arg string, insecure bool) (model.Backend, error) {
	if strings.HasPrefix(arg, "sftp://") {
		return NewSFTPBackend(arg)
	}
	if strings.HasPrefix(arg, "ssh://") || strings.HasPrefix(arg, "scp://") {
		return openSSHBackend(arg)
	}
	if strings.HasPrefix(arg, "ftp://") || strings.HasPrefix(arg, "ftps://") || strings.HasPrefix(arg, "ftpes://") {
		return NewFTPBackend(arg, insecure)
	}
	if strings.HasPrefix(arg, "rsync+ssh://") {
		return NewRsyncSSHBackend(arg)
	}
	if strings.HasPrefix(arg, "rsync://") {
		return NewRsyncBackend(arg)
	}
	return NewLocalBackend(arg), nil
}

// openSSHBackend dials SSH once, logs the server version, probes the SFTP
// subsystem, and returns the SFTPBackend when available — otherwise the
// shell-and-cat SCPBackend. Used for ssh:// and scp:// URLs; sftp:// always
// goes straight to SFTP without a fallback.
func openSSHBackend(rawURL string) (model.Backend, error) {
	conn, err := dialSSH(rawURL)
	if err != nil {
		return nil, err
	}
	ver := strings.TrimRight(string(conn.client.ServerVersion()), "\r\n")
	if probeSFTP(conn.client) {
		Log.Add("ssh", "<<<", "SFTP available — using SFTP backend ("+ver+")")
		b, err := newSFTPBackendFromConn(conn)
		if err != nil {
			conn.client.Close()
			return nil, err
		}
		return b, nil
	}
	Log.Add("ssh", "<<<", "SFTP unavailable — falling back to shell backend ("+ver+")")
	b, err := newSCPBackendFromConn(conn)
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
	if err := sess.RequestSubsystem("sftp"); err != nil {
		return false
	}
	return true
}

func OpenBackendLazy(arg string, insecure bool) model.Backend {
	if !IsRemote(arg) {
		info, err := os.Stat(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %v\n", arg, err)
			os.Exit(1)
		}
		if !info.IsDir() {
			fmt.Fprintf(os.Stderr, "error: %s: not a directory\n", arg)
			os.Exit(1)
		}
		return NewLocalBackend(arg)
	}
	return NewLazyBackend(MaskURLPassword(arg), func() (model.Backend, error) {
		return OpenBackend(arg, insecure)
	})
}

func TryOpenBackend(arg string, insecure bool) (model.Backend, error) {
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
		return OpenBackend(arg, insecure)
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
