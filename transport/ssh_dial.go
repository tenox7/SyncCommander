package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// sshKeepaliveInterval is how often we send keepalive@openssh.com global
// requests. Mirrors OpenSSH's ServerAliveInterval. If the request times out
// or errors, the underlying client is closed so any in-flight read/write on
// it (e.g. an SFTP write) unblocks with an error.
const sshKeepaliveInterval = 30 * time.Second
const sshKeepaliveTimeout = 15 * time.Second

func parseRemoteURL(rawURL string) (scheme, user, pass, host, port, remotePath string) {
	idx := strings.Index(rawURL, "://")
	if idx < 0 {
		return "", "", "", rawURL, "", "/"
	}
	scheme = rawURL[:idx]
	rest := rawURL[idx+3:]

	remotePath = "/"
	if si := strings.IndexByte(rest, '/'); si >= 0 {
		remotePath = rest[si:]
		rest = rest[:si]
	}

	hostport := rest
	if ai := strings.LastIndexByte(rest, '@'); ai >= 0 {
		creds := rest[:ai]
		hostport = rest[ai+1:]
		user, pass, _ = strings.Cut(creds, ":")
	}

	if strings.HasPrefix(hostport, "[") {
		if bi := strings.IndexByte(hostport, ']'); bi >= 0 {
			host = hostport[1:bi]
			if bi+1 < len(hostport) && hostport[bi+1] == ':' {
				port = hostport[bi+2:]
			}
		} else {
			host = hostport
		}
	} else if ci := strings.LastIndexByte(hostport, ':'); ci >= 0 {
		host = hostport[:ci]
		port = hostport[ci+1:]
	} else {
		host = hostport
	}
	return
}

type sshConn struct {
	client   *ssh.Client
	scheme   string
	user     string
	alias    string
	port     string
	basePath string
}

func dialSSH(rawURL string, insecure bool) (*sshConn, error) {
	scheme, user, pass, alias, port, remotePath := parseRemoteURL(rawURL)
	host := alias

	cfgHost, cfgUser, cfgPort, cfgKeys := lookupSSHConfig(alias)
	if cfgHost != "" {
		host = cfgHost
	}
	if port == "" {
		port = cfgPort
	}
	if port == "" {
		port = "22"
	}
	if user == "" {
		user = cfgUser
	}
	if user == "" {
		user = loginUser()
	}
	hostKeys, err := hostKeyCallback(insecure)
	if err != nil {
		return nil, err
	}

	var auths []ssh.AuthMethod
	if pass != "" {
		auths = append(auths, ssh.Password(pass))
	}

	var signers []ssh.Signer
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		if agentConn, err := net.Dial("unix", sock); err == nil {
			// The agent signs during the handshake, so its socket stays open
			// until this function returns.
			defer agentConn.Close()
			if agentSigners, err := agent.NewClient(agentConn).Signers(); err == nil {
				signers = append(signers, agentSigners...)
			}
		}
	}

	home, _ := os.UserHomeDir()
	keyFiles := cfgKeys
	if len(keyFiles) == 0 && home != "" {
		keyFiles = []string{
			home + "/.ssh/id_ed25519",
			home + "/.ssh/id_rsa",
			home + "/.ssh/id_ecdsa",
		}
	}
	for _, kf := range keyFiles {
		if strings.HasPrefix(kf, "~/") && home != "" {
			kf = home + kf[1:]
		}
		data, err := os.ReadFile(kf)
		if err != nil {
			continue
		}
		signer, err := ssh.ParsePrivateKey(data)
		if err != nil {
			continue
		}
		signers = append(signers, signer)
	}
	if len(signers) > 0 {
		auths = append(auths, ssh.PublicKeys(signers...))
	}

	addr := net.JoinHostPort(host, port)
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	rawConn, err := dialer.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %v", addr, err)
	}
	tracked := &trackedConn{Conn: LimitConn(rawConn)}
	tracked.touch()
	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            auths,
		HostKeyCallback: hostKeys,
		Timeout:         10 * time.Second,
	}
	c, chans, reqs, err := ssh.NewClientConn(tracked, addr, cfg)
	if err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("ssh handshake %s: %v", addr, hostKeyHint(err))
	}
	client := ssh.NewClient(c, chans, reqs)
	go sshKeepalive(client, tracked, scheme)

	return &sshConn{
		client:   client,
		scheme:   scheme,
		user:     user,
		alias:    alias,
		port:     port,
		basePath: remotePath,
	}, nil
}

// loginUser is the local account name, the ssh default for a missing user.
func loginUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	u, err := user.Current()
	if err != nil {
		return ""
	}
	name := u.Username
	if i := strings.LastIndexByte(name, '\\'); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// hostKeyCallback verifies against ~/.ssh/known_hosts unless -insecure.
func hostKeyCallback(insecure bool) (ssh.HostKeyCallback, error) {
	if insecure {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	var files []string
	for _, name := range []string{"known_hosts", "known_hosts2"} {
		if p := filepath.Join(home, ".ssh", name); fileExists(p) {
			files = append(files, p)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("ssh: no ~/.ssh/known_hosts; connect with ssh once or pass -insecure")
	}
	return knownhosts.New(files...)
}

// hostKeyHint turns knownhosts' terse errors into the action to take.
func hostKeyHint(err error) error {
	var ke *knownhosts.KeyError
	if !errors.As(err, &ke) {
		return err
	}
	if len(ke.Want) == 0 {
		return errors.New("host key not in known_hosts; connect with ssh once or pass -insecure")
	}
	return fmt.Errorf("HOST KEY MISMATCH: %v", err)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// trackedConn wraps a net.Conn and records the nanosecond timestamp of the
// most recent Read that returned bytes. The keepalive goroutine consults
// this to skip its probe when the peer has been actively talking: under
// heavy throughput, queueing a SendRequest behind file data can exceed the
// timeout even on a healthy link, causing false-positive disconnects.
type trackedConn struct {
	net.Conn
	lastReadNano atomic.Int64
}

func (c *trackedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.touch()
	}
	return n, err
}

func (c *trackedConn) touch() { c.lastReadNano.Store(time.Now().UnixNano()) }

func (c *trackedConn) idleFor() time.Duration {
	last := c.lastReadNano.Load()
	if last == 0 {
		return 0
	}
	return time.Since(time.Unix(0, last))
}

// sshKeepalive sends keepalive@openssh.com global requests on an interval.
// Skips the probe when the connection has had read activity within the
// interval: a recent read is proof the peer is alive, and probing while the
// transport is saturated risks false-positive timeouts. Closes the client if
// a probe errors or times out so any blocked session/SFTP write unblocks
// with an error.
func sshKeepalive(client *ssh.Client, conn *trackedConn, proto string) {
	t := time.NewTicker(sshKeepaliveInterval)
	defer t.Stop()
	done := make(chan struct{})
	go func() {
		_ = client.Wait()
		close(done)
	}()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if conn.idleFor() < sshKeepaliveInterval {
				continue
			}
			errCh := make(chan error, 1)
			go func() {
				_, _, err := client.SendRequest("keepalive@openssh.com", true, nil)
				errCh <- err
			}()
			select {
			case err := <-errCh:
				if err != nil {
					Log.Add(proto, "ERR", "ssh keepalive: "+err.Error()+", closing connection")
					client.Close()
					return
				}
			case <-time.After(sshKeepaliveTimeout):
				Log.Add(proto, "ERR", "ssh keepalive timeout, closing connection")
				client.Close()
				return
			case <-done:
				return
			}
		}
	}
}

func sshDisplayURL(conn *sshConn, remotePath string) string {
	displayHost := conn.alias
	if conn.port != "22" {
		displayHost = net.JoinHostPort(conn.alias, conn.port)
	}
	return fmt.Sprintf("%s://%s@%s%s", conn.scheme, conn.user, displayHost, remotePath)
}

func lookupSSHConfig(alias string) (hostname, user, port string, identityFiles []string) {
	home, _ := os.UserHomeDir()
	if home == "" {
		return
	}
	data, err := os.ReadFile(home + "/.ssh/config")
	if err != nil {
		return
	}

	var wHost, wUser, wPort string
	var wKeys []string
	matched, wild := false, false

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.ToLower(fields[0])
		val := strings.Join(fields[1:], " ")

		if key == "host" {
			matched, wild = false, false
			for _, h := range fields[1:] {
				if h == alias {
					matched = true
				}
				if h == "*" {
					wild = true
				}
			}
			continue
		}

		if !matched && !wild {
			continue
		}

		set := func(dst *string, v string) {
			if *dst == "" {
				*dst = v
			}
		}
		if matched {
			switch key {
			case "hostname":
				set(&hostname, val)
			case "user":
				set(&user, val)
			case "port":
				set(&port, val)
			case "identityfile":
				identityFiles = append(identityFiles, val)
			}
		} else {
			switch key {
			case "hostname":
				set(&wHost, val)
			case "user":
				set(&wUser, val)
			case "port":
				set(&wPort, val)
			case "identityfile":
				wKeys = append(wKeys, val)
			}
		}
	}

	if hostname == "" {
		hostname = wHost
	}
	if user == "" {
		user = wUser
	}
	if port == "" {
		port = wPort
	}
	if len(identityFiles) == 0 {
		identityFiles = wKeys
	}
	return
}
