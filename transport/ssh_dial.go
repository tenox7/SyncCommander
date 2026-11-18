package transport

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	"sc/model"
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
	gone     <-chan struct{} // closed once the connection has ended
	scheme   string
	user     string
	alias    string
	port     string
	basePath string
}

// sshHandshakeTimeout bounds the ssh handshake (version exchange, key
// exchange, login) after the TCP connect. ClientConfig.Timeout covers only
// ssh.Dial's own connect, and sc dials itself: an ssh -L forward whose far
// end is dead accepts the TCP connection and then never answers, and the
// dial hung for good, holding the pane's dial lock. A var for tests.
var sshHandshakeTimeout = 30 * time.Second

// dialSSH connects and logs in. ctx bounds the connect and the handshake
// only; the connection outlives it.
func dialSSH(ctx context.Context, rawURL string, insecure bool) (*sshConn, error) {
	scheme, user, pass, alias, port, remotePath := parseRemoteURL(rawURL)
	host := alias

	cfgHost, cfgUser, cfgPort, cfgKeys, cfgKeyAlias := lookupSSHConfig(alias)
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
		return nil, model.WithKind(model.ErrAuth, err)
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
	// known_hosts files a server's keys under its name and port, or under
	// its HostKeyAlias alone, with no port, as ssh does: through ssh -L the
	// server answers at a localhost port that changes (S15).
	filed := addr
	if cfgKeyAlias != "" {
		filed = net.JoinHostPort(cfgKeyAlias, "22")
	}
	if insecure {
		warnInsecure(scheme, addr, pass != "")
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	rawConn.SetDeadline(time.Now().Add(sshHandshakeTimeout))
	stopWatch := context.AfterFunc(ctx, func() { rawConn.Close() })
	tracked := &trackedConn{Conn: LimitConn(rawConn)}
	tracked.touch()
	cfg := &ssh.ClientConfig{
		User: user,
		Auth: auths,
		HostKeyCallback: func(_ string, remote net.Addr, key ssh.PublicKey) error {
			return hostKeys(filed, remote, key)
		},
		HostKeyAlgorithms: knownKeyAlgos(hostKeys, filed),
		Timeout:           10 * time.Second,
	}
	c, chans, reqs, err := ssh.NewClientConn(tracked, addr, cfg)
	if !stopWatch() && err == nil {
		c.Close() // ctx ended as the handshake finished: the watcher closed it
		err = ctx.Err()
	}
	if err != nil {
		rawConn.Close()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("ssh handshake %s: %w", addr, ctx.Err())
		}
		return nil, sshLoginFailure(fmt.Errorf("ssh handshake %s: %w", addr, hostKeyHint(err)), err)
	}
	rawConn.SetDeadline(time.Time{})
	client := ssh.NewClient(c, chans, reqs)
	gone := make(chan struct{})
	go func() {
		_ = client.Wait()
		close(gone)
	}()
	go sshKeepalive(client, tracked, scheme, gone)

	return &sshConn{
		client:   client,
		gone:     gone,
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

// hostKeyCallback verifies against the known_hosts files unless -insecure:
// the user's, then the system's, as ssh reads them. The system's were not
// read (S15).
func hostKeyCallback(insecure bool) (ssh.HostKeyCallback, error) {
	if insecure {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	var files []string
	if home, err := os.UserHomeDir(); err == nil {
		for _, name := range []string{"known_hosts", "known_hosts2"} {
			if p := filepath.Join(home, ".ssh", name); fileExists(p) {
				files = append(files, p)
			}
		}
	}
	for _, p := range globalKnownHosts {
		if fileExists(p) {
			files = append(files, p)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("ssh: no ~/.ssh/known_hosts; connect with ssh once or pass -insecure")
	}
	return knownhosts.New(files...)
}

// globalKnownHosts is the system's known_hosts files, which ssh reads
// after the user's own.
var globalKnownHosts = systemKnownHosts()

func systemKnownHosts() []string {
	dir := "/etc/ssh"
	if runtime.GOOS == "windows" {
		pd := os.Getenv("ProgramData")
		if pd == "" {
			return nil // never a path from the folder sc runs in
		}
		dir = filepath.Join(pd, "ssh")
	}
	return []string{filepath.Join(dir, "ssh_known_hosts"), filepath.Join(dir, "ssh_known_hosts2")}
}

// knownKeyAlgos is the host key algorithms to ask the server for: first
// those of the keys check holds for name, in x/crypto's order, then the
// rest. x/crypto asks for ECDSA before Ed25519 whatever known_hosts holds:
// a server with both keys, known_hosts holding only its Ed25519 one, was a
// "HOST KEY MISMATCH" (S15). nil, the default order, when none is known.
// The keys known come from a check of a key no known_hosts holds, whose
// error lists them, so knownhosts' own matching (patterns, hashed names,
// markers) decides.
func knownKeyAlgos(check ssh.HostKeyCallback, name string) []string {
	var ke *knownhosts.KeyError
	if err := check(name, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, probeKey); !errors.As(err, &ke) || len(ke.Want) == 0 {
		return nil
	}
	known := map[string]bool{}
	for _, k := range ke.Want {
		known[k.Key.Type()] = true
	}
	var first, rest []string
	for _, a := range ssh.SupportedAlgorithms().HostKeys {
		if known[keyTypeOf(a)] {
			first = append(first, a)
		} else {
			rest = append(rest, a)
		}
	}
	if len(first) == 0 {
		return nil
	}
	return append(first, rest...)
}

// keyTypeOf is the type of key a host key algorithm signs with.
func keyTypeOf(algo string) string {
	if algo == ssh.KeyAlgoRSASHA256 || algo == ssh.KeyAlgoRSASHA512 {
		return ssh.KeyAlgoRSA
	}
	return algo
}

// probeKey is a key no known_hosts holds.
var probeKey = func() ssh.PublicKey {
	k, err := ssh.NewPublicKey(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public())
	if err != nil {
		panic(err)
	}
	return k
}()

// insecureWarned holds the addresses -insecure has warned of.
var insecureWarned sync.Map

// warnInsecure says once for addr that its host key goes unchecked, and,
// with a password, that the password goes to whoever answers there (S15).
func warnInsecure(proto, addr string, password bool) {
	if _, seen := insecureWarned.LoadOrStore(addr, true); seen {
		return
	}
	msg := "-insecure: the host key of " + addr + " is not checked"
	if password {
		msg += ", and the password goes to whoever answers there"
	}
	Log.Add(proto, DirWarn, msg)
}

// sshLoginFailure marks a handshake error the server refused as model.ErrAuth:
// a login it did not take, too many keys offered, or a host key that is
// unknown or does not match. x/crypto/ssh gives a refused login no type of
// its own, so its words are read here, where the error is the handshake's
// alone and holds no file name.
func sshLoginFailure(err, cause error) error {
	var ke *knownhosts.KeyError
	msg := strings.ToLower(cause.Error())
	if errors.As(cause, &ke) || strings.Contains(msg, "ssh: unable to authenticate") ||
		strings.Contains(msg, "too many authentication failures") {
		return model.WithKind(model.ErrAuth, err)
	}
	return err
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
// with an error. done is closed once the connection has ended.
func sshKeepalive(client *ssh.Client, conn *trackedConn, proto string, done <-chan struct{}) {
	t := time.NewTicker(sshKeepaliveInterval)
	defer t.Stop()
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
					Log.Add(proto, DirErr, "ssh keepalive: "+err.Error()+", closing connection")
					client.Close()
					return
				}
			case <-time.After(sshKeepaliveTimeout):
				Log.Add(proto, DirErr, "ssh keepalive timeout, closing connection")
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

func lookupSSHConfig(alias string) (hostname, user, port string, identityFiles []string, hostKeyAlias string) {
	home, _ := os.UserHomeDir()
	if home == "" {
		return
	}
	data, err := os.ReadFile(home + "/.ssh/config")
	if err != nil {
		return
	}

	var wHost, wUser, wPort, wKeyAlias string
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
			case "hostkeyalias":
				set(&hostKeyAlias, val)
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
			case "hostkeyalias":
				set(&wKeyAlias, val)
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
	if hostKeyAlias == "" {
		hostKeyAlias = wKeyAlias
	}
	return
}
