package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

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
		if ci := strings.IndexByte(creds, ':'); ci >= 0 {
			user = creds[:ci]
			pass = creds[ci+1:]
		} else {
			user = creds
		}
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

func dialSSH(rawURL string) (*sshConn, error) {
	scheme, user, pass, alias, port, remotePath := parseRemoteURL(rawURL)
	host := alias
	hasPass := pass != ""

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
		user = os.Getenv("USER")
	}

	var auths []ssh.AuthMethod
	if hasPass {
		auths = append(auths, ssh.Password(pass))
	}

	var signers []ssh.Signer
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		if conn, err := net.Dial("unix", sock); err == nil {
			agentClient := agent.NewClient(conn)
			if agentSigners, err := agentClient.Signers(); err == nil {
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
	client, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		User:            user,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %v", addr, err)
	}

	return &sshConn{
		client:   client,
		scheme:   scheme,
		user:     user,
		alias:    alias,
		port:     port,
		basePath: remotePath,
	}, nil
}

func sshDisplayURL(conn *sshConn, remotePath string) string {
	displayHost := conn.alias
	if conn.port != "22" {
		displayHost = net.JoinHostPort(conn.alias, conn.port)
	}
	return fmt.Sprintf("%s://%s@%s%s", conn.scheme, conn.user, displayHost, remotePath)
}

var cksumProbes = []struct {
	algo string
	test string
	cmd  string
}{
	{"sha256", "echo -n test | sha256sum >/dev/null 2>&1", "sha256sum"},
	{"sha256", "echo -n test | shasum -a 256 >/dev/null 2>&1", "shasum -a 256"},
	{"sha1", "echo -n test | sha1sum >/dev/null 2>&1", "sha1sum"},
	{"sha1", "echo -n test | shasum >/dev/null 2>&1", "shasum"},
	{"md5", "echo -n test | md5sum >/dev/null 2>&1", "md5sum"},
	{"md5", "md5 -q -s test >/dev/null 2>&1", "md5 -q"},
}

func probeSSHChecksums(run func(string) (string, error)) (algos []string, cmds map[string]string) {
	cmds = make(map[string]string)
	seen := make(map[string]bool)
	for _, p := range cksumProbes {
		if cmds[p.algo] != "" {
			continue
		}
		if _, err := run(p.test); err == nil {
			cmds[p.algo] = p.cmd
			if !seen[p.algo] {
				algos = append(algos, p.algo)
				seen[p.algo] = true
			}
		}
	}
	return
}

func runSSHCmd(client interface{ NewSession() (*ssh.Session, error) }, proto, cmd string) (string, error) {
	remoteLog.Add(proto, ">>>", cmd)
	session, err := client.NewSession()
	if err != nil {
		remoteLog.Add(proto, "ERR", err.Error())
		return "", err
	}
	defer session.Close()
	var stdout bytes.Buffer
	session.Stdout = &stdout
	if err := session.Run(cmd); err != nil {
		remoteLog.Add(proto, "ERR", err.Error())
		return "", err
	}
	out := stdout.String()
	if out != "" {
		remoteLog.Add(proto, "<<<", strings.TrimRight(out, "\n"))
	}
	return out, nil
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
