package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// sshConfig is what ssh's configuration gives a host, for the options sc
// uses, with the URL's user and port in place of the configured ones.
type sshConfig struct {
	hostName, user, port string
	hostKeyAlias         string
	// identityFiles are the keys to offer, expanded (~, %h, ${VAR}...):
	// ssh's defaults when the configuration names none.
	identityFiles  []string
	identitiesOnly bool
	// agent is the agent's socket, "" for none: SSH_AUTH_SOCK's, unless
	// IdentityAgent names another.
	agent string
	// proxy is a ProxyJump or ProxyCommand line, which sc does not follow.
	proxy string
}

var (
	// sshConfigCommand is the ssh that judges the configuration (ssh -G).
	// "" has sc read the files itself, as when there is no ssh.
	sshConfigCommand = "ssh"
	// sshConfigFile, when set, is the one file ssh -G reads (-F): tests.
	sshConfigFile = ""
	// sshSystemConfig is the system's file, read after the user's when sc
	// reads them itself.
	sshSystemConfig = systemSSHConfig()
)

func systemSSHConfig() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("ProgramData"), "ssh", "ssh_config")
	}
	return "/etc/ssh/ssh_config"
}

// resolveSSHConfig is ssh's configuration for alias as `ssh alias` uses
// it, the URL's user and port given as -l and -p. ssh judges it (ssh -G):
// Match, Include, Host patterns, the system file and which value wins
// are its rules, of which sc read a part, and misread Match (S22). Where
// ssh cannot answer, sc reads the user's file and the system's itself.
func resolveSSHConfig(ctx context.Context, alias, user, port string) sshConfig {
	var cfg sshConfig
	asked := false
	if sshConfigCommand != "" && alias != "" && !strings.HasPrefix(alias, "-") {
		var err error
		if cfg, err = askSSH(ctx, alias, user, port); err == nil {
			asked = true
		} else {
			warnSSHConfig(alias, err)
		}
	}
	if !asked {
		cfg = readSSHConfig(alias)
	}
	if user != "" {
		cfg.user = user
	}
	if port != "" {
		cfg.port = port
	}
	if cfg.hostName == "" {
		cfg.hostName = alias
	}
	if cfg.port == "" {
		cfg.port = "22"
	}
	if cfg.user == "" {
		cfg.user = loginUser()
	}
	home, _ := os.UserHomeDir()
	if len(cfg.identityFiles) == 0 && home != "" {
		cfg.identityFiles = []string{"~/.ssh/id_rsa", "~/.ssh/id_ecdsa", "~/.ssh/id_ed25519"}
	}
	t := sshTokens{home: home, local: loginUser(), host: cfg.hostName, user: cfg.user, port: cfg.port, alias: alias, keyAlias: cfg.hostKeyAlias}
	for i, f := range cfg.identityFiles {
		cfg.identityFiles[i] = t.expand(f)
	}
	cfg.agent = agentSocket(cfg.agent, t, !asked)
	return cfg
}

// askSSH runs ssh -G: the configuration ssh would connect with, a "key
// value" pair a line, keys in lower case. -- keeps alias from being read
// as an option.
func askSSH(ctx context.Context, alias, user, port string) (sshConfig, error) {
	args := []string{"-G"}
	if sshConfigFile != "" {
		args = append(args, "-F", sshConfigFile)
	}
	if user != "" {
		args = append(args, "-l", user)
	}
	if port != "" {
		args = append(args, "-p", port)
	}
	args = append(args, "--", alias)
	// A Match exec that hangs is no reason to hang.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, sshConfigCommand, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			err = fmt.Errorf("%w: %s", err, msg)
		}
		return sshConfig{}, fmt.Errorf("ssh -G %s: %w", alias, err)
	}
	var cfg sshConfig
	for _, line := range strings.Split(string(out), "\n") {
		key, val, _ := strings.Cut(strings.TrimRight(line, "\r"), " ")
		switch key {
		case "hostname":
			cfg.hostName = val
		case "user":
			cfg.user = val
		case "port":
			cfg.port = val
		case "hostkeyalias":
			cfg.hostKeyAlias = val
		case "identityfile":
			cfg.identityFiles = append(cfg.identityFiles, val)
		case "identitiesonly":
			cfg.identitiesOnly = val == "yes"
		case "identityagent":
			cfg.agent = val
		case "proxyjump", "proxycommand":
			if val != "none" {
				cfg.proxy = proxyName[key] + " " + val
			}
		}
	}
	if cfg.hostName == "" {
		return sshConfig{}, fmt.Errorf("ssh -G %s: no hostname in its answer", alias)
	}
	return cfg, nil
}

var proxyName = map[string]string{"proxyjump": "ProxyJump", "proxycommand": "ProxyCommand"}

// sshConfigWarned is the hosts for which ssh -G gave no answer.
var sshConfigWarned sync.Map

func warnSSHConfig(alias string, err error) {
	if _, done := sshConfigWarned.LoadOrStore(alias, true); !done {
		Log.Add("ssh", DirWarn, err.Error()+": sc reads ssh_config itself, without Match or Include")
	}
}

// proxyWarned is the hosts whose proxy sc said it does not follow.
var proxyWarned sync.Map

// warnProxy says, once a host, that sc dials it itself where ssh would go
// through a ProxyJump or a ProxyCommand: a host behind one is often not
// there to dial.
func warnProxy(proto, alias, proxy, addr string) {
	if _, done := proxyWarned.LoadOrStore(alias, true); !done {
		Log.Add(proto, DirWarn, fmt.Sprintf("%s: ssh_config gives it %s, which sc does not follow: sc dials %s itself", alias, proxy, addr))
	}
}

// readSSHConfig reads the user's ~/.ssh/config, then the system's, as ssh
// does, for the options sc uses: the first value obtained wins and
// IdentityFile adds up, whatever block it is in; Host takes patterns (*,
// ? and !); Key=value and quotes are read. Match criteria are ssh's to
// judge, so a Match block applies to no host, but for "Match all".
// Include is not read.
func readSSHConfig(alias string) sshConfig {
	var cfg sshConfig
	set := map[string]bool{}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		parseSSHConfig(filepath.Join(home, ".ssh", "config"), alias, &cfg, set)
	}
	if sshSystemConfig != "" {
		parseSSHConfig(sshSystemConfig, alias, &cfg, set)
	}
	// HostName's %h is the alias.
	cfg.hostName = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(cfg.hostName, "%%", "\x00"), "%h", alias), "\x00", "%")
	return cfg
}

func parseSSHConfig(path, alias string, cfg *sshConfig, set map[string]bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	host := strings.ToLower(alias)
	applies := true // what comes before the first Host is for every host
	for _, line := range strings.Split(string(data), "\n") {
		key, args := configLine(line)
		switch {
		case len(args) == 0:
			continue
		case key == "host":
			applies = hostMatches(args, host)
			continue
		case key == "match":
			applies = len(args) == 1 && strings.EqualFold(args[0], "all")
			continue
		case !applies:
			continue
		case key == "identityfile":
			cfg.identityFiles = append(cfg.identityFiles, args[0])
			continue
		case key == "proxyjump" || key == "proxycommand":
			// The first of the two wins over the other too.
			if !set["proxy"] {
				set["proxy"] = true
				if v := strings.Join(args, " "); v != "none" {
					cfg.proxy = proxyName[key] + " " + v
				}
			}
			continue
		case set[key]:
			continue
		}
		v := args[0]
		switch key {
		case "hostname":
			cfg.hostName = v
		case "user":
			cfg.user = v
		case "port":
			cfg.port = v
		case "hostkeyalias":
			cfg.hostKeyAlias = v
		case "identitiesonly":
			cfg.identitiesOnly = strings.EqualFold(v, "yes")
		case "identityagent":
			cfg.agent = v
		default:
			continue
		}
		set[key] = true
	}
}

// configLine splits an ssh_config line into its keyword, in lower case,
// and its arguments: "Key value", "Key=value" and "Key = value" are the
// same; an argument may be quoted; an unquoted # starts a comment.
func configLine(line string) (string, []string) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] == '#' {
		return "", nil
	}
	i := strings.IndexAny(line, " \t=")
	if i < 0 {
		return strings.ToLower(line), nil
	}
	key, rest := strings.ToLower(line[:i]), strings.TrimLeft(line[i:], " \t")
	if strings.HasPrefix(rest, "=") {
		rest = strings.TrimLeft(rest[1:], " \t")
	}
	var args []string
	var arg strings.Builder
	inArg := false
	var quote byte
	for j := 0; j < len(rest); j++ {
		c := rest[j]
		switch {
		case quote != 0 && c == quote:
			quote = 0
		case quote != 0:
			arg.WriteByte(c)
		case c == '"' || c == '\'':
			quote, inArg = c, true
		case c == ' ' || c == '\t':
			if inArg {
				args, inArg = append(args, arg.String()), false
				arg.Reset()
			}
		case c == '#' && !inArg:
			j = len(rest)
		default:
			arg.WriteByte(c)
			inArg = true
		}
	}
	if inArg {
		args = append(args, arg.String())
	}
	return key, args
}

// hostMatches is ssh's Host test: a pattern matches, and no !pattern does.
func hostMatches(patterns []string, host string) bool {
	found := false
	for _, p := range patterns {
		neg := strings.HasPrefix(p, "!")
		if neg {
			p = p[1:]
		}
		if wildMatch(p, host) {
			if neg {
				return false
			}
			found = true
		}
	}
	return found
}

// wildMatch is ssh's pattern: * is any run of characters, ? any one.
func wildMatch(p, s string) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 0 && p[0] == '*' {
				p = p[1:]
			}
			if p == "" {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if wildMatch(p, s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if s == "" {
				return false
			}
		default:
			if s == "" || s[0] != p[0] {
				return false
			}
		}
		p, s = p[1:], s[1:]
	}
	return s == ""
}

// sshTokens are the values of ssh's % tokens in a path.
type sshTokens struct{ home, local, host, user, port, alias, keyAlias string }

// expand expands what ssh expands in IdentityFile and IdentityAgent: a
// leading ~, the % tokens, and ${VAR}. A token sc does not know stays.
func (t sshTokens) expand(s string) string {
	if t.home != "" && (s == "~" || strings.HasPrefix(s, "~/")) {
		s = t.home + s[1:]
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+1 < len(s) {
			if v, ok := t.token(s[i+1]); ok {
				b.WriteString(v)
				i++
				continue
			}
		}
		if s[i] == '$' && strings.HasPrefix(s[i+1:], "{") {
			if j := strings.IndexByte(s[i:], '}'); j > 0 {
				b.WriteString(os.Getenv(s[i+2 : i+j]))
				i += j
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func (t sshTokens) token(c byte) (string, bool) {
	switch c {
	case '%':
		return "%", true
	case 'd':
		return t.home, true
	case 'h':
		return t.host, true
	case 'i':
		return strconv.Itoa(os.Getuid()), true
	case 'k':
		if t.keyAlias != "" {
			return t.keyAlias, true
		}
		return t.host, true
	case 'l':
		h, _ := os.Hostname()
		return h, true
	case 'n':
		return t.alias, true
	case 'p':
		return t.port, true
	case 'r':
		return t.user, true
	case 'u':
		return t.local, true
	}
	return "", false
}

// agentSocket is the agent's socket for IdentityAgent's value v:
// SSH_AUTH_SOCK's when v is empty or names it, none for "none", the
// variable's when v is $VAR, else the path v, which ssh -G gives
// expanded and sc expands when it read the file itself.
func agentSocket(v string, t sshTokens, expand bool) string {
	switch {
	case v == "" || v == "SSH_AUTH_SOCK":
		return os.Getenv("SSH_AUTH_SOCK")
	case v == "none":
		return ""
	case strings.HasPrefix(v, "$"):
		return os.Getenv(strings.Trim(v[1:], "{}"))
	case expand:
		return t.expand(v)
	}
	return v
}

// sshSigners orders the keys to offer as ssh does: the agent's keys that
// the configuration names, in its order; then the agent's others, unless
// IdentitiesOnly; then the named keys the agent does not hold. sc offered
// every agent key first, and a server lets a few through (MaxAuthTries,
// 6 by default) before it hangs up (S22).
func sshSigners(agentKeys []ssh.Signer, files []string, identitiesOnly bool) []ssh.Signer {
	var named, fromFiles []ssh.Signer
	used := make([]bool, len(agentKeys))
	for _, f := range files {
		pub, signer := readIdentity(f)
		if pub == nil {
			continue
		}
		held := false
		for i, a := range agentKeys {
			if !used[i] && bytes.Equal(a.PublicKey().Marshal(), pub.Marshal()) {
				named, used[i], held = append(named, a), true, true
				break
			}
		}
		if !held && signer != nil {
			fromFiles = append(fromFiles, signer)
		}
	}
	out := named
	if !identitiesOnly {
		for i, a := range agentKeys {
			if !used[i] {
				out = append(out, a)
			}
		}
	}
	return append(out, fromFiles...)
}

// readIdentity reads an IdentityFile: its public key, and a signer when
// the private key needs no passphrase. The public key comes from the key
// (an encrypted OpenSSH key holds it in the clear), from the file itself
// when it is a public key, as for a key that lives in an agent, or from
// the file with .pub added.
func readIdentity(path string) (ssh.PublicKey, ssh.Signer) {
	if data, err := os.ReadFile(path); err == nil {
		s, err := ssh.ParsePrivateKey(data)
		if err == nil {
			return s.PublicKey(), s
		}
		var missing *ssh.PassphraseMissingError
		if errors.As(err, &missing) && missing.PublicKey != nil {
			return missing.PublicKey, nil
		}
		if pub, _, _, _, err := ssh.ParseAuthorizedKey(data); err == nil {
			return pub, nil
		}
	}
	if data, err := os.ReadFile(path + ".pub"); err == nil {
		if pub, _, _, _, err := ssh.ParseAuthorizedKey(data); err == nil {
			return pub, nil
		}
	}
	return nil, nil
}
