package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// Tests read no ssh configuration but the ones they write: ssh -G reads
// the user's own, from the home the password file gives whatever $HOME
// says, and the system's.
func init() {
	sshConfigCommand = ""
	sshSystemConfig = ""
}

// s22Config holds what sc misread: a Match block, patterns and !patterns,
// Key=value, quotes, % tokens, and a Host * first whose values win.
const s22Config = `# what sc misread
Host *.example web?
  HostName=%h.lan
  Port = 2222
  IdentityFile "~/.ssh/my key"   # a comment
  IdentityFile ~/.ssh/id_%r@%h

Host web1
  User first

Match host web1
  User matched
  Port 3333
  HostKeyAlias matchalias

Host !web2 web*  # not *
  HostKeyAlias walias

Host *
  User fallback
  IdentitiesOnly yes
`

// S22: sc read ~/.ssh/config itself, and a part of it: options under a
// Match line went to the Host before it, Host patterns, Key=value and
// quoted values were not read, a value under Host * lost to a later one
// though ssh takes the first. sc asks ssh now (ssh -G), and reads the
// files itself, better, where there is no ssh.
func TestSSHConfigReadAsSSHReadsIt(t *testing.T) {
	noLocalSSHSetup(t)
	home := os.Getenv("HOME")
	dir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "config")
	if err := os.WriteFile(file, []byte(s22Config), 0o600); err != nil {
		t.Fatal(err)
	}
	type want struct {
		host, user, port, keyAlias string
		only                       bool
		keys                       []string
	}
	cases := []struct {
		alias, user, port string
		want              want
	}{
		{"web1", "", "", want{"web1.lan", "first", "2222", "walias", true, []string{dir + "/my key", dir + "/id_first@web1.lan"}}},
		{"web2", "", "", want{"web2.lan", "fallback", "2222", "", true, []string{dir + "/my key", dir + "/id_fallback@web2.lan"}}},
		{"other", "bob", "2020", want{"other", "bob", "2020", "", true, nil}},
	}
	check := func(t *testing.T) {
		for _, c := range cases {
			cfg := resolveSSHConfig(context.Background(), c.alias, c.user, c.port)
			got := want{cfg.hostName, cfg.user, cfg.port, cfg.hostKeyAlias, cfg.identitiesOnly, cfg.identityFiles}
			if c.want.keys == nil {
				got.keys = nil // ssh's defaults
				if !containsString(cfg.identityFiles, dir+"/id_ed25519") {
					t.Errorf("%s: keys %q, want ssh's defaults", c.alias, cfg.identityFiles)
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%s: got %+v\nwant %+v", c.alias, got, c.want)
			}
		}
	}
	t.Run("sc's own reader", check)
	t.Run("the system's file, after the user's", func(t *testing.T) {
		sys := filepath.Join(t.TempDir(), "ssh_config")
		if err := os.WriteFile(sys, []byte("Host sys\n  HostName from.system\nHost *\n  Port 2299\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		sshSystemConfig = sys
		t.Cleanup(func() { sshSystemConfig = "" })
		if cfg := resolveSSHConfig(context.Background(), "sys", "", ""); cfg.hostName != "from.system" || cfg.port != "2299" {
			t.Errorf("sys: host %q port %q, want the system file's", cfg.hostName, cfg.port)
		}
		if cfg := resolveSSHConfig(context.Background(), "web1", "", ""); cfg.port != "2222" {
			t.Errorf("web1: port %q, want the user's 2222 before the system's", cfg.port)
		}
	})
	t.Run("ssh -G", func(t *testing.T) {
		if _, err := exec.LookPath("ssh"); err != nil {
			t.Skip("no ssh here")
		}
		if err := exec.Command("ssh", "-G", "-F", file, "--", "web1").Run(); err != nil {
			t.Skipf("this ssh has no -G: %v", err)
		}
		sshConfigCommand, sshConfigFile = "ssh", file
		t.Cleanup(func() { sshConfigCommand, sshConfigFile = "", "" })
		check(t)
	})
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// keySSH is an ssh server that lets in the password pw and the key
// accept, and notes each key a client offers, in order.
func keySSH(t *testing.T, accept ssh.PublicKey) (addr string, offered func() []string) {
	t.Helper()
	var mu sync.Mutex
	var keys []string
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
			if string(pw) == "pw" {
				return nil, nil
			}
			return nil, errors.New("no")
		},
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			mu.Lock()
			keys = append(keys, ssh.FingerprintSHA256(key))
			mu.Unlock()
			if accept != nil && bytes.Equal(key.Marshal(), accept.Marshal()) {
				return nil, nil
			}
			return nil, errors.New("no")
		},
	}
	_, hk, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(hk)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AddHostKey(signer)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer nc.Close()
				_, chans, reqs, err := ssh.NewServerConn(nc, cfg)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					ch.Reject(ssh.Prohibited, "no channels here")
				}
			}()
		}
	}()
	return l.Addr().String(), func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), keys...)
	}
}

// testAgent serves keys from an agent of its own and gives its socket.
// The socket's folder is short: a unix socket's path has about 100 bytes.
func testAgent(t *testing.T, keys ...ed25519.PrivateKey) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sca")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	kr := agent.NewKeyring()
	for _, k := range keys {
		if err := kr.Add(agent.AddedKey{PrivateKey: k}); err != nil {
			t.Fatal(err)
		}
	}
	l, err := net.Listen("unix", filepath.Join(dir, "s"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				agent.ServeAgent(kr, c)
			}()
		}
	}()
	return l.Addr().String()
}

func newKey(t *testing.T) (ed25519.PrivateKey, ssh.PublicKey, string) {
	t.Helper()
	_, k, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ssh.NewPublicKey(k.Public())
	if err != nil {
		t.Fatal(err)
	}
	return k, pub, ssh.FingerprintSHA256(pub)
}

// writeKeys puts files in the test's ~/.ssh: a config for host "keys" at
// addr with lines, and key files.
func writeKeys(t *testing.T, addr string, lines string, files map[string][]byte) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(os.Getenv("HOME"), ".ssh")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	files["config"] = []byte(fmt.Sprintf("Host keys\n  HostName %s\n  Port %s\n%s", host, port, lines))
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// S22: sc offered every key its agent held, then the files the
// configuration names; ssh offers the agent's keys the configuration
// names first, the agent's others after, none of them with
// IdentitiesOnly, and the named keys the agent lacks last. A server lets
// a few keys through before it hangs up, so the named key was never
// reached behind an agent full of others.
func TestSSHOffersTheConfiguredKeyFirst(t *testing.T) {
	a, pubA, fa := newKey(t)
	b, pubB, fb := newKey(t)
	c, pubC, fc := newKey(t)
	d, pubD, fd := newKey(t)
	encC, err := ssh.MarshalPrivateKeyWithPassphrase(c, "", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	plainD, err := ssh.MarshalPrivateKey(d, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("the named key first", func(t *testing.T) {
		noLocalSSHSetup(t)
		t.Setenv("SSH_AUTH_SOCK", testAgent(t, a, b, c))
		addr, offered := keySSH(t, pubC)
		// c is a key with a passphrase: its public half is in the clear.
		writeKeys(t, addr, "  IdentityFile ~/.ssh/c\n", map[string][]byte{"c": pem.EncodeToMemory(encC)})
		if err := dialed(t, "sftp://u@keys/", true); err != nil {
			t.Fatalf("the agent holds the named key: %v", err)
		}
		if got := offered(); len(got) == 0 || got[0] != fc {
			t.Errorf("offered %q, want the named key %s first", got, fc)
		}
	})
	t.Run("IdentitiesOnly", func(t *testing.T) {
		noLocalSSHSetup(t)
		t.Setenv("SSH_AUTH_SOCK", testAgent(t, a, b, c))
		addr, offered := keySSH(t, pubD)
		// b has its public half beside it alone, a.pub is a public key
		// itself, both held by the agent; d is a key on disk alone.
		writeKeys(t, addr, "  IdentitiesOnly yes\n  IdentityFile ~/.ssh/b\n  IdentityFile ~/.ssh/a.pub\n  IdentityFile ~/.ssh/d\n", map[string][]byte{
			"b.pub": ssh.MarshalAuthorizedKey(pubB), "a.pub": ssh.MarshalAuthorizedKey(pubA), "d": pem.EncodeToMemory(plainD),
		})
		if err := dialed(t, "sftp://u@keys/", true); err != nil {
			t.Fatalf("the key on disk: %v", err)
		}
		if got := offered(); !reflect.DeepEqual(got, []string{fb, fa, fd}) {
			t.Errorf("offered %q, want the named keys alone, in the configuration's order: %s, %s, %s (not %s)", got, fb, fa, fd, fc)
		}
	})
}

// S22: IdentityAgent was not read: sc asked SSH_AUTH_SOCK's agent where
// ssh asks the one the configuration names, and asked one where it says
// none.
func TestSSHAsksTheAgentTheConfigNames(t *testing.T) {
	a, pubA, _ := newKey(t)
	c, pubC, fc := newKey(t)
	t.Run("another agent", func(t *testing.T) {
		noLocalSSHSetup(t)
		t.Setenv("SSH_AUTH_SOCK", testAgent(t, a))
		addr, offered := keySSH(t, pubC)
		writeKeys(t, addr, "  IdentityAgent "+testAgent(t, c)+"\n", map[string][]byte{})
		if err := dialed(t, "sftp://u@keys/", true); err != nil {
			t.Errorf("the agent the configuration names holds the key: %v", err)
		}
		if got := offered(); !reflect.DeepEqual(got, []string{fc}) {
			t.Errorf("offered %q, want that agent's key alone", got)
		}
	})
	t.Run("none", func(t *testing.T) {
		noLocalSSHSetup(t)
		t.Setenv("SSH_AUTH_SOCK", testAgent(t, a))
		addr, offered := keySSH(t, pubA)
		writeKeys(t, addr, "  IdentityAgent none\n", map[string][]byte{})
		if err := dialed(t, "sftp://u@keys/", true); err == nil {
			t.Error("IdentityAgent none, and the agent's key got in")
		}
		if got := offered(); len(got) != 0 {
			t.Errorf("offered %q, want no agent's key", got)
		}
	})
}

// S22: a ProxyJump or ProxyCommand was not read: sc dialed the host
// itself, where ssh goes through the proxy, and said nothing. It still
// dials it, and says so.
func TestSSHSaysItDoesNotFollowAProxy(t *testing.T) {
	noLocalSSHSetup(t)
	addr, _ := keySSH(t, nil)
	writeKeys(t, addr, "  ProxyJump jump.invalid\n", map[string][]byte{})
	proxyWarned.Delete("keys") // once a host, and a run of the test before
	mark := len(Log.Lines())
	if err := dialed(t, "sftp://u:pw@keys/", true); err != nil {
		t.Fatalf("the host is there to dial: %v", err)
	}
	if !loggedSince(mark, "keys: ssh_config gives it ProxyJump jump.invalid, which sc does not follow: sc dials "+addr+" itself") {
		t.Error("no line says sc dialed the host itself")
	}
}

// fakeSSH puts an ssh on the test's PATH that notes its arguments in the
// file it returns and runs script.
func fakeSSH(t *testing.T, script string) (cmd, args string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("a shell script for ssh")
	}
	dir := t.TempDir()
	args = filepath.Join(dir, "args")
	cmd = filepath.Join(dir, "ssh")
	body := fmt.Sprintf("#!/bin/sh\nfor a in \"$@\"; do echo \"$a\" >> %s; done\n%s\n", args, script)
	if err := os.WriteFile(cmd, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return cmd, args
}

// S22: the configuration comes from ssh -G, with the URL's user and port,
// the host after --; a host that starts with - is no host and goes to no
// ssh. Where ssh -G fails, sc reads the files itself and says why.
func TestSSHConfigComesFromSSHG(t *testing.T) {
	noLocalSSHSetup(t)
	ctx := context.Background()
	cmd, args := fakeSSH(t, "printf 'hostname fake.example\\nuser fakeuser\\nport 2200\\nidentitiesonly yes\\nidentityfile ~/.ssh/k_%%r\\nhostkeyalias fakealias\\nproxyjump j.example\\nidentityagent /x/agent.sock\\n'")
	sshConfigCommand = cmd
	t.Cleanup(func() { sshConfigCommand = "" })
	cfg := resolveSSHConfig(ctx, "plain", "", "")
	if cfg.hostName != "fake.example" || cfg.user != "fakeuser" || cfg.port != "2200" || !cfg.identitiesOnly ||
		!reflect.DeepEqual(cfg.identityFiles, []string{filepath.Join(os.Getenv("HOME"), ".ssh", "k_fakeuser")}) ||
		cfg.hostKeyAlias != "fakealias" || cfg.proxy != "ProxyJump j.example" || cfg.agent != "/x/agent.sock" {
		t.Errorf("got %+v, want ssh -G's answer", cfg)
	}
	resolveSSHConfig(ctx, "plain", "bob", "2020")
	data, _ := os.ReadFile(args)
	if want := "-G\n--\nplain\n-G\n-l\nbob\n-p\n2020\n--\nplain\n"; string(data) != want {
		t.Errorf("ssh was run with\n%s\nwant\n%s", data, want)
	}
	os.Remove(args)
	resolveSSHConfig(ctx, "-oProxyCommand=x", "", "")
	if _, err := os.Stat(args); err == nil {
		t.Error("a host that starts with - went to ssh")
	}

	bad, _ := fakeSSH(t, "echo 'line 3: bad option' >&2; exit 255")
	sshConfigCommand = bad
	dir := filepath.Join(os.Getenv("HOME"), ".ssh")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("Host broken\n  HostName from.file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sshConfigWarned.Delete("broken")
	mark := len(Log.Lines())
	if cfg := resolveSSHConfig(ctx, "broken", "", ""); cfg.hostName != "from.file" {
		t.Errorf("ssh -G failed and the host is %q; want the file's", cfg.hostName)
	}
	if !loggedSince(mark, "line 3: bad option") {
		t.Error("no line says why ssh -G gave no answer")
	}
}
