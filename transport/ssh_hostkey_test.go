package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"sc/model"
)

// hostKeySSH serves ssh on a local port with an Ed25519 and an ECDSA host
// key, as servers have, and takes the password pw; x/crypto asks for the
// ECDSA key first. It returns the address and both keys.
func hostKeySSH(t *testing.T) (addr string, ed, ec ssh.PublicKey) {
	t.Helper()
	cfg := &ssh.ServerConfig{PasswordCallback: func(_ ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
		if string(pw) == "pw" {
			return nil, nil
		}
		return nil, errors.New("wrong password")
	}}
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var keys []ssh.PublicKey
	for _, k := range []any{edKey, ecKey} {
		signer, err := ssh.NewSignerFromKey(k)
		if err != nil {
			t.Fatal(err)
		}
		cfg.AddHostKey(signer)
		keys = append(keys, signer.PublicKey())
	}
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
	return l.Addr().String(), keys[0], keys[1]
}

// sshFiles gives the test a home of its own with files in its ~/.ssh,
// and a system known_hosts that holds global, or none for "".
func sshFiles(t *testing.T, files map[string]string, global string) {
	t.Helper()
	noLocalSSHSetup(t)
	dir := filepath.Join(os.Getenv("HOME"), ".ssh")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := globalKnownHosts
	t.Cleanup(func() { globalKnownHosts = old })
	globalKnownHosts = nil
	if global != "" {
		p := filepath.Join(t.TempDir(), "ssh_known_hosts")
		if err := os.WriteFile(p, []byte(global), 0o644); err != nil {
			t.Fatal(err)
		}
		globalKnownHosts = []string{p}
	}
}

func knownLine(name string, key ssh.PublicKey) string {
	return knownhosts.Line([]string{knownhosts.Normalize(name)}, key) + "\n"
}

func dialed(t *testing.T, url string, insecure bool) error {
	t.Helper()
	conn, err := dialSSH(context.Background(), url, insecure)
	if err == nil {
		conn.client.Close()
	}
	return err
}

// S15: x/crypto asks for an ECDSA host key before an Ed25519 one, whatever
// known_hosts holds. A server with both, known_hosts holding its Ed25519
// key alone, was a "HOST KEY MISMATCH": a false alarm, which teaches to
// pass -insecure. Now the types known_hosts holds are asked for first. A
// key that does not match is still a mismatch, and a host not filed is
// still unknown.
func TestSSHAsksForTheHostKeyKnownHostsHolds(t *testing.T) {
	addr, ed, ec := hostKeySSH(t)
	url := "sftp://u:pw@" + addr + "/"
	sshFiles(t, map[string]string{"known_hosts": knownLine(addr, ed)}, "")
	if err := dialed(t, url, false); err != nil {
		t.Errorf("known_hosts holds the server's Ed25519 key: %v; want a login", err)
	}
	sshFiles(t, map[string]string{"known_hosts": knownLine(addr, ec)}, "")
	if err := dialed(t, url, false); err != nil {
		t.Errorf("known_hosts holds the server's ECDSA key: %v; want a login", err)
	}
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ssh.NewPublicKey(otherKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	sshFiles(t, map[string]string{"known_hosts": knownLine(addr, other)}, "")
	if err := dialed(t, url, false); err == nil || !strings.Contains(err.Error(), "HOST KEY MISMATCH") || !errors.Is(err, model.ErrAuth) {
		t.Errorf("known_hosts holds another key: %v; want a HOST KEY MISMATCH", err)
	}
	sshFiles(t, map[string]string{"known_hosts": knownLine("elsewhere", ed)}, "")
	if err := dialed(t, url, false); err == nil || !strings.Contains(err.Error(), "not in known_hosts") {
		t.Errorf("known_hosts holds no key for the host: %v; want it unknown", err)
	}
}

// S15: HostKeyAlias was not read. Through ssh -L the server answers at a
// localhost port that changes, and only an alias can name its key; with
// no way to name it, -insecure was the way through. Now the key is looked
// for under the alias alone, with no port, and not under the address, as
// ssh does.
func TestSSHHostKeyAliasNamesTheKey(t *testing.T) {
	addr, ed, _ := hostKeySSH(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("Host tun\n  HostName %s\n  Port %s\n  HostKeyAlias realhost\n", host, port)
	sshFiles(t, map[string]string{"config": config, "known_hosts": knownLine("realhost", ed)}, "")
	if err := dialed(t, "sftp://u:pw@tun/", false); err != nil {
		t.Errorf("known_hosts holds the key under the HostKeyAlias: %v; want a login", err)
	}
	sshFiles(t, map[string]string{"config": config, "known_hosts": knownLine(addr, ed)}, "")
	if err := dialed(t, "sftp://u:pw@tun/", false); err == nil {
		t.Error("with a HostKeyAlias, a key filed under the address let sc in; ssh looks under the alias alone")
	}
}

// S15: the system's known_hosts (/etc/ssh/ssh_known_hosts) was not read:
// a key an admin filed there for every user was unknown to sc.
func TestSSHReadsTheSystemKnownHosts(t *testing.T) {
	addr, ed, _ := hostKeySSH(t)
	sshFiles(t, nil, knownLine(addr, ed))
	if err := dialed(t, "sftp://u:pw@"+addr+"/", false); err != nil {
		t.Errorf("the system known_hosts holds the key: %v; want a login", err)
	}
}

// S15: -insecure took any host key without a word, and then gave the
// password to whoever answered. Now it says so, once for each address.
func TestSSHInsecureSaysTheHostKeyIsNotChecked(t *testing.T) {
	addr, _, _ := hostKeySSH(t)
	sshFiles(t, nil, "")
	insecureWarned.Delete(addr) // a port another test used before
	want := "-insecure: the host key of " + addr + " is not checked, and the password goes to whoever answers there"
	count := func() (n int) {
		for _, l := range Log.Lines() {
			if strings.Contains(l, want) {
				n++
			}
		}
		return n
	}
	before := count()
	for range 2 {
		if err := dialed(t, "sftp://u:pw@"+addr+"/", true); err != nil {
			t.Fatal(err)
		}
	}
	if n := count() - before; n != 1 {
		t.Errorf("%d warnings %q for two logins; want one", n, want)
	}
}
