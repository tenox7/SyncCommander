package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

// refusingSSH serves ssh on a local port and refuses every password. It
// returns the address and how many logins it saw.
func refusingSSH(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var logins atomic.Int64
	cfg := &ssh.ServerConfig{PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
		logins.Add(1)
		return nil, errors.New("wrong password")
	}}
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
				_, _, _, _ = ssh.NewServerConn(nc, cfg)
			}()
		}
	}()
	return l.Addr().String(), &logins
}

// noLocalSSHSetup keeps the user's ssh agent, keys and config out of a test.
func noLocalSSHSetup(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	t.Setenv("HOME", t.TempDir())
}

// F7: a wrong password cost a login per connect inside each try inside
// each try: 36 refused logins for one call at the default settings, and as
// many again for the next call, which can lock the account or get the
// address banned. The login is tried once, and no call tries it again.
func TestWrongPasswordLogsInOnce(t *testing.T) {
	noLocalSSHSetup(t)
	SetMaxRetries(1)
	defer SetMaxRetries(5)
	addr, logins := refusingSSH(t)
	b, err := TryOpenBackend("sftp://u:wrong@"+addr+"/x", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	for i := range 3 {
		if _, err := b.List(ctx, ""); !errors.Is(err, model.ErrAuth) {
			t.Errorf("List %d: err = %v, want a refused login", i+1, err)
		}
	}
	if n := logins.Load(); n != 1 {
		t.Errorf("%d logins for 3 calls, want 1", n)
	}
	if !NotConnecting(b) {
		t.Error("the backend does not say it stopped connecting, so opening it again would keep it")
	}
}

// F7: the same for a WebDAV server that answers 401.
func TestWebDAVRefusedLoginAsksOnce(t *testing.T) {
	SetMaxRetries(1)
	defer SetMaxRetries(5)
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("WWW-Authenticate", `Basic realm="x"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()
	b, err := TryOpenBackend("webdav://u:wrong@"+strings.TrimPrefix(srv.URL, "http://")+"/x", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(b)
	ctx := context.Background()
	for i := range 3 {
		if _, err := b.List(ctx, ""); !errors.Is(err, model.ErrAuth) {
			t.Errorf("List %d: err = %v, want a refused login", i+1, err)
		}
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("%d requests for 3 calls, want 1", n)
	}
}

// closedAddr returns a local address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// F7: with the server gone, one call made a connect per try inside each of
// its tries, and every later call did the same: about 11 minutes per file
// of a copy at the default settings. The connects now form one backoff for
// the whole backend, which no call can multiply: max-retries+1 of them,
// then the backend stops connecting for a while and every call fails at
// once.
func TestGoneServerConnectsAreBounded(t *testing.T) {
	noLocalSSHSetup(t)
	SetMaxRetries(2)
	defer SetMaxRetries(5)
	url := "sftp://u@" + closedAddr(t) + "/x"
	var dials atomic.Int64
	b := NewLazyBackend(url, func(ctx context.Context) (model.Backend, error) {
		dials.Add(1)
		return dialSFTPBackend(ctx, url, true, 1)
	})
	defer CloseBackend(b)
	ctx := context.Background()
	if _, err := b.List(ctx, ""); !GivesUp(err) {
		t.Errorf("first List: err = %v, want the backend to stop connecting", err)
	}
	if n := dials.Load(); n != 3 {
		t.Errorf("%d connects for the first call, want 3 (max-retries+1)", n)
	}
	t0 := time.Now()
	for i := range 3 {
		if _, err := b.List(ctx, ""); !GivesUp(err) {
			t.Errorf("List %d after: err = %v, want it to fail at once", i+2, err)
		}
	}
	if n := dials.Load(); n != 3 {
		t.Errorf("%d connects after 4 calls, want still 3", n)
	}
	if d := time.Since(t0); d > time.Second {
		t.Errorf("3 calls to a backend that stopped connecting took %v, want at once", d)
	}
	if !NotConnecting(b) {
		t.Error("the backend does not say it stopped connecting")
	}
}

// A connection that comes back after a few failed connects is used again,
// and the failures are forgotten.
func TestConnectsRecoverWithinTheBudget(t *testing.T) {
	SetMaxRetries(3)
	defer SetMaxRetries(5)
	var dials atomic.Int64
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		if dials.Add(1) <= 2 {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}
		}
		return &connStub{}, nil
	})
	defer CloseBackend(b)
	if _, err := b.List(context.Background(), ""); err != nil {
		t.Fatalf("List = %v, want it to get through on the third connect", err)
	}
	if NotConnecting(b) {
		t.Error("the backend stopped connecting although the server came back")
	}
}

// D3: the reconnect decision read the error's text, so a file whose name
// holds "connection reset" dropped a healthy connection, and with it every
// transfer that shared it.
func TestFileNameInAnErrorKeepsTheConnection(t *testing.T) {
	SetMaxRetries(0)
	defer SetMaxRetries(5)
	var dials atomic.Int64
	first := &connStub{}
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) {
		if dials.Add(1) == 1 {
			return first, nil
		}
		return &connStub{}, nil
	})
	defer CloseBackend(b)
	ctx := context.Background()
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	named := error(errors.New(`sftp: open "/x/connection reset by peer.txt": permission denied`))
	first.listErr.Store(&named)
	if _, err := b.List(ctx, ""); err == nil {
		t.Fatal("List did not fail")
	}
	if first.closed.Load() || dials.Load() != 1 {
		t.Errorf("an error naming a file \"connection reset by peer.txt\" dropped the connection (closed=%v, %d connects)", first.closed.Load(), dials.Load())
	}
}

// D4: a Retry inside a Retry multiplied the tries. The outer one owns them;
// the inner one runs its op once per outer try.
func TestNestedRetryRunsOnce(t *testing.T) {
	SetMaxRetries(2)
	defer SetMaxRetries(5)
	var calls atomic.Int64
	err := Retry(context.Background(), "test", "outer", func(ctx context.Context) error {
		return Retry(ctx, "test", "inner", func(context.Context) error {
			calls.Add(1)
			return errors.New("transient")
		})
	})
	if err == nil {
		t.Fatal("Retry = nil, want the error")
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("%d calls, want 3 (max-retries+1)", n)
	}
}

// pathAuthStub refuses one folder, as a WebDAV share whose folder asks for
// other credentials.
type pathAuthStub struct {
	connStub
	calls atomic.Int64
}

func (s *pathAuthStub) List(_ context.Context, dir string) ([]model.FileEntry, error) {
	s.calls.Add(1)
	if dir == "private" {
		return nil, model.WithKind(model.ErrAuth, errors.New("PROPFIND /x/private/: 401 Unauthorized"))
	}
	return nil, nil
}

// After a call got through, a refusal is about its path alone: that call
// fails without a retry, and the backend goes on. Before, it is the login,
// and it is kept.
func TestRefusalAfterALoginIsAboutThePath(t *testing.T) {
	SetMaxRetries(2)
	defer SetMaxRetries(5)
	ctx := context.Background()

	s := &pathAuthStub{}
	b := NewLazyBackend("webdav://host/x", func(context.Context) (model.Backend, error) { return s, nil })
	defer CloseBackend(b)
	if _, err := b.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, "private"); !errors.Is(err, model.ErrAuth) {
		t.Errorf("List of the refused folder: err = %v, want a refusal", err)
	}
	if n := s.calls.Load(); n != 2 {
		t.Errorf("%d calls, want 2: a refusal is not tried again", n)
	}
	if _, err := b.List(ctx, ""); err != nil {
		t.Errorf("List after a folder's refusal = %v, want the backend to go on", err)
	}
	if NotConnecting(b) {
		t.Error("one folder's refusal stopped the whole backend")
	}

	s2 := &pathAuthStub{}
	b2 := NewLazyBackend("webdav://host/x", func(context.Context) (model.Backend, error) { return s2, nil })
	defer CloseBackend(b2)
	for range 3 {
		if _, err := b2.List(ctx, "private"); !errors.Is(err, model.ErrAuth) {
			t.Errorf("List: err = %v, want a refusal", err)
		}
	}
	if n := s2.calls.Load(); n != 1 {
		t.Errorf("%d calls refused before any got through, want 1: the login is wrong", n)
	}
}
