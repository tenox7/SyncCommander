package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"sc/model"
)

// W-L3: sc logs in with Basic only, and a server that asks for another
// login answered 401, which read as a wrong password. Now the error names
// the login asked for, and is still a refused login (model.ErrAuth). A
// server that offers Basic among others says nothing more: the password
// is wrong.
func TestALoginSCCannotDoIsNamed(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		challenges []string
		want       string // in the error; "" for no word of another login
	}{
		{[]string{`Digest realm="dav", qop="auth", nonce="abc, def"`}, "the server asks for a Digest login"},
		{[]string{"Negotiate", "NTLM"}, "the server asks for a Negotiate or NTLM login"},
		{[]string{`Negotiate YIIBhg==, NTLM`}, "the server asks for a Negotiate or NTLM login"},
		{[]string{`Digest realm="x", Basic realm="y"`}, ""},
		{[]string{`Basic realm="a, Digest b"`}, ""},
		{[]string{`Digest realm = "x", qop = "auth"`}, "the server asks for a Digest login"},
		{nil, ""},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, ch := range c.challenges {
				w.Header().Add("WWW-Authenticate", ch)
			}
			w.WriteHeader(http.StatusUnauthorized)
		}))
		b, err := NewWebDAVBackend(ctx, "webdav://u:p@"+srv.Listener.Addr().String()+"/", false, 1)
		if err == nil {
			_, err = b.Stat(ctx, "f")
		}
		srv.Close()
		if !errors.Is(err, model.ErrAuth) {
			t.Errorf("%q: %v; want a refused login", c.challenges, err)
			continue
		}
		got := err.Error()
		if c.want != "" && !strings.Contains(got, c.want) {
			t.Errorf("%q: %q; want it to say %q", c.challenges, got, c.want)
		}
		if c.want == "" && strings.Contains(got, "asks for") {
			t.Errorf("%q: %q; want no word of another login", c.challenges, got)
		}
	}
}

// W-L3: HTTP_PROXY was not kept to: sc went to the server direct, where
// curl and rclone go by the proxy. The child process asks a server it can
// reach only through the proxy the environment names. Go reads those
// variables once a process, so the test runs the child in one of its own.
func TestWebDAVGoesByTheProxyTheEnvironmentNames(t *testing.T) {
	if os.Getenv("SC_TEST_PROXY_CHILD") != "" {
		ctx := context.Background()
		b, err := NewWebDAVBackend(ctx, "webdav://dav.invalid/", false, 1)
		if err == nil {
			_, err = b.Stat(ctx, "f")
		}
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	s := newWDTree()
	s.files["f"] = "by the proxy"
	var mu sync.Mutex
	var hosts []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hosts = append(hosts, r.Host)
		mu.Unlock()
		s.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWebDAVGoesByTheProxyTheEnvironmentNames$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "SC_TEST_PROXY_CHILD=1", "HTTP_PROXY="+proxy.URL, "http_proxy="+proxy.URL, "NO_PROXY=", "no_proxy=", "REQUEST_METHOD=")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the child, by the proxy %s: %v\n%s", proxy.URL, err, out)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(hosts) == 0 || strings.TrimSuffix(hosts[0], ":80") != "dav.invalid" {
		t.Errorf("the proxy was asked for %q, want dav.invalid", hosts)
	}
}
