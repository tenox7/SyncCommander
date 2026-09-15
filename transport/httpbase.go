package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// idleSetting is a configurable idle deadline: unset means def, a
// non-positive set disables it.
type idleSetting struct {
	n   atomic.Int64
	def time.Duration
}

func (s *idleSetting) set(d time.Duration) {
	if d <= 0 {
		d = -1
	}
	s.n.Store(int64(d))
}

func (s *idleSetting) get() time.Duration {
	n := s.n.Load()
	switch {
	case n == 0:
		return s.def
	case n < 0:
		return 0
	}
	return time.Duration(n)
}

// idleTimeoutConn enforces a no-bytes-for-this-long deadline at the socket
// (not a total deadline), so a long listing survives while a dead connection
// still aborts.
type idleTimeoutConn struct {
	net.Conn
	idle time.Duration
}

func (c *idleTimeoutConn) nudge() error {
	if c.idle <= 0 {
		return nil
	}
	return c.SetDeadline(time.Now().Add(c.idle))
}

func (c *idleTimeoutConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err == nil && n > 0 {
		_ = c.nudge()
	}
	return n, err
}

func (c *idleTimeoutConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err == nil && n > 0 {
		_ = c.nudge()
	}
	return n, err
}

// httpBase is the request plumbing the WebDAV and restic backends share: one
// metered transport with idle deadlines, basic auth, an optional Accept
// header, path mapping under base, and a log line per request.
type httpBase struct {
	client      *http.Client
	baseURL     *url.URL
	base        string
	displayHost string
	proto       string
	user        string
	pass        string
	accept      string
}

func newHTTPBase(proto string, secure bool, host, port, base, user, pass string, insecure bool, parallel int, idle time.Duration) (*httpBase, error) {
	httpScheme, defPort := "http", "80"
	if secure {
		httpScheme, defPort = "https", "443"
	}
	if port == "" {
		port = defPort
	}
	hostport := net.JoinHostPort(host, port)
	baseURL, err := url.Parse(httpScheme + "://" + hostport)
	if err != nil {
		return nil, fmt.Errorf("%s: bad url: %v", proto, err)
	}
	maxIdle := max(parallel+2, 8)
	connIdle := idle
	if connIdle <= 0 || connIdle > 90*time.Second {
		connIdle = 90 * time.Second
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{ServerName: host, InsecureSkipVerify: insecure},
		DisableCompression:    true,
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   maxIdle,
		IdleConnTimeout:       connIdle,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			tc := &idleTimeoutConn{Conn: LimitConn(c), idle: idle}
			return tc, tc.nudge()
		},
	}
	displayHost := host
	if port != defPort {
		displayHost = hostport
	}
	return &httpBase{
		client:      &http.Client{Transport: tr},
		baseURL:     baseURL,
		base:        strings.Trim(base, "/"),
		displayHost: displayHost,
		proto:       proto,
		user:        user,
		pass:        pass,
	}, nil
}

// displayURL is the password-free form shown in the UI.
func (h *httpBase) displayURL(scheme string) string {
	s := scheme + "://"
	if h.user != "" {
		s += h.user + "@"
	}
	return s + h.displayHost + "/" + h.base
}

func (h *httpBase) Close() error {
	h.client.CloseIdleConnections()
	return nil
}

// pathFor is the absolute server path of relPath under base.
func (h *httpBase) pathFor(relPath string) string {
	p := "/" + h.base
	if rel := strings.Trim(relPath, "/"); rel != "" {
		p = strings.TrimSuffix(p, "/") + "/" + rel
	}
	return p
}

// urlFor addresses relPath, with a trailing slash for collections.
func (h *httpBase) urlFor(relPath string, dir bool) string {
	u := *h.baseURL
	u.Path = h.pathFor(relPath)
	if dir && !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String()
}

func (h *httpBase) newReq(ctx context.Context, method, rawurl string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawurl, body)
	if err != nil {
		return nil, err
	}
	if h.user != "" || h.pass != "" {
		req.SetBasicAuth(h.user, h.pass)
	}
	if h.accept != "" {
		req.Header.Set("Accept", h.accept)
	}
	return req, nil
}

func (h *httpBase) doReq(req *http.Request) (*http.Response, error) {
	Log.Add(h.proto, DirOut, req.Method+" "+req.URL.String())
	resp, err := h.client.Do(req)
	if err != nil {
		Log.Add(h.proto, DirErr, err.Error())
		return nil, err
	}
	return resp, nil
}

func (h *httpBase) do(ctx context.Context, method, rawurl string, body io.Reader, hdrs map[string]string) (*http.Response, error) {
	req, err := h.newReq(ctx, method, rawurl, body)
	if err != nil {
		return nil, err
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	return h.doReq(req)
}

func drainClose(rc io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(rc, 64<<10))
	rc.Close()
}
