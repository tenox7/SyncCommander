package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"sc/model"
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
		client:      &http.Client{Transport: tr, CheckRedirect: noRedirect},
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

// pathFor is the absolute server path of relPath under base (cleanRel).
func (h *httpBase) pathFor(relPath string) string {
	p := "/" + h.base
	if rel := cleanRel(relPath); rel != "" {
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

// noRedirect keeps the client from following a redirect. Go sends a
// request that got a 301, 302 or 303 again as a GET, whatever its method,
// and sc took the GET's answer for the MOVE's, PUT's or DELETE's (W-H3).
// doReq decides what a redirect means.
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// folderMethods may name a folder, whose path ends with a slash.
var folderMethods = map[string]bool{"PROPFIND": true, "PROPPATCH": true, "DELETE": true, "MOVE": true, "COPY": true, "MKCOL": true}

// folderSlash reports a redirect to the request's own path with a slash at
// its end: Apache's answer (mod_dir) to any method on a folder's path
// without one. It returns the request's URL with that slash, on the host
// the request went to.
func folderSlash(req *http.Request, resp *http.Response) (*url.URL, bool) {
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
	default:
		return nil, false
	}
	loc, err := resp.Location()
	if err != nil || strings.HasSuffix(req.URL.Path, "/") || loc.Path != req.URL.Path+"/" {
		return nil, false
	}
	to := *req.URL
	to.Path += "/"
	if to.RawPath != "" {
		to.RawPath += "/"
	}
	return &to, true
}

// doReq sends req. A redirect is no answer (noRedirect): one that names the
// path a folder sends a method that may name one again there, once, and
// any other fails the call.
func (h *httpBase) doReq(req *http.Request) (*http.Response, error) {
	Log.Add(h.proto, DirOut, req.Method+" "+req.URL.Redacted())
	resp, err := h.client.Do(req)
	if err != nil {
		Log.Add(h.proto, DirErr, err.Error())
		return nil, err
	}
	to, folder := folderSlash(req, resp)
	if folder && folderMethods[req.Method] && (req.Body == nil || req.GetBody != nil) {
		drainClose(resp.Body)
		again := req.Clone(req.Context())
		again.URL = to
		if req.GetBody != nil {
			if again.Body, err = req.GetBody(); err != nil {
				return nil, err
			}
		}
		req = again
		Log.Add(h.proto, DirOut, req.Method+" "+req.URL.Redacted()+" (a folder, says the server)")
		if resp, err = h.client.Do(req); err != nil {
			Log.Add(h.proto, DirErr, err.Error())
			return nil, err
		}
		folder = false
	}
	if resp.StatusCode/100 == 3 {
		drainClose(resp.Body)
		msg := fmt.Sprintf("%s %s: %s to %s: sc follows no redirect", req.Method, req.URL.Redacted(), resp.Status, resp.Header.Get("Location"))
		if folder {
			msg = fmt.Sprintf("%s %s: the server says it is a folder (%s)", req.Method, req.URL.Redacted(), resp.Status)
		}
		err := &statusError{code: resp.StatusCode, msg: msg}
		Log.Add(h.proto, DirErr, err.Error())
		return nil, err
	}
	// The answers no backend reads for itself, of the kind their code says
	// (D3): the login refused, and the server full.
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusProxyAuthRequired, http.StatusInsufficientStorage:
		drainClose(resp.Body)
		err := statusErr(resp, "%s %s", req.Method, req.URL.Redacted())
		Log.Add(h.proto, DirErr, err.Error())
		return nil, err
	}
	return resp, nil
}

// statusError is an HTTP answer that failed a request. What it means is
// read from its code, never from its text, which holds the path: a 404 or
// a 403 was tried again and again, and a 502 for "no such file.bin" was
// taken for a missing file and tried once (W-M1).
type statusError struct {
	code int
	msg  string
}

func (e *statusError) Error() string { return e.msg }

// Unwrap is the kind the code says: a missing file (404, 410), one sc may
// not touch (403), a refused login (401, 407), a full server (507).
func (e *statusError) Unwrap() error {
	switch e.code {
	case http.StatusNotFound, http.StatusGone:
		return fs.ErrNotExist
	case http.StatusForbidden:
		return fs.ErrPermission
	case http.StatusUnauthorized, http.StatusProxyAuthRequired:
		return model.ErrAuth
	case http.StatusInsufficientStorage:
		return model.ErrNoSpace
	}
	return nil
}

// final reports an answer another try of the same request gets again: a
// redirect; a 4xx but a timeout (408), a lock (423, which rclone answers
// to two requests on one path at once), too early (425) and too many
// (429); and the 5xx that say what the server cannot do.
func (e *statusError) final() bool {
	switch e.code {
	case http.StatusRequestTimeout, http.StatusLocked, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	case http.StatusNotImplemented, http.StatusHTTPVersionNotSupported, http.StatusInsufficientStorage,
		http.StatusLoopDetected, http.StatusNotExtended, http.StatusNetworkAuthenticationRequired:
		return true
	}
	return e.code/100 == 3 || e.code/100 == 4
}

// statusErr is the error of resp failing the request what:
// "what: 404 Not Found".
func statusErr(resp *http.Response, what string, args ...any) error {
	return &statusError{code: resp.StatusCode, msg: fmt.Sprintf(what, args...) + ": " + resp.Status}
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
