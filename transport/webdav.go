package transport

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sc/model"
)

const defaultWDDepth = "1"

const wdPropfindBody = `<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
 <d:prop>
  <d:getlastmodified/>
  <d:getcontentlength/>
  <d:resourcetype/>
  <oc:checksums/>
 </d:prop>
</d:propfind>`

var webdavIdleNanos atomic.Int64

func SetWebDAVIdleTimeout(d time.Duration) {
	if d <= 0 {
		webdavIdleNanos.Store(-1)
		return
	}
	webdavIdleNanos.Store(int64(d))
}

func webdavIdle() time.Duration {
	n := webdavIdleNanos.Load()
	if n == 0 {
		return 5 * time.Minute
	}
	if n < 0 {
		return 0
	}
	return time.Duration(n)
}

func protoManagesLiveness(proto string) bool {
	return proto == "webdav" || proto == "webdavs"
}

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

type wdMultistatus struct {
	XMLName  xml.Name     `xml:"multistatus"`
	Response []wdResponse `xml:"response"`
}

type wdResponse struct {
	Href     string       `xml:"href"`
	Propstat []wdPropstat `xml:"propstat"`
}

type wdPropstat struct {
	Status string `xml:"status"`
	Prop   wdProp `xml:"prop"`
}

type wdProp struct {
	LastModified string    `xml:"getlastmodified"`
	ContentLen   string    `xml:"getcontentlength"`
	Collection   *struct{} `xml:"resourcetype>collection"`
	Checksums    []string  `xml:"checksums>checksum"`
}

type WebDAVBackend struct {
	client     *http.Client
	baseURL    *url.URL
	base       string
	display    string
	user       string
	pass       string
	cksumAlgo  string
	availAlgos []string
	sums       *wdSumCache
	listCache  *rsyncListCache
	noInfinity atomic.Bool
}

func NewWebDAVBackend(rawURL string, insecure bool, parallel int) (*WebDAVBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)

	httpScheme := "http"
	if scheme == "webdavs" {
		httpScheme = "https"
	}
	if port == "" {
		if httpScheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	hostport := net.JoinHostPort(host, port)
	baseURL, err := url.Parse(httpScheme + "://" + hostport)
	if err != nil {
		return nil, fmt.Errorf("webdav: bad url: %v", err)
	}
	base := strings.Trim(remotePath, "/")

	maxIdle := parallel + 2
	if maxIdle < 8 {
		maxIdle = 8
	}
	idleTimeout := webdavIdle()
	connIdle := idleTimeout
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
			tc := &idleTimeoutConn{Conn: c, idle: idleTimeout}
			return tc, tc.nudge()
		},
	}

	displayHost := host
	if !(httpScheme == "http" && port == "80") && !(httpScheme == "https" && port == "443") {
		displayHost = hostport
	}
	display := scheme + "://"
	if user != "" {
		display += user + "@"
	}
	display += displayHost + "/" + base

	b := &WebDAVBackend{
		client:  &http.Client{Transport: tr},
		baseURL: baseURL,
		base:    base,
		display: display,
		user:    user,
		pass:    pass,
		sums:    newWDSumCache(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := b.propfind(ctx, "", "0"); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *WebDAVBackend) BasePath() string { return b.display }

func (b *WebDAVBackend) Close() error {
	b.client.CloseIdleConnections()
	return nil
}

func (b *WebDAVBackend) pathFor(relPath string) string {
	rel := strings.Trim(relPath, "/")
	switch {
	case b.base != "" && rel != "":
		return "/" + b.base + "/" + rel
	case b.base != "":
		return "/" + b.base
	case rel != "":
		return "/" + rel
	default:
		return "/"
	}
}

func (b *WebDAVBackend) urlFor(relPath string, dir bool) string {
	u := *b.baseURL
	u.Path = b.pathFor(relPath)
	if dir && u.Path != "/" {
		u.Path += "/"
	}
	return u.String()
}

func (b *WebDAVBackend) do(ctx context.Context, method, rawurl string, body io.Reader, hdrs map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawurl, body)
	if err != nil {
		return nil, err
	}
	if b.user != "" || b.pass != "" {
		req.SetBasicAuth(b.user, b.pass)
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	Log.Add("webdav", ">>>", method+" "+rawurl)
	resp, err := b.client.Do(req)
	if err != nil {
		Log.Add("webdav", "ERR", err.Error())
		return nil, err
	}
	return resp, nil
}

func drainClose(rc io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(rc, 64<<10))
	rc.Close()
}

func (b *WebDAVBackend) propfind(ctx context.Context, relPath, depth string) (*wdMultistatus, error) {
	resp, err := b.do(ctx, "PROPFIND", b.urlFor(relPath, true), strings.NewReader(wdPropfindBody), map[string]string{
		"Depth":        depth,
		"Content-Type": "application/xml",
	})
	if err != nil {
		return nil, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PROPFIND %s: %s", relPath, resp.Status)
	}
	var ms wdMultistatus
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, fmt.Errorf("PROPFIND %s: decode: %v", relPath, err)
	}
	return &ms, nil
}

func (b *WebDAVBackend) hrefToRel(href string) (string, bool) {
	u, err := url.Parse(href)
	if err != nil {
		return "", false
	}
	p := path.Clean("/" + strings.Trim(u.Path, "/"))
	if b.base != "" {
		basePrefix := "/" + b.base
		if p != basePrefix && !strings.HasPrefix(p, basePrefix+"/") {
			return "", false
		}
		p = p[len(basePrefix):]
	}
	return strings.Trim(p, "/"), true
}

func (b *WebDAVBackend) entryFromResponse(r wdResponse, listDir string) (model.FileEntry, map[string]string, bool, bool) {
	prop, ok := okProp(r.Propstat)
	if !ok {
		return model.FileEntry{}, nil, false, false
	}
	rel, ok := b.hrefToRel(r.Href)
	if !ok {
		return model.FileEntry{}, nil, false, false
	}
	isSelf := rel == strings.Trim(listDir, "/")
	isDir := prop.Collection != nil
	mode := os.FileMode(0644)
	if isDir {
		mode = os.ModeDir | 0755
	}
	size, _ := strconv.ParseInt(strings.TrimSpace(prop.ContentLen), 10, 64)
	e := model.FileEntry{
		RelPath: rel,
		Name:    path.Base(rel),
		Size:    size,
		ModTime: parseWDTime(prop.LastModified),
		IsDir:   isDir,
		Mode:    mode,
	}
	return e, parseChecksums(prop.Checksums), isSelf, true
}

func okProp(ps []wdPropstat) (wdProp, bool) {
	for _, p := range ps {
		if statusOK(p.Status) {
			return p.Prop, true
		}
	}
	if len(ps) == 1 {
		return ps[0].Prop, true
	}
	return wdProp{}, false
}

func statusOK(s string) bool {
	for _, f := range strings.Fields(s) {
		if c, err := strconv.Atoi(f); err == nil {
			return c >= 200 && c < 300
		}
	}
	return s == ""
}

func parseChecksums(list []string) map[string]string {
	out := map[string]string{}
	for _, s := range list {
		for _, tok := range strings.Fields(s) {
			i := strings.IndexByte(tok, ':')
			if i <= 0 {
				continue
			}
			algo := normAlgo(tok[:i])
			hexv := strings.ToLower(tok[i+1:])
			if algo != "" && hexv != "" {
				out[algo] = hexv
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normAlgo(a string) string {
	switch strings.ToLower(strings.TrimSpace(a)) {
	case "md5":
		return "md5"
	case "sha1", "sha-1":
		return "sha1"
	case "sha256", "sha-256":
		return "sha256"
	}
	return ""
}

func parseWDTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC1123,
		time.RFC1123Z,
		"Mon, 2 Jan 2006 15:04:05 MST",
		time.RFC850,
		time.ANSIC,
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (b *WebDAVBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if b.listCache != nil {
		entries, hit, active, done := b.listCache.lookup(relDir)
		if hit {
			return entries, nil
		}
		if active && !done {
			if e, ok := b.listCache.await(relDir); ok {
				return e, nil
			}
		}
	}
	return b.liveList(ctx, relDir)
}

func (b *WebDAVBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	Log.Add("webdav", ">>>", "PROPFIND "+b.pathFor(relDir))
	ms, err := b.propfind(ctx, relDir, defaultWDDepth)
	if err != nil {
		return nil, err
	}
	var entries []model.FileEntry
	for _, r := range ms.Response {
		e, sums, isSelf, ok := b.entryFromResponse(r, relDir)
		if !ok || isSelf {
			continue
		}
		if sums != nil {
			b.sums.put(e.RelPath, sums)
		}
		entries = append(entries, e)
	}
	Log.Add("webdav", "<<<", fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// PreloadRecursive fires a single PROPFIND Depth:infinity in the background,
// streaming entries into the cache grouped by parent dir. Returns immediately;
// subsequent List calls under scope hit or wait on the cache. Servers may
// refuse Depth:infinity (RFC 4918 §9.1, 403 + propfind-finite-depth); on that
// rejection we latch noInfinity so every later scan goes straight to per-dir
// Depth:1 without retrying. A second preload while one is in flight is a no-op.
func (b *WebDAVBackend) PreloadRecursive(ctx context.Context, scope string) error {
	if b.noInfinity.Load() {
		return nil
	}
	if b.listCache == nil {
		b.listCache = newRsyncListCache()
	}
	c := b.listCache
	c.mu.Lock()
	if c.active && !c.done {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	c.reset(ctx, scope)

	go func() {
		_ = b.runRecursiveList(ctx, scope, c.emit)
		c.finish()
	}()
	return nil
}

func (b *WebDAVBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	Log.Add("webdav", ">>>", "RPROPFIND "+b.pathFor(scope))
	resp, err := b.do(ctx, "PROPFIND", b.urlFor(scope, true), strings.NewReader(wdPropfindBody), map[string]string{
		"Depth":        "infinity",
		"Content-Type": "application/xml",
	})
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode == http.StatusForbidden {
		b.noInfinity.Store(true)
		Log.Add("webdav", "ERR", "RPROPFIND rejected ("+resp.Status+"); falling back to per-dir Depth:1")
		return fmt.Errorf("PROPFIND infinity %s: %s", scope, resp.Status)
	}
	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("PROPFIND infinity %s: %s", scope, resp.Status)
		Log.Add("webdav", "ERR", "RPROPFIND: "+err.Error())
		return err
	}

	// rsync's recursive output revisits parents, so emit appends rather than
	// replaces; we flush a parent's batch whenever the running parent changes.
	// WebDAV multistatus ordering isn't guaranteed grouped, but append keeps
	// it correct either way (just more flushes when parents interleave).
	var current string
	var haveCurrent bool
	var batch []model.FileEntry
	var seenDirs []string
	flush := func() {
		if !haveCurrent {
			return
		}
		emit(current, batch)
		batch = nil
		haveCurrent = false
	}

	dec := xml.NewDecoder(resp.Body)
	n := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			flush()
			Log.Add("webdav", "ERR", "RPROPFIND decode: "+err.Error())
			return err
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "response" {
			continue
		}
		var r wdResponse
		if err := dec.DecodeElement(&r, &se); err != nil {
			flush()
			return err
		}
		e, sums, isSelf, ok := b.entryFromResponse(r, scope)
		if !ok || isSelf {
			continue
		}
		if sums != nil {
			b.sums.put(e.RelPath, sums)
		}
		parent := parentDir(e.RelPath)
		if !haveCurrent || parent != current {
			flush()
			current = parent
			haveCurrent = true
		}
		batch = append(batch, e)
		if e.IsDir {
			seenDirs = append(seenDirs, e.RelPath)
		}
		n++
	}
	flush()
	// Register every dir we saw so an empty leaf reports a cache hit (with no
	// children) instead of falling through to a live per-dir PROPFIND.
	for _, d := range seenDirs {
		emit(d, nil)
	}
	Log.Add("webdav", "<<<", fmt.Sprintf("RPROPFIND %d entries", n))
	return nil
}

func (b *WebDAVBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "" {
		return "", fmt.Errorf("no checksum algorithm configured")
	}
	if h, ok := b.sums.get(relPath, b.cksumAlgo); ok {
		return h, nil
	}
	if err := b.statInto(ctx, relPath); err != nil {
		return "", err
	}
	if h, ok := b.sums.get(relPath, b.cksumAlgo); ok {
		return h, nil
	}
	return "", fmt.Errorf("no %s checksum for %s", b.cksumAlgo, relPath)
}

func (b *WebDAVBackend) statInto(ctx context.Context, relPath string) error {
	ms, err := b.propfind(ctx, relPath, "0")
	if err != nil {
		return err
	}
	for _, r := range ms.Response {
		e, sums, _, ok := b.entryFromResponse(r, "\x00")
		if !ok || sums == nil {
			continue
		}
		b.sums.put(e.RelPath, sums)
	}
	return nil
}

func (b *WebDAVBackend) ProbeChecksums() []string {
	if b.availAlgos != nil {
		return b.availAlgos
	}
	b.availAlgos = []string{}
	seen := b.probeAlgos(context.Background())
	for _, a := range []string{"sha256", "sha1", "md5"} {
		if seen[a] {
			b.availAlgos = append(b.availAlgos, a)
		}
	}
	return b.availAlgos
}

func (b *WebDAVBackend) probeAlgos(ctx context.Context) map[string]bool {
	seen := map[string]bool{}
	dir := ""
	for depth := 0; depth < 4; depth++ {
		ms, err := b.propfind(ctx, dir, "1")
		if err != nil {
			return seen
		}
		firstSub := ""
		found := false
		for _, r := range ms.Response {
			e, sums, isSelf, ok := b.entryFromResponse(r, dir)
			if !ok || isSelf {
				continue
			}
			if sums != nil {
				b.sums.put(e.RelPath, sums)
				for a := range sums {
					seen[a] = true
				}
				found = true
			}
			if e.IsDir && firstSub == "" {
				firstSub = e.RelPath
			}
		}
		if found || firstSub == "" {
			return seen
		}
		dir = firstSub
	}
	return seen
}

func (b *WebDAVBackend) SetChecksumAlgo(algo string) { b.cksumAlgo = algo }

func (b *WebDAVBackend) SetTimes(ctx context.Context, relPath string, mtime, _, _ time.Time) error {
	body := fmt.Sprintf(`<?xml version="1.0"?>`+
		`<d:propertyupdate xmlns:d="DAV:"><d:set><d:prop>`+
		`<d:lastmodified>%d</d:lastmodified>`+
		`</d:prop></d:set></d:propertyupdate>`, mtime.Unix())
	resp, err := b.do(ctx, "PROPPATCH", b.urlFor(relPath, false), strings.NewReader(body), map[string]string{
		"Content-Type": "application/xml",
	})
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode/100 != 2 {
		err := fmt.Errorf("PROPPATCH %s: %s", relPath, resp.Status)
		Log.Add("webdav", "ERR", err.Error())
		return err
	}
	var ms wdMultistatus
	if xml.NewDecoder(resp.Body).Decode(&ms) == nil {
		for _, r := range ms.Response {
			if _, ok := okProp(r.Propstat); !ok {
				err := fmt.Errorf("PROPPATCH %s: property update rejected", relPath)
				Log.Add("webdav", "ERR", err.Error())
				return err
			}
		}
	}
	return nil
}

func (b *WebDAVBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	if err := b.mkdirAll(ctx, parentDir(relPath)); err != nil {
		Log.Add("webdav", "ERR", "mkcol parents: "+err.Error())
	}
	var hdrs map[string]string
	putLog := "PUT " + relPath
	if mt, ok := modTimeFromContext(ctx); ok {
		hdrs = map[string]string{"X-OC-Mtime": strconv.FormatInt(mt.Unix(), 10)}
		putLog = fmt.Sprintf("PUT %s X-OC-Mtime=%d", relPath, mt.Unix())
	}
	Log.Add("webdav", ">>>", putLog)
	resp, err := b.do(ctx, "PUT", b.urlFor(relPath, false), src, hdrs)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("PUT %s: %s", relPath, resp.Status)
		Log.Add("webdav", "ERR", err.Error())
		return err
	}
	b.sums.invalidate(relPath)
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *WebDAVBackend) Mkdir(ctx context.Context, relPath string, _ os.FileMode) error {
	Log.Add("webdav", ">>>", "MKCOL "+relPath)
	if err := b.mkdirAll(ctx, relPath); err != nil {
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *WebDAVBackend) mkdirAll(ctx context.Context, dir string) error {
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	parts := strings.Split(dir, "/")
	cur := ""
	for _, p := range parts {
		if cur == "" {
			cur = p
		} else {
			cur += "/" + p
		}
		resp, err := b.do(ctx, "MKCOL", b.urlFor(cur, true), nil, nil)
		if err != nil {
			return err
		}
		code := resp.StatusCode
		drainClose(resp.Body)
		switch {
		case code/100 == 2, code == http.StatusMethodNotAllowed, code == http.StatusConflict, code == http.StatusMovedPermanently:
		case code == http.StatusUnauthorized, code == http.StatusForbidden:
			return fmt.Errorf("MKCOL %s: %s", cur, resp.Status)
		}
	}
	return nil
}

func (b *WebDAVBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	if err := b.mkdirAll(ctx, parentDir(newRelPath)); err != nil {
		Log.Add("webdav", "ERR", "mkcol parents: "+err.Error())
	}
	Log.Add("webdav", ">>>", "MOVE "+oldRelPath+" -> "+newRelPath)
	resp, err := b.do(ctx, "MOVE", b.urlFor(oldRelPath, false), nil, map[string]string{
		"Destination": b.urlFor(newRelPath, false),
		"Overwrite":   "T",
	})
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("MOVE %s: %s", oldRelPath, resp.Status)
		Log.Add("webdav", "ERR", err.Error())
		return err
	}
	b.sums.invalidate(oldRelPath)
	b.listCache.invalidateTree(oldRelPath)
	b.listCache.invalidateAncestors(oldRelPath)
	b.listCache.invalidateAncestors(newRelPath)
	return nil
}

func (b *WebDAVBackend) Remove(ctx context.Context, relPath string) error {
	return b.delete(ctx, relPath)
}

func (b *WebDAVBackend) RemoveAll(ctx context.Context, relPath string) error {
	return b.delete(ctx, relPath)
}

func (b *WebDAVBackend) delete(ctx context.Context, relPath string) error {
	Log.Add("webdav", ">>>", "DELETE "+relPath)
	resp, err := b.do(ctx, "DELETE", b.urlFor(relPath, false), nil, nil)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		err := fmt.Errorf("DELETE %s: %s", relPath, resp.Status)
		Log.Add("webdav", "ERR", err.Error())
		return err
	}
	b.sums.invalidate(relPath)
	b.listCache.invalidateTree(relPath)
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *WebDAVBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	Log.Add("webdav", ">>>", "GET "+relPath)
	resp, err := b.do(ctx, "GET", b.urlFor(relPath, false), nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		drainClose(resp.Body)
		return nil, fmt.Errorf("GET %s: %s", relPath, resp.Status)
	}
	return resp.Body, nil
}

func (b *WebDAVBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	if offset <= 0 {
		return b.Open(ctx, relPath)
	}
	Log.Add("webdav", ">>>", fmt.Sprintf("GET %s @%d", relPath, offset))
	resp, err := b.do(ctx, "GET", b.urlFor(relPath, false), nil, map[string]string{
		"Range": fmt.Sprintf("bytes=%d-", offset),
	})
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		return resp.Body, nil
	case http.StatusOK:
		if _, err := io.CopyN(io.Discard, resp.Body, offset); err != nil {
			drainClose(resp.Body)
			return nil, err
		}
		return resp.Body, nil
	default:
		drainClose(resp.Body)
		return nil, fmt.Errorf("GET %s @%d: %s", relPath, offset, resp.Status)
	}
}

type wdSumCache struct {
	mu sync.Mutex
	m  map[string]map[string]string
}

func newWDSumCache() *wdSumCache {
	return &wdSumCache{m: make(map[string]map[string]string)}
}

func (c *wdSumCache) put(relPath string, sums map[string]string) {
	if len(sums) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[relPath]
	if e == nil {
		e = make(map[string]string, len(sums))
		c.m[relPath] = e
	}
	for k, v := range sums {
		e[k] = v
	}
}

func (c *wdSumCache) get(relPath, algo string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.m[relPath]; e != nil {
		h, ok := e[algo]
		return h, ok
	}
	return "", false
}

func (c *wdSumCache) invalidate(relPath string) {
	c.mu.Lock()
	delete(c.m, relPath)
	c.mu.Unlock()
}
