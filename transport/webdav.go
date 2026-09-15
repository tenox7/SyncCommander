package transport

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
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

var webdavIdle = idleSetting{def: 5 * time.Minute}

func SetWebDAVIdleTimeout(d time.Duration) { webdavIdle.set(d) }

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
	*httpBase
	display    string
	cksumAlgo  string
	availAlgos []string
	sums       *wdSumCache
	listCache  *listCache
	dirs       sync.Map // collections known to exist, so uploads skip the MKCOL walk
	noInfinity atomic.Bool
}

func NewWebDAVBackend(rawURL string, insecure bool, parallel int) (*WebDAVBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	hb, err := newHTTPBase("webdav", scheme == "webdavs", host, port, remotePath, user, pass, insecure, parallel, webdavIdle.get())
	if err != nil {
		return nil, err
	}
	b := &WebDAVBackend{
		httpBase:  hb,
		display:   hb.displayURL(scheme),
		sums:      newWDSumCache(),
		listCache: newListCache(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := b.propfind(ctx, "", "0"); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *WebDAVBackend) BasePath() string { return b.display }

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

// entryFromResponse converts one multistatus response; callers drop the
// listed collection itself by comparing RelPath with the directory asked for.
func (b *WebDAVBackend) entryFromResponse(r wdResponse) (model.FileEntry, map[string]string, bool) {
	prop, ok := okProp(r.Propstat)
	if !ok {
		return model.FileEntry{}, nil, false
	}
	rel, ok := b.hrefToRel(r.Href)
	if !ok {
		return model.FileEntry{}, nil, false
	}
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
	return e, parseChecksums(prop.Checksums), true
}

// okProp picks the successful propstat of a PROPFIND response, tolerating
// servers that omit the status on their only propstat.
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

// propstatOK is the strict form for PROPPATCH, whose single propstat carries
// the verdict: a 403 there is a rejection, not something to tolerate.
func propstatOK(ps []wdPropstat) bool {
	for _, p := range ps {
		if !statusOK(p.Status) {
			return false
		}
	}
	return true
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
	return b.listCache.serve(ctx, relDir, b.liveList)
}

func (b *WebDAVBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	ms, err := b.propfind(ctx, relDir, defaultWDDepth)
	if err != nil {
		return nil, err
	}
	self := strings.Trim(relDir, "/")
	var entries []model.FileEntry
	for _, r := range ms.Response {
		e, sums, ok := b.entryFromResponse(r)
		if !ok || e.RelPath == self {
			continue
		}
		b.sums.put(e.RelPath, sums)
		entries = append(entries, e)
	}
	Log.Add("webdav", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// PreloadRecursive fires a single PROPFIND Depth:infinity in the background,
// streaming entries into the cache grouped by parent dir. Servers may refuse
// Depth:infinity (RFC 4918 §9.1, 403 + propfind-finite-depth); on that
// rejection noInfinity latches so every later scan goes straight to per-dir
// Depth:1 without retrying.
func (b *WebDAVBackend) PreloadRecursive(ctx context.Context, scope string) error {
	if b.noInfinity.Load() {
		return nil
	}
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

func (b *WebDAVBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	Log.Add("webdav", DirOut, "RPROPFIND "+b.pathFor(scope))
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
		Log.Add("webdav", DirErr, "RPROPFIND rejected ("+resp.Status+"); falling back to per-dir Depth:1")
		return fmt.Errorf("PROPFIND infinity %s: %s", scope, resp.Status)
	}
	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("PROPFIND infinity %s: %s", scope, resp.Status)
		Log.Add("webdav", DirErr, "RPROPFIND: "+err.Error())
		return err
	}

	// Multistatus ordering is not guaranteed grouped by parent; the grouper
	// appends, so interleaving only costs extra flushes.
	self := strings.Trim(scope, "/")
	g := &emitGrouper{emit: emit}
	dec := xml.NewDecoder(resp.Body)
	n := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			Log.Add("webdav", DirErr, "RPROPFIND decode: "+err.Error())
			return err
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "response" {
			continue
		}
		var r wdResponse
		if err := dec.DecodeElement(&r, &se); err != nil {
			return err
		}
		e, sums, ok := b.entryFromResponse(r)
		if !ok || e.RelPath == self {
			continue
		}
		b.sums.put(e.RelPath, sums)
		g.add(e)
		n++
	}
	g.finish()
	Log.Add("webdav", DirIn, fmt.Sprintf("RPROPFIND %d entries", n))
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
		if e, sums, ok := b.entryFromResponse(r); ok {
			b.sums.put(e.RelPath, sums)
		}
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

// probeAlgos descends the first subdirectory chain until a listing carries
// checksums, so a tree whose top level is all directories still probes.
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
			e, sums, ok := b.entryFromResponse(r)
			if !ok || e.RelPath == dir {
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
		Log.Add("webdav", DirErr, err.Error())
		return err
	}
	var ms wdMultistatus
	if xml.NewDecoder(resp.Body).Decode(&ms) == nil {
		for _, r := range ms.Response {
			if !propstatOK(r.Propstat) {
				err := fmt.Errorf("PROPPATCH %s: property update rejected", relPath)
				Log.Add("webdav", DirErr, err.Error())
				return err
			}
		}
	}
	return nil
}

// CopyFrom PUTs the body with its length, so servers that insist on
// Content-Length are served and nothing is chunked needlessly. The parent
// walk is skipped for collections already seen; a MKCOL failure is not fatal
// here because the PUT's own status is the verdict.
func (b *WebDAVBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	if err := b.ensureDir(ctx, parentDir(relPath)); err != nil {
		Log.Add("webdav", DirErr, "mkcol parents: "+err.Error())
	}
	req, err := b.newReq(ctx, "PUT", b.urlFor(relPath, false), src)
	if err != nil {
		return err
	}
	if sz, ok := fileSizeFromContext(ctx); ok && sz >= 0 {
		req.ContentLength = sz
	}
	if mt, ok := modTimeFromContext(ctx); ok {
		req.Header.Set("X-OC-Mtime", strconv.FormatInt(mt.Unix(), 10))
	}
	resp, err := b.doReq(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("PUT %s: %s", relPath, resp.Status)
		Log.Add("webdav", DirErr, err.Error())
		return err
	}
	b.dirs.Store(parentDir(relPath), struct{}{})
	b.sums.invalidate(relPath)
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *WebDAVBackend) Mkdir(ctx context.Context, relPath string, _ os.FileMode) error {
	if err := b.ensureDir(ctx, relPath); err != nil {
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// ensureDir MKCOLs dir and any missing parents, remembering what exists.
// 405 and 301 mean the collection is already there; 409 is tolerated because
// some servers answer it for existing collections too.
func (b *WebDAVBackend) ensureDir(ctx context.Context, dir string) error {
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	if _, ok := b.dirs.Load(dir); ok {
		return nil
	}
	if err := b.ensureDir(ctx, parentDir(dir)); err != nil {
		return err
	}
	resp, err := b.do(ctx, "MKCOL", b.urlFor(dir, true), nil, nil)
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	switch code := resp.StatusCode; {
	case code/100 == 2, code == http.StatusMethodNotAllowed, code == http.StatusConflict, code == http.StatusMovedPermanently:
		b.dirs.Store(dir, struct{}{})
		return nil
	default:
		return fmt.Errorf("MKCOL %s: %s", dir, resp.Status)
	}
}

func (b *WebDAVBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	if err := b.ensureDir(ctx, parentDir(newRelPath)); err != nil {
		Log.Add("webdav", DirErr, "mkcol parents: "+err.Error())
	}
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
		Log.Add("webdav", DirErr, err.Error())
		return err
	}
	b.dirs.Clear()
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
	resp, err := b.do(ctx, "DELETE", b.urlFor(relPath, false), nil, nil)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		err := fmt.Errorf("DELETE %s: %s", relPath, resp.Status)
		Log.Add("webdav", DirErr, err.Error())
		return err
	}
	b.dirs.Clear()
	b.sums.invalidate(relPath)
	b.listCache.invalidateTree(relPath)
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *WebDAVBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(ctx, relPath, 0)
}

// OpenAt resumes from offset via a Range request; a server that ignores Range
// (200 instead of 206) is handled by discarding the prefix.
func (b *WebDAVBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	var hdrs map[string]string
	if offset > 0 {
		hdrs = map[string]string{"Range": fmt.Sprintf("bytes=%d-", offset)}
	}
	resp, err := b.do(ctx, "GET", b.urlFor(relPath, false), nil, hdrs)
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
