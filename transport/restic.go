package transport

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"sc/model"
)

// resticAPIV2 is the Accept value that selects the v2 list format (objects with
// name+size in one response). rclone serve restic is v2-only and answers 400 to
// anything else, so we send it on every request; we still branch on the
// response Content-Type to tolerate a v1-only upstream rest-server.
const resticAPIV2 = "application/vnd.x.restic.rest.v2"

// resticTypes are the object kinds a restic repo exposes as top-level dirs.
// config is a separate singleton at the repo root, handled out of band.
var resticTypes = []string{"data", "index", "keys", "locks", "snapshots"}

var resticIdleNanos atomic.Int64

func SetResticIdleTimeout(d time.Duration) {
	if d <= 0 {
		resticIdleNanos.Store(-1)
		return
	}
	resticIdleNanos.Store(int64(d))
}

func resticIdle() time.Duration {
	n := resticIdleNanos.Load()
	if n == 0 {
		return 5 * time.Minute
	}
	if n < 0 {
		return 0
	}
	return time.Duration(n)
}

// ResticBackend speaks the restic REST API (restic://, restics://), the
// protocol implemented by `rclone serve restic` and the restic rest-server. The
// repo is a content-addressed object store with a fixed shallow shape: a config
// file plus five type dirs (data/index/keys/locks/snapshots) of objects whose
// names are the SHA-256 of their content. We present that as a two-level tree.
type ResticBackend struct {
	client  *http.Client
	baseURL *url.URL
	base    string
	display string
	user    string
	pass    string
	// listCache is populated by PreloadRecursive (one GET per type) and shared
	// with the WebDAV backend's recursive-preload machinery. nil until the
	// first preload; the cache methods are nil-safe so writes can ignore that.
	listCache *rsyncListCache
}

var (
	_ model.Backend            = (*ResticBackend)(nil)
	_ model.ChecksumProber     = (*ResticBackend)(nil)
	_ model.SeekableOpener     = (*ResticBackend)(nil)
	_ model.RecursivePreloader = (*ResticBackend)(nil)
)

func NewResticBackend(rawURL string, insecure bool, parallel int) (*ResticBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)

	httpScheme := "http"
	if scheme == "restics" {
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
		return nil, fmt.Errorf("restic: bad url: %v", err)
	}
	base := strings.Trim(remotePath, "/")

	maxIdle := parallel + 2
	if maxIdle < 8 {
		maxIdle = 8
	}
	// idleTimeoutConn enforces a no-bytes-for-this-long deadline at the socket
	// (not a total deadline), so a legitimately long type listing survives while
	// a dead connection still aborts. Mirrors the WebDAV backend.
	idleTimeout := resticIdle()
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

	b := &ResticBackend{
		client:  &http.Client{Transport: tr},
		baseURL: baseURL,
		base:    base,
		display: display,
		user:    user,
		pass:    pass,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.validate(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *ResticBackend) BasePath() string { return b.display }

func (b *ResticBackend) Close() error {
	b.client.CloseIdleConnections()
	return nil
}

// validate confirms the endpoint speaks the restic REST API before we commit to
// it. An existing config means a live repo; a 404 there can still be a freshly
// created (not yet initialized) repo, so we fall back to a v2 listing of the
// data type — any restic REST server answers that with 200 (rclone returns 200
// + [] even when the dir is absent).
func (b *ResticBackend) validate(ctx context.Context) error {
	resp, err := b.do(ctx, "HEAD", b.objectURL("config"), nil, nil)
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	if resp.StatusCode/100 == 2 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		if _, err := b.listType(ctx, "data"); err == nil {
			return nil
		}
	}
	return fmt.Errorf("restic: %s: %s", b.display, resp.Status)
}

func (b *ResticBackend) pathFor(relPath string) string {
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

// objectURL addresses a single object (or config) by its flat path. Note data
// objects use the flat name here (data/<hash>) even though the server shards
// them on disk as data/<aa>/<hash> — the sharding is server-side only.
func (b *ResticBackend) objectURL(relPath string) string {
	u := *b.baseURL
	u.Path = b.pathFor(relPath)
	return u.String()
}

// typeURL is the listing URL for a type dir. The trailing slash is mandatory:
// restic REST routes a slash-terminated GET to a listing and a bare path to a
// single-object fetch.
func (b *ResticBackend) typeURL(typeName string) string {
	u := *b.baseURL
	u.Path = b.pathFor(typeName)
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String()
}

// newReq builds a request with basic auth and the v2 Accept header, which the
// restic client sends on every call (not just listings).
func (b *ResticBackend) newReq(ctx context.Context, method, rawurl string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawurl, body)
	if err != nil {
		return nil, err
	}
	if b.user != "" || b.pass != "" {
		req.SetBasicAuth(b.user, b.pass)
	}
	req.Header.Set("Accept", resticAPIV2)
	return req, nil
}

func (b *ResticBackend) doReq(req *http.Request) (*http.Response, error) {
	Log.Add("restic", ">>>", req.Method+" "+req.URL.String())
	resp, err := b.client.Do(req)
	if err != nil {
		Log.Add("restic", "ERR", err.Error())
		return nil, err
	}
	return resp, nil
}

func (b *ResticBackend) do(ctx context.Context, method, rawurl string, body io.Reader, hdrs map[string]string) (*http.Response, error) {
	req, err := b.newReq(ctx, method, rawurl, body)
	if err != nil {
		return nil, err
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	return b.doReq(req)
}

func isResticType(name string) bool {
	for _, t := range resticTypes {
		if name == t {
			return true
		}
	}
	return false
}

// List serves from the recursive-preload cache when one is in flight or done,
// otherwise lists live. Mirrors the WebDAV backend's cache-then-live path.
func (b *ResticBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
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

// liveList resolves a directory directly. The repo root is synthesized (no list
// call); a type dir is fetched; anything else is a leaf with no children.
func (b *ResticBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	rel := strings.Trim(relDir, "/")
	if rel == "" {
		return b.rootEntries(ctx), nil
	}
	if isResticType(rel) {
		return b.listType(ctx, rel)
	}
	return nil, nil
}

// rootEntries is the repo's fixed top level (the depth-0 view): the five type
// dirs plus config when present. It needs no listing call — only a cheap HEAD
// to size config.
func (b *ResticBackend) rootEntries(ctx context.Context) []model.FileEntry {
	entries := make([]model.FileEntry, 0, len(resticTypes)+1)
	for _, t := range resticTypes {
		entries = append(entries, model.FileEntry{
			RelPath: t,
			Name:    t,
			IsDir:   true,
			Mode:    os.ModeDir | 0755,
		})
	}
	if e, ok := b.statConfig(ctx); ok {
		entries = append(entries, e)
	}
	return entries
}

func (b *ResticBackend) statConfig(ctx context.Context) (model.FileEntry, bool) {
	resp, err := b.do(ctx, "HEAD", b.objectURL("config"), nil, nil)
	if err != nil {
		return model.FileEntry{}, false
	}
	drainClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		return model.FileEntry{}, false
	}
	size := resp.ContentLength
	if size < 0 {
		size = 0
	}
	return model.FileEntry{
		RelPath: "config",
		Name:    "config",
		Size:    size,
		ModTime: parseWDTime(resp.Header.Get("Last-Modified")),
		Mode:    0644,
	}, true
}

func (b *ResticBackend) listType(ctx context.Context, typeName string) ([]model.FileEntry, error) {
	resp, err := b.do(ctx, "GET", b.typeURL(typeName), nil, nil)
	if err != nil {
		return nil, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("restic: GET %s/: %s", typeName, resp.Status)
	}
	return b.decodeList(resp, typeName)
}

// decodeList parses a type listing. v2 carries names and sizes together and is
// stream-decoded element by element so a data/ dir with millions of objects
// never fully buffers. A non-v2 Content-Type means the server fell back to v1.
func (b *ResticBackend) decodeList(resp *http.Response, typeName string) ([]model.FileEntry, error) {
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), resticAPIV2) {
		return b.decodeListV1(resp, typeName)
	}
	dec := json.NewDecoder(resp.Body)
	if _, err := dec.Token(); err != nil { // consume the opening '['
		return nil, fmt.Errorf("restic: list %s: %v", typeName, err)
	}
	var entries []model.FileEntry
	for dec.More() {
		var it struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
		}
		if err := dec.Decode(&it); err != nil {
			return nil, fmt.Errorf("restic: list %s: %v", typeName, err)
		}
		if it.Name == "" {
			continue
		}
		entries = append(entries, objectEntry(typeName, it.Name, it.Size))
	}
	Log.Add("restic", "<<<", fmt.Sprintf("%s/: %d objects", typeName, len(entries)))
	return entries, nil
}

// decodeListV1 handles the legacy v1 format (names only). Sizes require a HEAD
// per object, which is brutal on large dirs — only a v1-only rest-server takes
// this path; rclone is always v2.
func (b *ResticBackend) decodeListV1(resp *http.Response, typeName string) ([]model.FileEntry, error) {
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, fmt.Errorf("restic: list %s (v1): %v", typeName, err)
	}
	Log.Add("restic", "<<<", fmt.Sprintf("v1 %s/: %d objects, HEAD per object for sizes", typeName, len(names)))
	entries := make([]model.FileEntry, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		entries = append(entries, objectEntry(typeName, name, b.statSize(resp.Request.Context(), typeName+"/"+name)))
	}
	return entries, nil
}

func objectEntry(typeName, name string, size int64) model.FileEntry {
	return model.FileEntry{
		RelPath: typeName + "/" + name,
		Name:    name,
		Size:    size,
		Mode:    0644,
	}
}

func (b *ResticBackend) statSize(ctx context.Context, relPath string) int64 {
	resp, err := b.do(ctx, "HEAD", b.objectURL(relPath), nil, nil)
	if err != nil {
		return 0
	}
	drainClose(resp.Body)
	if resp.StatusCode/100 != 2 || resp.ContentLength < 0 {
		return 0
	}
	return resp.ContentLength
}

// PreloadRecursive is the depth-infinity expansion: it fills the list cache for
// the whole repo (scope "") or a single type with one GET per type, so the
// per-dir List calls the scanner makes during a deep scan hit memory instead of
// the network. Returns immediately; the listing runs in the background and List
// awaiters block until it finishes. A preload already in flight is a no-op.
// Mirrors the WebDAV Depth:infinity preload, reusing the same cache type.
func (b *ResticBackend) PreloadRecursive(ctx context.Context, scope string) error {
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

func (b *ResticBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	scope = strings.Trim(scope, "/")
	types := resticTypes
	if scope != "" {
		// A scoped preload (rescan of one type dir) lists just that type and
		// skips the root emit; an unknown scope has nothing to preload.
		if !isResticType(scope) {
			return nil
		}
		types = []string{scope}
	} else {
		emit("", b.rootEntries(ctx))
	}
	for _, t := range types {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entries, err := b.listType(ctx, t)
		if err != nil {
			Log.Add("restic", "ERR", "preload "+t+": "+err.Error())
			continue
		}
		// Emit even when empty so the type dir registers a cache hit (with no
		// children) rather than falling through to a live list later.
		emit(t, entries)
	}
	return nil
}

func (b *ResticBackend) ProbeChecksums() []string { return []string{"sha256"} }

func (b *ResticBackend) SetChecksumAlgo(_ string) {}

// Checksum returns the object's SHA-256. For data/index/keys/locks/snapshots
// the object name *is* the SHA-256 hex of its content (restic is content
// addressed), so the checksum is free — no download, and the value is fixed by
// the server's own naming. config and any non-hash name fall back to a
// download + local hash.
func (b *ResticBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	name := path.Base(strings.Trim(relPath, "/"))
	if isHexSHA256(name) {
		return name, nil
	}
	return b.computeChecksum(ctx, relPath)
}

func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (b *ResticBackend) computeChecksum(ctx context.Context, relPath string) (string, error) {
	rc, err := b.Open(ctx, relPath)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SetTimes is a no-op: restic objects are immutable and content addressed, with
// no settable modification time. The copy path ignores the (nil) result.
func (b *ResticBackend) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}

// CopyFrom uploads an object with a single POST. restic objects are immutable
// and addressed by content hash, so a partial upload can't be appended to — the
// copy path falls back here to a full PUT when resume isn't possible. We send
// an explicit Content-Length (from the copy context) so the server gets the
// size up front and avoids chunked encoding.
func (b *ResticBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	req, err := b.newReq(ctx, "POST", b.objectURL(relPath), src)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if sz := fileSizeFromContext(ctx); sz > 0 {
		req.ContentLength = sz
	}
	resp, err := b.doReq(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 { // restic POST succeeds with 200, not 201
		err := fmt.Errorf("restic: POST %s: %s", relPath, resp.Status)
		Log.Add("restic", "ERR", err.Error())
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// Mkdir is a no-op: the five type dirs are the only directories and they are
// implicit — objects POST straight into them, so there is nothing to create.
func (b *ResticBackend) Mkdir(context.Context, string, os.FileMode) error {
	return nil
}

// Rename is unsupported: the restic REST API has no MOVE verb, and renaming a
// content-addressed object would break the name == hash(content) invariant.
func (b *ResticBackend) Rename(context.Context, string, string) error {
	return fmt.Errorf("restic: rename not supported")
}

func (b *ResticBackend) Remove(ctx context.Context, relPath string) error {
	return b.delete(ctx, relPath)
}

// RemoveAll is the same single DELETE as Remove: restic objects are leaves with
// no subtree to recurse into.
func (b *ResticBackend) RemoveAll(ctx context.Context, relPath string) error {
	return b.delete(ctx, relPath)
}

func (b *ResticBackend) delete(ctx context.Context, relPath string) error {
	resp, err := b.do(ctx, "DELETE", b.objectURL(relPath), nil, nil)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		err := fmt.Errorf("restic: DELETE %s: %s", relPath, resp.Status)
		Log.Add("restic", "ERR", err.Error())
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *ResticBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	resp, err := b.do(ctx, "GET", b.objectURL(relPath), nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		drainClose(resp.Body)
		return nil, fmt.Errorf("restic: GET %s: %s", relPath, resp.Status)
	}
	return resp.Body, nil
}

// OpenAt resumes a download from offset via a Range request — this is what makes
// restic-as-source resumable. A server that ignores Range (200 instead of 206)
// is handled by discarding the prefix.
func (b *ResticBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	if offset <= 0 {
		return b.Open(ctx, relPath)
	}
	resp, err := b.do(ctx, "GET", b.objectURL(relPath), nil, map[string]string{
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
		return nil, fmt.Errorf("restic: GET %s @%d: %s", relPath, offset, resp.Status)
	}
}
