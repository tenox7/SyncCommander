package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
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

var resticIdle = idleSetting{def: 5 * time.Minute}

func SetResticIdleTimeout(d time.Duration) { resticIdle.set(d) }

// ResticBackend speaks the restic REST API (restic://, restics://), the
// protocol implemented by `rclone serve restic` and the restic rest-server. The
// repo is a content-addressed object store with a fixed shallow shape: a config
// file plus five type dirs (data/index/keys/locks/snapshots) of objects whose
// names are the SHA-256 of their content. We present that as a two-level tree.
type ResticBackend struct {
	*httpBase
	display   string
	listCache *listCache
}

func NewResticBackend(rawURL string, insecure bool, parallel int) (*ResticBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	hb, err := newHTTPBase("restic", scheme == "restics", host, port, remotePath, user, pass, insecure, parallel, resticIdle.get())
	if err != nil {
		return nil, err
	}
	hb.accept = resticAPIV2
	b := &ResticBackend{httpBase: hb, display: hb.displayURL(scheme), listCache: newListCache()}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.validate(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *ResticBackend) BasePath() string { return b.display }

// validate confirms the endpoint speaks the restic REST API before we commit
// to it. An existing config means a live repo; a freshly created repo has none
// yet, so a 2xx listing of the data type is accepted instead. A bare 404 is
// not: any web server answers that.
func (b *ResticBackend) validate(ctx context.Context) error {
	resp, err := b.do(ctx, "HEAD", b.urlFor("config", false), nil, nil)
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	if resp.StatusCode/100 == 2 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		lresp, err := b.do(ctx, "GET", b.urlFor("data", true), nil, nil)
		if err != nil {
			return err
		}
		drainClose(lresp.Body)
		if lresp.StatusCode/100 == 2 {
			return nil
		}
	}
	return fmt.Errorf("restic: %s: %s", b.display, resp.Status)
}

// objectURL addresses a single object (or config) by its flat path. Data
// objects use the flat name here (data/<hash>) even though the server shards
// them on disk as data/<aa>/<hash>; the sharding is server-side only.
func (b *ResticBackend) objectURL(relPath string) string { return b.urlFor(relPath, false) }

// typeURL is the listing URL for a type dir. The trailing slash is mandatory:
// restic REST routes a slash-terminated GET to a listing and a bare path to a
// single-object fetch.
func (b *ResticBackend) typeURL(typeName string) string { return b.urlFor(typeName, true) }

func isResticType(name string) bool { return slices.Contains(resticTypes, name) }

func (b *ResticBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.liveList)
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

// rootEntries is the repo's fixed top level: the five type dirs plus config
// when present. It needs no listing call, only a cheap HEAD to size config.
func (b *ResticBackend) rootEntries(ctx context.Context) []model.FileEntry {
	entries := make([]model.FileEntry, 0, len(resticTypes)+1)
	for _, t := range resticTypes {
		entries = append(entries, model.FileEntry{RelPath: t, Name: t, IsDir: true, Mode: os.ModeDir | 0755})
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
	return model.FileEntry{
		RelPath: "config",
		Name:    "config",
		Size:    max(resp.ContentLength, 0),
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
		if it.Name != "" {
			entries = append(entries, objectEntry(typeName, it.Name, it.Size))
		}
	}
	Log.Add("restic", DirIn, fmt.Sprintf("%s/: %d objects", typeName, len(entries)))
	return entries, nil
}

// decodeListV1 handles the legacy v1 format (names only). Sizes require a HEAD
// per object, which is brutal on large dirs; only a v1-only rest-server takes
// this path, rclone is always v2.
func (b *ResticBackend) decodeListV1(resp *http.Response, typeName string) ([]model.FileEntry, error) {
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, fmt.Errorf("restic: list %s (v1): %v", typeName, err)
	}
	Log.Add("restic", DirIn, fmt.Sprintf("v1 %s/: %d objects, HEAD per object for sizes", typeName, len(names)))
	entries := make([]model.FileEntry, 0, len(names))
	for _, name := range names {
		if name != "" {
			entries = append(entries, objectEntry(typeName, name, b.statSize(resp.Request.Context(), typeName+"/"+name)))
		}
	}
	return entries, nil
}

func objectEntry(typeName, name string, size int64) model.FileEntry {
	return model.FileEntry{RelPath: typeName + "/" + name, Name: name, Size: size, Mode: 0644}
}

func (b *ResticBackend) statSize(ctx context.Context, relPath string) int64 {
	resp, err := b.do(ctx, "HEAD", b.objectURL(relPath), nil, nil)
	if err != nil {
		return 0
	}
	drainClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		return 0
	}
	return max(resp.ContentLength, 0)
}

// PreloadRecursive fills the list cache for the whole repo (scope "") or a
// single type with one GET per type, so the per-dir List calls of a deep scan
// hit memory instead of the network.
func (b *ResticBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.runRecursiveList)
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
			Log.Add("restic", DirErr, "preload "+t+": "+err.Error())
			return err
		}
		// Emit even when empty so the type dir registers a cache hit (with no
		// children) rather than falling through to a live list later.
		emit(t, entries)
	}
	return nil
}

func (b *ResticBackend) ProbeChecksums() []string { return []string{"sha256"} }

func (b *ResticBackend) SetChecksumAlgo(string) {}

// Checksum returns the object's SHA-256. For data/index/keys/locks/snapshots
// the object name *is* the SHA-256 hex of its content (restic is content
// addressed), so the checksum is free: no download, and the value is fixed by
// the server's own naming. config and any non-hash name fall back to a
// download + local hash.
func (b *ResticBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	name := path.Base(strings.Trim(relPath, "/"))
	if isHexSHA256(name) {
		return name, nil
	}
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

// SetTimes is a no-op: restic objects are immutable and content addressed, with
// no settable modification time.
func (b *ResticBackend) SetTimes(context.Context, string, time.Time, time.Time, time.Time) error {
	return nil
}

// CopyFrom uploads an object with a single POST. restic objects are immutable
// and addressed by content hash, so a partial upload can't be appended to; the
// copy path falls back here to a full upload when resume isn't possible. An
// explicit Content-Length (from the copy context) avoids chunked encoding.
func (b *ResticBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, _ os.FileMode) error {
	req, err := b.newReq(ctx, "POST", b.objectURL(relPath), src)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if sz, ok := fileSizeFromContext(ctx); ok && sz >= 0 {
		req.ContentLength = sz
	}
	resp, err := b.doReq(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 { // restic POST succeeds with 200, not 201
		err := fmt.Errorf("restic: POST %s: %s", relPath, resp.Status)
		Log.Add("restic", DirErr, err.Error())
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// Mkdir is a no-op: the five type dirs are the only directories and they are
// implicit; objects POST straight into them.
func (b *ResticBackend) Mkdir(context.Context, string, os.FileMode) error { return nil }

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
		Log.Add("restic", DirErr, err.Error())
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

func (b *ResticBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(ctx, relPath, 0)
}

// OpenAt resumes a download from offset via a Range request, which is what
// makes restic-as-source resumable. A server that ignores Range (200 instead
// of 206) is handled by discarding the prefix.
func (b *ResticBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	var hdrs map[string]string
	if offset > 0 {
		hdrs = map[string]string{"Range": fmt.Sprintf("bytes=%d-", offset)}
	}
	resp, err := b.do(ctx, "GET", b.objectURL(relPath), nil, hdrs)
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
