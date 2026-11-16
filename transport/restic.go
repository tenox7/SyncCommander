package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"slices"
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

var resticIdle = idleSetting{def: 5 * time.Minute}

func SetResticIdleTimeout(d time.Duration) { resticIdle.set(d) }

// resticDeletes is -restic-delete. A repository is a backup: a pack deleted
// while a snapshot needs it is data lost, and a mirror deletes whatever its
// source does not list. Off, no delete reaches the server (W-C3).
var resticDeletes atomic.Bool

// SetResticDeletes lets restic:// delete objects.
func SetResticDeletes(on bool) { resticDeletes.Store(on) }

func resticDeleteAllowed(rel string) error {
	if resticDeletes.Load() {
		return nil
	}
	return fmt.Errorf("restic: %s: deletes are off, start sc with -restic-delete to allow them: %w", strings.Trim(rel, "/"), fs.ErrPermission)
}

// restic keeps a data object at data/<first two characters of its
// name>/<name>, in its own repositories (local, sftp, rest-server) and in
// rclone's, while the REST API names it data/<name>. The tree shows the
// layout on disk, so a repository folder and the same repository over
// restic:// list one name per object. When the tree showed data/<name>, a
// mirror from a repository folder wrote each pack onto itself as
// data/<aa>/<name>, then deleted it as data/<name>: every pack (W-C3, D10).
// Each object has one path; any other path names no object, so a write
// there is refused and cannot reach an object under a second name.

// resticShard is the folder of a data object: the first two characters of
// its name when both are ASCII letters or digits, as restic's hex names are.
func resticShard(name string) (string, bool) {
	if len(name) < 2 || !isASCIIAlnum(name[0]) || !isASCIIAlnum(name[1]) {
		return "", false
	}
	return name[:2], true
}

func isASCIIAlnum(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z'
}

// validResticName: an object name is one path element.
func validResticName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/")
}

// resticPath is the tree path of object name of type typ.
func resticPath(typ, name string) string {
	if shard, ok := resticShard(name); ok && typ == "data" {
		return "data/" + shard + "/" + name
	}
	return typ + "/" + name
}

// resticObject is the REST path of the object whose tree path is rel, and
// whether rel is the one path of an object.
func resticObject(rel string) (string, bool) {
	rel = strings.Trim(rel, "/")
	if rel == "config" {
		return rel, true
	}
	typ, name, ok := strings.Cut(rel, "/")
	if !ok || !isResticType(typ) {
		return "", false
	}
	if typ == "data" {
		shard, rest, sharded := strings.Cut(name, "/")
		if s, ok := resticShard(rest); sharded && (!ok || s != shard) {
			return "", false
		}
		if _, ok := resticShard(name); !sharded && ok {
			return "", false // data/<name> of a name that has a shard folder
		}
		if sharded {
			name = rest
		}
	}
	if !validResticName(name) {
		return "", false
	}
	return typ + "/" + name, true
}

// isResticShard reports whether rel is a shard folder of data.
func isResticShard(rel string) bool {
	shard, ok := strings.CutPrefix(strings.Trim(rel, "/"), "data/")
	return ok && len(shard) == 2 && isASCIIAlnum(shard[0]) && isASCIIAlnum(shard[1])
}

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

// NewResticBackend connects to rawURL and checks it speaks the restic REST
// API; a cancel of ctx stops the check.
func NewResticBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*ResticBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	hb, err := newHTTPBase("restic", scheme == "restics", host, port, remotePath, user, pass, insecure, parallel, resticIdle.get())
	if err != nil {
		return nil, err
	}
	hb.accept = resticAPIV2
	b := &ResticBackend{httpBase: hb, display: hb.displayURL(scheme), listCache: newListCache()}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
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

// objectURL addresses one object by its REST path: config, data/<name>,
// keys/<name> and so on, as resticObject gives it for a tree path.
func (b *ResticBackend) objectURL(obj string) string { return b.urlFor(obj, false) }

// object is the REST path of the object at tree path rel, or an error that
// says nothing is there.
func (b *ResticBackend) object(rel string) (string, error) {
	if obj, ok := resticObject(rel); ok {
		return obj, nil
	}
	return "", fmt.Errorf("restic: %s: not where a repository keeps an object: %w", strings.Trim(rel, "/"), fs.ErrNotExist)
}

// typeURL is the listing URL for a type dir. The trailing slash is mandatory:
// restic REST routes a slash-terminated GET to a listing and a bare path to a
// single-object fetch.
func (b *ResticBackend) typeURL(typeName string) string { return b.urlFor(typeName, true) }

func isResticType(name string) bool { return slices.Contains(resticTypes, name) }

func (b *ResticBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.liveList)
}

// liveList resolves a directory directly. The repo root is synthesized (no
// list call); data and its shard folders come from one listing of data; a
// type dir is fetched; anything else is a leaf with no children.
func (b *ResticBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	rel := strings.Trim(relDir, "/")
	switch {
	case rel == "":
		return b.rootEntries(ctx), nil
	case rel == "data" || isResticShard(rel):
		return b.listData(ctx, rel)
	case isResticType(rel):
		return b.listType(ctx, rel)
	}
	return nil, nil
}

// listData lists data and keeps each of its folders in the cache: the REST
// API lists a whole type at once, so opening the shard folders one by one
// would fetch all of data once per shard.
func (b *ResticBackend) listData(ctx context.Context, rel string) ([]model.FileEntry, error) {
	objs, err := b.listType(ctx, "data")
	if err != nil {
		return nil, err
	}
	dirs := dataFolders(objs)
	for dir, entries := range dirs {
		b.listCache.fill(dir, entries)
	}
	return dirs[rel], nil
}

// dataFolders sorts the objects of data into its folders: data holds the
// shard folders and any object whose name has none, data/<aa> the objects
// of shard aa. It reorders objs, and the folders share its memory.
func dataFolders(objs []model.FileEntry) map[string][]model.FileEntry {
	slices.SortFunc(objs, func(a, b model.FileEntry) int { return strings.Compare(a.RelPath, b.RelPath) })
	dirs := map[string][]model.FileEntry{"data": nil}
	for i := 0; i < len(objs); {
		dir := path.Dir(objs[i].RelPath)
		j := i + 1
		for j < len(objs) && path.Dir(objs[j].RelPath) == dir {
			j++
		}
		if dir == "data" {
			dirs[dir] = append(dirs[dir], objs[i:j]...)
		} else {
			dirs["data"] = append(dirs["data"], model.FileEntry{RelPath: dir, Name: path.Base(dir), IsDir: true, Mode: os.ModeDir | 0755})
			dirs[dir] = objs[i:j:j] // capped: an append never writes over the next folder
		}
		i = j
	}
	return dirs
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
		return nil, fmt.Errorf("restic: list %s: %w", typeName, err)
	}
	var entries []model.FileEntry
	for dec.More() {
		var it struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
		}
		if err := dec.Decode(&it); err != nil {
			return nil, fmt.Errorf("restic: list %s: %w", typeName, err)
		}
		if validResticName(it.Name) {
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
		return nil, fmt.Errorf("restic: list %s (v1): %w", typeName, err)
	}
	Log.Add("restic", DirIn, fmt.Sprintf("v1 %s/: %d objects, HEAD per object for sizes", typeName, len(names)))
	entries := make([]model.FileEntry, 0, len(names))
	for _, name := range names {
		if validResticName(name) {
			entries = append(entries, objectEntry(typeName, name, b.statSize(resp.Request.Context(), typeName+"/"+name)))
		}
	}
	return entries, nil
}

func objectEntry(typeName, name string, size int64) model.FileEntry {
	return model.FileEntry{RelPath: resticPath(typeName, name), Name: name, Size: size, Mode: 0644}
}

// Stat answers relPath alone: the fixed top level for what it is, config or
// an object from one HEAD, never from the listing cache. A row has what a
// listing gives: an object has no time, config has its Last-Modified.
func (b *ResticBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	rel := strings.Trim(relPath, "/")
	switch {
	case rel == "":
		return model.FileEntry{IsDir: true, Mode: os.ModeDir | 0755}, nil
	case isResticType(rel) || isResticShard(rel):
		return model.FileEntry{RelPath: rel, Name: path.Base(rel), IsDir: true, Mode: os.ModeDir | 0755}, nil
	}
	obj, err := b.object(rel)
	if err != nil {
		return model.FileEntry{}, err
	}
	resp, err := b.do(ctx, "HEAD", b.objectURL(obj), nil, nil)
	if err != nil {
		return model.FileEntry{}, err
	}
	drainClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return model.FileEntry{}, fmt.Errorf("restic: %s: %w", rel, fs.ErrNotExist)
	case resp.StatusCode/100 != 2:
		return model.FileEntry{}, fmt.Errorf("restic: HEAD %s: %s", rel, resp.Status)
	}
	if rel == "config" {
		return model.FileEntry{RelPath: rel, Name: rel, Size: max(resp.ContentLength, 0), ModTime: parseWDTime(resp.Header.Get("Last-Modified")), Mode: 0644}, nil
	}
	typ, name, _ := strings.Cut(obj, "/")
	return objectEntry(typ, name, max(resp.ContentLength, 0)), nil
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

func (b *ResticBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
	scope = strings.Trim(scope, "/")
	types := resticTypes
	switch {
	case scope == "":
		emit("", b.rootEntries(ctx))
	case isResticType(scope):
		// A scoped preload (rescan of one type dir) lists just that type
		// and skips the root emit.
		types = []string{scope}
	case isResticShard(scope):
		types = []string{"data"}
	default:
		return nil // an unknown scope has nothing to preload
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
		dirs := map[string][]model.FileEntry{t: entries}
		if t == "data" {
			dirs = dataFolders(entries)
		}
		for dir, list := range dirs {
			if scope != "" && dir != scope && !strings.HasPrefix(dir, scope+"/") {
				continue
			}
			// Emit even when empty so the folder registers a cache hit
			// (with no children) rather than falling through to a live
			// list later.
			emit(dir, list)
			complete(dir)
		}
	}
	return nil
}

func (b *ResticBackend) ChecksumAlgos(context.Context) ([]string, error) {
	return []string{"sha256"}, nil
}

// Checksum returns the object's SHA-256. For data/index/keys/locks/snapshots
// the object name *is* the SHA-256 hex of its content (restic is content
// addressed), so the checksum is free: no download, and the value is fixed by
// the server's own naming. config and any non-hash name fall back to a
// download + local hash.
func (b *ResticBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	if algo != "sha256" {
		return "", fmt.Errorf("restic: no %q checksum, only sha256", algo)
	}
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
// explicit Content-Length (the size the copy was given) avoids chunked
// encoding.
//
// A path that is not where the repository keeps an object is refused: the
// server would store it under another name, or under one an object has.
func (b *ResticBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	obj, ok := resticObject(relPath)
	if !ok {
		return fmt.Errorf("restic: %s: not where a repository keeps an object (config, data/<aa>/<name>, index/<name>, keys/<name>, locks/<name>, snapshots/<name>): %w", strings.Trim(relPath, "/"), ErrUnsupported)
	}
	req, err := b.newReq(ctx, "POST", b.objectURL(obj), src)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if p.SizeKnown() {
		req.ContentLength = p.Size
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

// Remove deletes one object, when -restic-delete allows it.
func (b *ResticBackend) Remove(ctx context.Context, relPath string) error {
	obj, err := b.object(relPath)
	if err != nil {
		return err
	}
	if err := resticDeleteAllowed(relPath); err != nil {
		return err
	}
	return b.delete(ctx, obj, relPath)
}

// RemoveAll deletes the objects in a folder, or one object. The folders are
// the layout's and stay; the repository itself is never removed. A folder
// was sent as one DELETE of its path, which deleted nothing and succeeded.
func (b *ResticBackend) RemoveAll(ctx context.Context, relPath string) error {
	rel := strings.Trim(relPath, "/")
	switch {
	case rel == "" || rel == ".":
		return fmt.Errorf("restic: refusing to remove the repository: %w", fs.ErrPermission)
	case isResticType(rel) || isResticShard(rel):
		return b.removeFolder(ctx, rel)
	}
	return b.Remove(ctx, rel)
}

// removeFolder deletes each object in folder rel, from a live listing of
// its type, and stops at the first failure.
func (b *ResticBackend) removeFolder(ctx context.Context, rel string) error {
	typ, _, _ := strings.Cut(rel, "/")
	objs, err := b.listType(ctx, typ)
	if err != nil {
		return err
	}
	for _, e := range objs {
		if strings.HasPrefix(e.RelPath, rel+"/") {
			if err := b.Remove(ctx, e.RelPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ResticBackend) delete(ctx context.Context, obj, relPath string) error {
	resp, err := b.do(ctx, "DELETE", b.objectURL(obj), nil, nil)
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
	obj, err := b.object(relPath)
	if err != nil {
		return nil, err
	}
	resp, err := b.do(ctx, "GET", b.objectURL(obj), nil, hdrs)
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
