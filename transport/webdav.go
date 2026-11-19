package transport

import (
	"context"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
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
	Status   string       `xml:"status"` // per-member outcome of a DELETE or MOVE
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
	algosMu    sync.Mutex
	availAlgos []string // the checksums the server gives; nil until asked
	sums       *wdSumCache
	listCache  *listCache
	dirs       dirMemo  // collections known to exist, so uploads skip the MKCOL walk
	mkcols     sync.Map // in-flight MKCOLs by dir, so racing uploads send only one
	noInfinity atomic.Bool
	// The mtime a copy sets (W-M4). putMtime holds, by path, the time a
	// PUT sent in X-OC-Mtime, until SetTimes. headerSets says the server
	// takes that header: it answered "accepted" (ownCloud, Nextcloud), or
	// one Stat saw the time (rclone serve webdav takes it with no word).
	// noMtime says the server keeps no time it is sent, header or
	// PROPPATCH: one Stat after the first SetTimes found the upload's
	// time. mtimeProbe claims that Stat: 0 not yet, 1 under way, 2 done.
	putMtime   sync.Map
	headerSets atomic.Bool
	noMtime    atomic.Bool
	mtimeProbe atomic.Int32
	noSum      sync.Once // the first file the server gives no sum of the algo asked
}

// NewWebDAVBackend connects to rawURL and checks its base is a folder; a
// cancel of ctx stops the check.
func NewWebDAVBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*WebDAVBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	hb, err := newHTTPBase("webdav", scheme == "webdavs", host, port, remotePath, user, pass, insecure, parallel, webdavIdle.get())
	if err != nil {
		return nil, err
	}
	b := &WebDAVBackend{
		httpBase:  hb,
		display:   hb.displayURL(scheme),
		sums:      newWDSumCache(),
		listCache: newListCache().retryingRuns("webdav"), // each run is a request of its own
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ms, err := b.propfind(ctx, "", "0")
	if err != nil {
		return nil, err
	}
	if err := b.checkBase(ms); err != nil {
		Log.Add("webdav", DirErr, err.Error())
		return nil, err
	}
	return b, nil
}

// checkBase requires the answer about the base to name it as typed, and as
// a folder. A proxy that rewrites paths, or a server that spells the path
// in another letter case, gives hrefs that do not start with the base: no
// entry of a listing would map, and the remote side would read as empty.
func (b *WebDAVBackend) checkBase(ms *wdMultistatus) error {
	for _, r := range ms.Response {
		e, _, err := b.entryFromResponse(r)
		if err != nil {
			return fmt.Errorf("webdav: %s: %v (a proxy that rewrites paths, or another letter case?)", b.pathFor(""), err)
		}
		if e.RelPath == "" {
			if !e.IsDir {
				return fmt.Errorf("webdav: %s is a file, not a folder", b.pathFor(""))
			}
			return nil
		}
	}
	return fmt.Errorf("webdav: the server's answer about %s does not name it", b.pathFor(""))
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
		return nil, statusErr(resp, "PROPFIND %s", relPath)
	}
	var ms wdMultistatus
	if err := readMultistatus(resp.Body, func(r wdResponse) error { ms.Response = append(ms.Response, r); return nil }); err != nil {
		err = &fs.PathError{Op: "PROPFIND", Path: relPath, Err: err}
		Log.Add("webdav", DirErr, err.Error())
		return nil, err
	}
	return &ms, nil
}

// readMultistatus calls each for every response of a PROPFIND answer. The
// answer must be one multistatus element with nothing after it but white
// space: golang.org/x/net/webdav, which rclone serve webdav runs, closes the
// multistatus when its walk fails part way and then writes "Internal Server
// Error" after it. That is a short listing in a well-formed document.
func readMultistatus(body io.Reader, each func(wdResponse) error) error {
	dec := xml.NewDecoder(body)
	root, err := wdRoot(dec)
	if err != nil {
		return err
	}
	if root.Name.Local != "multistatus" {
		return fmt.Errorf("the answer is <%s>, not a multistatus", root.Name.Local)
	}
	for {
		tok, err := dec.Token() // the end of the body inside the root is a syntax error
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local != "response" {
				if err := dec.Skip(); err != nil {
					return fmt.Errorf("decode: %w", err)
				}
				continue
			}
			var r wdResponse
			if err := dec.DecodeElement(&r, &t); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if err := each(r); err != nil {
				return err
			}
		case xml.EndElement:
			return wdTrailer(dec)
		}
	}
}

// wdRoot returns the root element, past the XML declaration, comments and
// white space.
func wdRoot(dec *xml.Decoder) (xml.StartElement, error) {
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return xml.StartElement{}, fmt.Errorf("the answer is empty, not a multistatus")
		}
		if err != nil {
			return xml.StartElement{}, fmt.Errorf("decode: %w", err)
		}
		if se, ok := tok.(xml.StartElement); ok {
			return se, nil
		}
	}
}

// wdTrailer fails unless only white space, comments and processing
// instructions follow the root element.
func wdTrailer(dec *xml.Decoder) error {
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("after the multistatus: %w", err)
		}
		switch t := tok.(type) {
		case xml.Comment, xml.ProcInst:
		case xml.CharData:
			if text := strings.TrimSpace(string(t)); text != "" {
				return fmt.Errorf("the server wrote %q after the multistatus: its listing was cut short", text[:min(len(text), 80)])
			}
		default:
			return fmt.Errorf("the server wrote more XML after the multistatus: its listing was cut short")
		}
	}
}

// hrefToRel maps a response's href to a path under the base. An href is an
// absolute URL or an absolute path, percent-encoded. A raw "?" or "#" is
// part of the name, as a server that does not encode them means it; so is
// a "%" that does not start an escape. An href outside the base is an
// error: dropping it would hide the entry.
func (b *WebDAVBackend) hrefToRel(href string) (string, error) {
	p := strings.TrimSpace(href)
	if i := strings.Index(p, "://"); i >= 0 && !strings.Contains(p[:i], "/") {
		rest := p[i+3:]
		j := strings.IndexByte(rest, '/')
		if j < 0 {
			j = len(rest)
			rest += "/"
		}
		p = rest[j:]
	}
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("the server named %q, not an absolute path", href)
	}
	if u, err := url.PathUnescape(p); err == nil {
		p = u
	}
	p = path.Clean(p)
	base := path.Clean("/" + b.base)
	switch {
	case base == "/":
	case p == base:
		p = "/"
	case strings.HasPrefix(p, base+"/"):
		p = p[len(base):]
	default:
		return "", fmt.Errorf("the server named %q, which is not under %s", href, base)
	}
	return strings.Trim(p, "/"), nil
}

// entryFromResponse converts one multistatus response; callers drop the
// listed collection itself by comparing RelPath with the directory asked
// for. A response it cannot convert is an error, never skipped: a listing
// without it would show the entry as missing.
func (b *WebDAVBackend) entryFromResponse(r wdResponse) (model.FileEntry, map[string]string, error) {
	rel, err := b.hrefToRel(r.Href)
	if err != nil {
		return model.FileEntry{}, nil, err
	}
	prop, ok := okProp(r.Propstat)
	if !ok {
		why := strings.TrimSpace(r.Status)
		for _, p := range r.Propstat {
			why = strings.TrimSpace(p.Status)
		}
		return model.FileEntry{}, nil, fmt.Errorf("the server gave no properties for %q (%s)", r.Href, why)
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
		TimeRes: model.TimeSecond, // an HTTP date
		IsDir:   isDir,
		Mode:    mode,
	}
	if b.noMtime.Load() {
		e.TimeRes = model.TimeNone // the upload's time, not the file's
	}
	return e, parseChecksums(prop.Checksums), nil
}

// okProp picks the successful propstat of a PROPFIND response; a propstat
// without a status counts as successful (statusOK). A lone 403 or 404
// propstat is not: it would make a 0-byte file of whatever the entry is.
func okProp(ps []wdPropstat) (wdProp, bool) {
	for _, p := range ps {
		if statusOK(p.Status) {
			return p.Prop, true
		}
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
	entries, err := b.listCache.serve(ctx, relDir, b.liveList)
	if b.noMtime.Load() {
		entries = withoutMtime(entries) // also those the cache holds from before
	}
	return entries, err
}

// withoutMtime is entries with their mtime left out of the compare. It
// copies them when one is not so already: the cache's slices are shared.
func withoutMtime(entries []model.FileEntry) []model.FileEntry {
	for i := range entries {
		if entries[i].TimeRes != model.TimeNone {
			out := slices.Clone(entries)
			for j := range out {
				out[j].TimeRes = model.TimeNone
			}
			return out
		}
	}
	return entries
}

func (b *WebDAVBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	ms, err := b.propfind(ctx, relDir, defaultWDDepth)
	if err != nil {
		return nil, err
	}
	self := strings.Trim(relDir, "/")
	var entries []model.FileEntry
	sawSelf := false
	for _, r := range ms.Response {
		e, sums, err := b.entryFromResponse(r)
		switch {
		case err != nil:
		case e.RelPath == self && !e.IsDir:
			err = fmt.Errorf("%s is a file on the server, not a folder", b.pathFor(relDir))
		case e.RelPath == self:
			sawSelf = true
			continue
		case parentDir(e.RelPath) != self && !strings.HasPrefix(e.RelPath, self+"/") && self != "":
			err = fmt.Errorf("the server listed %q, which is not in %s", r.Href, b.pathFor(relDir))
		case parentDir(e.RelPath) != self:
			continue // deeper than asked; listed with its own folder
		}
		if err != nil {
			err = &fs.PathError{Op: "PROPFIND", Path: b.pathFor(relDir), Err: err}
			Log.Add("webdav", DirErr, err.Error())
			return nil, err
		}
		b.sums.put(e.RelPath, sums)
		entries = append(entries, e)
	}
	if !sawSelf {
		err := fmt.Errorf("PROPFIND %s: the answer does not include the folder itself", b.pathFor(relDir))
		Log.Add("webdav", DirErr, err.Error())
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir {
			b.knowDir(e.RelPath)
		}
	}
	Log.Add("webdav", DirIn, fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

// Stat asks for relPath alone, a PROPFIND of depth 0, never from the listing
// cache. The path goes without a slash at its end, as a file's: servers
// answer that for a folder too, and one that redirects to the slash is
// asked again there (doReq). Sums in the answer are kept, as a listing keeps
// them.
func (b *WebDAVBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	rel := strings.Trim(relPath, "/")
	Log.Add("webdav", DirOut, "STAT "+b.pathFor(rel))
	resp, err := b.do(ctx, "PROPFIND", b.urlFor(rel, rel == ""), strings.NewReader(wdPropfindBody), map[string]string{
		"Depth":        "0",
		"Content-Type": "application/xml",
	})
	if err != nil {
		return model.FileEntry{}, err
	}
	defer drainClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return model.FileEntry{}, fmt.Errorf("PROPFIND %s: %w", b.pathFor(rel), fs.ErrNotExist)
	case resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK:
		return model.FileEntry{}, statusErr(resp, "PROPFIND %s", b.pathFor(rel))
	}
	var e model.FileEntry
	found := false
	err = readMultistatus(resp.Body, func(r wdResponse) error {
		got, sums, err := b.entryFromResponse(r)
		switch {
		case err != nil:
			return err
		case got.RelPath != rel:
			return nil // depth 0 is the path itself; a server that adds more is ignored
		}
		b.sums.put(got.RelPath, sums)
		e, found = got, true
		return nil
	})
	if err == nil && !found {
		err = errors.New("the answer does not include the path itself")
	}
	if err != nil {
		err = &fs.PathError{Op: "PROPFIND", Path: b.pathFor(rel), Err: err}
		Log.Add("webdav", DirErr, err.Error())
		return model.FileEntry{}, err
	}
	if rel == "" {
		e.Name = ""
	}
	return e, nil
}

// PreloadRecursive fires a single PROPFIND Depth:infinity in the background,
// streaming entries into the cache grouped by parent dir. Servers may refuse
// Depth:infinity (RFC 4918 §9.1, 403 + propfind-finite-depth; refusesInfinity);
// on that rejection noInfinity latches so every later scan goes straight to
// per-dir Depth:1 without retrying.
func (b *WebDAVBackend) PreloadRecursive(ctx context.Context, scope string) error {
	if b.noInfinity.Load() {
		return nil
	}
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

// refusesInfinity reports an answer to PROPFIND Depth: infinity that the
// same request gets again: 403 with propfind-finite-depth (RFC 4918 §9.1),
// 400 or 501 from a server that has no such depth. A 507, from one that
// cannot build so big an answer, comes as model.ErrNoSpace. Only 403
// latched noInfinity, so every scan sent the request first again (W-M7).
func refusesInfinity(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusNotImplemented:
		return true
	}
	return false
}

func (b *WebDAVBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
	Log.Add("webdav", DirOut, "RPROPFIND "+b.pathFor(scope))
	resp, err := b.do(ctx, "PROPFIND", b.urlFor(scope, true), strings.NewReader(wdPropfindBody), map[string]string{
		"Depth":        "infinity",
		"Content-Type": "application/xml",
	})
	if errors.Is(err, model.ErrNoSpace) {
		b.noInfinity.Store(true)
		Log.Add("webdav", DirErr, "RPROPFIND rejected (507: no room for so big an answer); falling back to per-dir Depth:1")
		return fmt.Errorf("PROPFIND infinity %s: %w: %w", scope, err, ErrUnsupported)
	}
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if refusesInfinity(resp.StatusCode) {
		b.noInfinity.Store(true)
		Log.Add("webdav", DirErr, "RPROPFIND rejected ("+resp.Status+"); falling back to per-dir Depth:1")
		return fmt.Errorf("PROPFIND infinity %s: %s: %w", scope, resp.Status, ErrUnsupported)
	}
	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		err := statusErr(resp, "PROPFIND infinity %s", scope)
		Log.Add("webdav", DirErr, "RPROPFIND: "+err.Error())
		return err
	}

	// Multistatus ordering is not guaranteed grouped by parent; the grouper
	// appends, so interleaving only costs extra flushes. A 207 does not say
	// how deep the server went: sabre/dav answers Depth: infinity with one
	// level by default and Tomcat stops at depth 3. So a folder with no
	// children in the answer is listed again on its own (childlessUnknown).
	// A failed answer returns before finish: the folders still open then
	// were never completed, and the cache drops them. A cut-off answer
	// keeps the folders completed before the cut (listCache.keepAfterLocked).
	self := strings.Trim(scope, "/")
	g := &emitGrouper{emit: emit, complete: complete, childlessUnknown: true}
	n := 0
	err = readMultistatus(resp.Body, func(r wdResponse) error {
		e, sums, err := b.entryFromResponse(r)
		switch {
		case err != nil:
			return err
		case e.RelPath == self:
			return nil
		case self != "" && !strings.HasPrefix(e.RelPath, self+"/"):
			return fmt.Errorf("the server listed %q, which is not in %s", r.Href, b.pathFor(scope))
		}
		b.sums.put(e.RelPath, sums)
		if e.IsDir {
			b.knowDir(e.RelPath)
		}
		g.add(e)
		n++
		return nil
	})
	if err != nil {
		err = fmt.Errorf("RPROPFIND %s: %w; listing each folder on its own", b.pathFor(scope), err)
		Log.Add("webdav", DirWarn, err.Error())
		return err
	}
	g.finish()
	Log.Add("webdav", DirIn, fmt.Sprintf("RPROPFIND %d entries", n))
	return nil
}

// Checksum is the algo checksum the server gives for relPath (oc:checksums),
// from the listing that carried it or a PROPFIND of relPath alone. A file
// an answer gave no such sum is asked no more, and none is a final answer.
// Each such file was a PROPFIND, and its error one that a retry took for
// passing: six PROPFINDs and some 15 s of waits a file (W-M8).
func (b *WebDAVBackend) Checksum(ctx context.Context, relPath, algo string) (string, error) {
	h, ok, known := b.sums.get(relPath, algo)
	if !known {
		if err := b.statInto(ctx, relPath); err != nil {
			return "", err
		}
		h, ok, _ = b.sums.get(relPath, algo)
	}
	if !ok {
		err := noSumError{algo: algo, path: relPath}
		b.noSum.Do(func() {
			Log.Add("webdav", DirWarn, fmt.Sprintf("%v; it and other files without one show as unknown by checksum", err))
		})
		return "", err
	}
	return h, nil
}

// noSumError is a file the server gives no sum of algo. Another try gets
// none either, and it is no fault of the server's: ErrUnsupported, which
// Retry gives back at once, with no line in the log.
type noSumError struct{ algo, path string }

func (e noSumError) Error() string {
	return fmt.Sprintf("the server gives no %s checksum for %s", e.algo, e.path)
}

func (noSumError) Is(target error) bool { return target == ErrUnsupported }

func (b *WebDAVBackend) statInto(ctx context.Context, relPath string) error {
	ms, err := b.propfind(ctx, relPath, "0")
	if err != nil {
		return err
	}
	for _, r := range ms.Response {
		e, sums, err := b.entryFromResponse(r)
		if err != nil {
			return err
		}
		b.sums.put(e.RelPath, sums)
	}
	return nil
}

// ChecksumAlgos are the checksums the server gives with its files, found on
// the first ask and kept. An ask that failed keeps nothing, so the next one
// asks again: it used to keep "none" for good, and could not be stopped.
func (b *WebDAVBackend) ChecksumAlgos(ctx context.Context) ([]string, error) {
	b.algosMu.Lock()
	defer b.algosMu.Unlock()
	if b.availAlgos != nil {
		return b.availAlgos, nil
	}
	seen, err := b.probeAlgos(ctx)
	if err != nil {
		return nil, fmt.Errorf("webdav: asking for checksums: %w", err)
	}
	algos := []string{}
	for _, a := range []string{"sha256", "sha1", "md5"} {
		if seen[a] {
			algos = append(algos, a)
		}
	}
	b.availAlgos = algos
	return algos, nil
}

// probeAlgos is the checksums the server gives: those the listings so far
// carried, with no request. It listed up to four folders again, live, for
// them, after a scan that had listed them all (W-L3). Before a listing
// that carries any, it lists the first folder of each level, through the
// listing cache, down to one that does, so a tree whose top level is all
// folders still probes.
func (b *WebDAVBackend) probeAlgos(ctx context.Context) (map[string]bool, error) {
	dir := ""
	for depth := 0; ; depth++ {
		if seen := b.sums.algos(); len(seen) > 0 || depth == 4 {
			return seen, nil
		}
		entries, err := b.List(ctx, dir)
		if err != nil {
			return nil, err
		}
		i := slices.IndexFunc(entries, func(e model.FileEntry) bool { return e.IsDir })
		if i < 0 {
			return b.sums.algos(), nil
		}
		dir = entries[i].RelPath
	}
}

// SetTimes sets relPath's mtime with PROPPATCH, unless the PUT that wrote
// it sent the time in X-OC-Mtime and the server takes that header. Both
// went out for every file, and neither was checked: on a server that
// keeps neither, every file copied there was "different" after the copy
// (W-M4). The first call checks with one Stat that the time holds. A
// server that keeps none says so from then on (noMtime): its listings
// leave the mtime out of the compare, and SetTimes is ErrUnsupported, as
// for a restic repository.
func (b *WebDAVBackend) SetTimes(ctx context.Context, relPath string, mtime, _, _ time.Time) error {
	if b.noMtime.Load() {
		return errNoMtime(relPath)
	}
	v, ok := b.putMtime.LoadAndDelete(relPath)
	headerSent := ok && v.(int64) == mtime.Unix()
	if headerSent && b.headerSets.Load() {
		return nil
	}
	probe := b.mtimeProbe.CompareAndSwap(0, 1)
	if probe && headerSent {
		if kept, err := b.keepsMtime(ctx, relPath, mtime); err == nil && kept {
			b.headerSets.Store(true)
			b.mtimeProbe.Store(2)
			Log.Add("webdav", DirIn, "the server takes the mtime with the upload (X-OC-Mtime): no PROPPATCH")
			return nil
		}
	}
	err := b.proppatchMtime(ctx, relPath, mtime)
	if !probe {
		return err
	}
	kept, serr := b.keepsMtime(ctx, relPath, mtime)
	switch {
	case serr != nil:
		b.mtimeProbe.Store(0) // the next call checks
		return err
	case kept:
		b.mtimeProbe.Store(2)
		return nil
	}
	b.noMtime.Store(true)
	b.mtimeProbe.Store(2)
	Log.Add("webdav", DirWarn, "the server keeps no mtime it is sent (X-OC-Mtime, PROPPATCH): "+
		relPath+" lists the upload's time; comparing without mtimes")
	return errNoMtime(relPath)
}

func errNoMtime(relPath string) error {
	return fmt.Errorf("webdav: %s: the server keeps no mtime it is sent: %w", relPath, ErrUnsupported)
}

// keepsMtime reports whether relPath lists mtime, to the second of an HTTP
// date.
func (b *WebDAVBackend) keepsMtime(ctx context.Context, relPath string, mtime time.Time) (bool, error) {
	e, err := b.Stat(ctx, relPath)
	if err != nil {
		return false, err
	}
	d := e.ModTime.Sub(mtime)
	return d < time.Second && d > -time.Second, nil
}

// proppatchMtime sends relPath's mtime as DAV:lastmodified, in seconds.
func (b *WebDAVBackend) proppatchMtime(ctx context.Context, relPath string, mtime time.Time) error {
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
		err := statusErr(resp, "PROPPATCH %s", relPath)
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
// walk is skipped for collections already seen; a MKCOL failure is only a
// warning here because the PUT's own status is the verdict. A PUT that
// finds its folder not there makes it again (remakeParent).
func (b *WebDAVBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, p model.PutParams) error {
	if err := b.ensureDir(ctx, parentDir(relPath)); err != nil {
		Log.Add("webdav", DirWarn, "mkcol parents: "+err.Error())
	}
	body, size, err := putBody(src, p.Size)
	if err != nil {
		return err
	}
	req, err := b.newReq(ctx, "PUT", b.urlFor(relPath, false), body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if !p.ModTime.IsZero() {
		req.Header.Set("X-OC-Mtime", strconv.FormatInt(p.ModTime.Unix(), 10))
	}
	resp, err := b.doReq(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		err := statusErr(resp, "PUT %s", relPath)
		if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusNotFound {
			err = b.remakeParent(ctx, relPath, resp.Status, err)
		}
		Log.Add("webdav", DirErr, err.Error())
		return err
	}
	if !p.ModTime.IsZero() {
		b.putMtime.Store(relPath, p.ModTime.Unix())
		if resp.Header.Get("X-OC-Mtime") == "accepted" {
			b.headerSets.Store(true)
		}
	}
	b.dirs.know(parentDir(relPath))
	b.sums.invalidate(relPath)
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// errFolderRemade is a PUT that found its folder not there, though sc took
// it for there, and the folder made again: the copy is worth another try,
// with its source read again (W-M5).
var errFolderRemade = errors.New("its folder was not there; sc made it again")

// remakeParent answers a PUT of relPath that failed with err, a 409 or a
// 404 (status): the folder it goes into is not there. sc took it for
// there: a MKCOL answered 405 or 409, as x/net/webdav answers any Mkdir
// that failed, or it was there and is gone. The memo kept it for the
// session, and every file into that folder failed (W-M5). Now the folder
// and those above it are forgotten and made again, once. There now, the
// copy is tried again; still not there, the PUT's answer stands.
func (b *WebDAVBackend) remakeParent(ctx context.Context, relPath, status string, err error) error {
	dir := parentDir(strings.Trim(relPath, "/"))
	if dir == "" {
		return err
	}
	for d := dir; d != ""; d = parentDir(d) {
		b.dirs.forget(d)
	}
	if merr := b.ensureDir(ctx, dir); merr != nil {
		return &fs.PathError{Op: "PUT", Path: relPath, Err: fmt.Errorf("%s, and its folder could not be made: %w", status, merr)}
	}
	if e, serr := b.Stat(ctx, dir); serr != nil || !e.IsDir {
		return fmt.Errorf("%w: the server took the folder %s for made, and it is not there", err, dir)
	}
	return &fs.PathError{Op: "PUT", Path: relPath, Err: fmt.Errorf("%s: %w", status, errFolderRemade)}
}

// forgetDirs drops rel and the folders under it from the memo of folders
// that are there: deleted or moved. Any delete or rename emptied the memo,
// and every folder then cost a MKCOL again (W-M5). It looked at every
// folder known, each a listing showed, on each delete of a file too: now
// it walks those under rel alone (W-M8).
func (b *WebDAVBackend) forgetDirs(rel string) {
	b.dirs.forgetTree(strings.Trim(rel, "/"))
}

// knowDir puts rel into the memo of folders that are there: a listing
// showed it. Only the folders sc made were in it, and each folder a
// listing showed cost a MKCOL (W-M5).
func (b *WebDAVBackend) knowDir(rel string) {
	if rel = strings.Trim(rel, "/"); rel != "" {
		b.dirs.know(rel)
	}
}

// knowsDir reports whether rel is in the memo of folders that are there.
func (b *WebDAVBackend) knowsDir(rel string) bool {
	return b.dirs.knows(strings.Trim(rel, "/"))
}

func (b *WebDAVBackend) Mkdir(ctx context.Context, relPath string, _ os.FileMode) error {
	if err := b.ensureDir(ctx, relPath); err != nil {
		return err
	}
	b.listCache.invalidateAncestors(relPath)
	return nil
}

// mkcol is one in-flight MKCOL; waiters read err once done is closed.
type mkcol struct {
	done chan struct{}
	err  error
}

// ensureDir MKCOLs dir and any missing parents, remembering what exists.
// Uploads racing into one new directory share a single MKCOL, because the Go
// webdav server behind rclone answers concurrent MKCOLs of one path with 423
// Locked. 405 means the collection is already there; 409 is tolerated
// because some servers answer it for existing collections too. Both are
// also what a server may answer for a folder it could not make: a PUT
// that then finds the folder not there makes it again (remakeParent).
func (b *WebDAVBackend) ensureDir(ctx context.Context, dir string) error {
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	if b.knowsDir(dir) {
		return nil
	}
	if err := b.ensureDir(ctx, parentDir(dir)); err != nil {
		return err
	}
	m := &mkcol{done: make(chan struct{})}
	if cur, loaded := b.mkcols.LoadOrStore(dir, m); loaded {
		m = cur.(*mkcol)
		select {
		case <-m.done:
			return m.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.err = b.mkcolOnce(ctx, dir)
	b.mkcols.Delete(dir)
	close(m.done)
	return m.err
}

func (b *WebDAVBackend) mkcolOnce(ctx context.Context, dir string) error {
	resp, err := b.do(ctx, "MKCOL", b.urlFor(dir, true), nil, nil)
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	switch code := resp.StatusCode; {
	case code/100 == 2, code == http.StatusMethodNotAllowed, code == http.StatusConflict:
		b.dirs.know(dir)
		return nil
	default:
		return statusErr(resp, "MKCOL %s", dir)
	}
}

func (b *WebDAVBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	if err := renameBase("webdav", oldRelPath, newRelPath); err != nil {
		return err
	}
	if err := b.ensureDir(ctx, parentDir(newRelPath)); err != nil {
		Log.Add("webdav", DirWarn, "mkcol parents: "+err.Error())
	}
	resp, err := b.do(ctx, "MOVE", b.urlFor(oldRelPath, false), nil, map[string]string{
		"Destination": b.urlFor(newRelPath, false),
		// T would make the server delete an existing target first, a
		// folder with all it holds.
		"Overwrite": "F",
	})
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode == http.StatusPreconditionFailed {
		return renameTargetTaken("webdav", newRelPath)
	}
	if resp.StatusCode/100 != 2 {
		err := statusErr(resp, "MOVE %s", oldRelPath)
		Log.Add("webdav", DirErr, err.Error())
		return err
	}
	b.forgetDirs(oldRelPath)
	b.forgetDirs(newRelPath)
	b.sums.invalidate(oldRelPath)
	b.listCache.invalidateTree(oldRelPath)
	b.listCache.invalidateAncestors(oldRelPath)
	b.listCache.invalidateAncestors(newRelPath)
	return nil
}

// Remove removes the file relPath, or a folder with nothing in it, as
// os.Remove does. A DELETE takes a collection with all it holds (RFC 4918
// §9.6.1), and sc removes with Remove what it saw as a file: one that is a
// folder on the server now went with its whole subtree, where the other
// backends refuse (W-L3). So the path is looked at first, live, at one
// PROPFIND more per file, and a folder that holds anything is refused, as
// rmdir refuses it. One gone already goes to the DELETE all the same: its
// 404 is no failure, and the listings forget the path.
func (b *WebDAVBackend) Remove(ctx context.Context, relPath string) error {
	e, err := b.Stat(ctx, relPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return err
	case e.IsDir:
		entries, err := b.liveList(ctx, relPath)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			err := &fs.PathError{Op: "DELETE", Path: b.pathFor(relPath), Err: errFolderNotEmpty}
			Log.Add("webdav", DirErr, err.Error())
			return err
		}
	}
	return b.delete(ctx, relPath, false)
}

// errFolderNotEmpty refuses a Remove of a folder that holds something. It
// is an fs.ErrExist, as ENOTEMPTY is, so it is final.
var errFolderNotEmpty error = notEmptyError{}

type notEmptyError struct{}

func (notEmptyError) Error() string        { return "it is a folder on the server, and not empty" }
func (notEmptyError) Is(target error) bool { return target == fs.ErrExist }

func (b *WebDAVBackend) RemoveAll(ctx context.Context, relPath string) error {
	return b.delete(ctx, relPath, true)
}

// delete is one DELETE. A collection goes with its whole subtree (RFC 4918
// §9.6.1), and a 207 answer means some member survived, so it counts as a
// failure despite its 2xx class. A recursive delete is checked afterwards: a
// server that only unlinked the collection entry, or quietly refused, would
// otherwise report success while the tree still holds the data.
func (b *WebDAVBackend) delete(ctx context.Context, relPath string, recursive bool) error {
	if isBaseRel(relPath) {
		return fmt.Errorf("webdav: refusing to remove the base collection")
	}
	var hdrs map[string]string
	if recursive {
		hdrs = map[string]string{"Depth": "infinity"}
	}
	resp, err := b.do(ctx, "DELETE", b.urlFor(relPath, false), nil, hdrs)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusMultiStatus:
		err = fmt.Errorf("DELETE %s: %s", relPath, multistatusFailures(resp.Body))
	case resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound:
		err = statusErr(resp, "DELETE %s", relPath)
	case recursive:
		err = b.gone(ctx, relPath)
	}
	b.forgetDirs(relPath)
	b.sums.invalidate(relPath)
	b.listCache.invalidateTree(relPath)
	b.listCache.invalidateAncestors(relPath)
	if err != nil {
		Log.Add("webdav", DirErr, err.Error())
	}
	return err
}

// multistatusFailures summarises the members a 207 reply could not delete.
func multistatusFailures(body io.Reader) string {
	var ms wdMultistatus
	if err := xml.NewDecoder(body).Decode(&ms); err != nil {
		return "207 Multi-Status"
	}
	var failed []string
	for _, r := range ms.Response {
		if r.Status == "" || !statusOK(r.Status) {
			failed = append(failed, strings.TrimSpace(r.Href+" "+r.Status))
		}
	}
	if len(failed) == 0 {
		return "207 Multi-Status"
	}
	s := fmt.Sprintf("%d member(s) not deleted: %s", len(failed), failed[0])
	if len(failed) > 1 {
		s += ", ..."
	}
	return s
}

// gone confirms relPath no longer answers a PROPFIND after a DELETE.
func (b *WebDAVBackend) gone(ctx context.Context, relPath string) error {
	resp, err := b.do(ctx, "PROPFIND", b.urlFor(relPath, false), strings.NewReader(wdPropfindBody), map[string]string{
		"Depth":        "0",
		"Content-Type": "application/xml",
	})
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		return nil
	case http.StatusMultiStatus, http.StatusOK:
		return fmt.Errorf("DELETE %s: still exists afterwards", relPath)
	}
	return statusErr(resp, "DELETE %s: verify", relPath)
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
	return rangeBody(ctx, resp, fmt.Sprintf("GET %s @%d", relPath, offset), offset)
}

// wdSums is the sums one answer gave for a file, as bytes, and which there
// are (have). A map of strings for each file took some 370 bytes of it,
// 370 MB for 1M files (W-M8).
type wdSums struct {
	have   uint8
	md5    [16]byte
	sha1   [20]byte
	sha256 [32]byte
}

// wdSumBits are the sums sc reads, each with its bit in wdSums.have.
var wdSumBits = map[string]uint8{"md5": 1, "sha1": 2, "sha256": 4}

func (s *wdSums) field(algo string) []byte {
	switch algo {
	case "md5":
		return s.md5[:]
	case "sha1":
		return s.sha1[:]
	case "sha256":
		return s.sha256[:]
	}
	return nil
}

// wdSumsOf is sums as bytes. A sum that is no hex, or not as long as its
// algo's, is none.
func wdSumsOf(sums map[string]string) wdSums {
	var s wdSums
	for algo, h := range sums {
		f := s.field(algo)
		if f == nil || len(h) != 2*len(f) {
			continue
		}
		if _, err := hex.Decode(f, []byte(h)); err == nil {
			s.have |= wdSumBits[algo]
		}
	}
	return s
}

func (s *wdSums) sum(algo string) (string, bool) {
	if s.have&wdSumBits[algo] == 0 {
		return "", false
	}
	return hex.EncodeToString(s.field(algo)), true
}

// wdSumCache is the sums the latest answer about each file gave, and the
// files it gave none.
type wdSumCache struct {
	mu   sync.Mutex
	m    map[string]wdSums
	none map[string]struct{}
	seen uint8 // each sum an answer gave, for any file
}

func newWDSumCache() *wdSumCache {
	return &wdSumCache{m: make(map[string]wdSums), none: make(map[string]struct{})}
}

// put records the sums an answer gave for relPath. They replace what an
// older answer said, and an answer with none says the file has none. They
// were added to it, and one with none was ignored: after another client
// rewrote a file of the same size, Checksum gave the old sum, with no
// request, and the compare said "same" (W-M3).
func (c *wdSumCache) put(relPath string, sums map[string]string) {
	s := wdSumsOf(sums)
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.have == 0 {
		delete(c.m, relPath)
		c.none[relPath] = struct{}{}
		return
	}
	delete(c.none, relPath)
	c.m[relPath] = s
	c.seen |= s.have
}

// get is relPath's algo sum, as the latest answer about it gave it: ok
// says there is one. known says there was an answer: with none, a file
// is asked no more.
func (c *wdSumCache) get(relPath, algo string) (h string, ok, known bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, found := c.m[relPath]; found {
		h, ok = s.sum(algo)
		return h, ok, true
	}
	_, known = c.none[relPath]
	return "", false, known
}

// algos is the checksums the answers so far carried, for any file.
func (c *wdSumCache) algos() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	for a, bit := range wdSumBits {
		if c.seen&bit != 0 {
			seen[a] = true
		}
	}
	return seen
}

func (c *wdSumCache) invalidate(relPath string) {
	c.mu.Lock()
	delete(c.m, relPath)
	delete(c.none, relPath)
	c.mu.Unlock()
}
