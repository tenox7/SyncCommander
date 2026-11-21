package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"

	"sc/model"
)

var httpIdle = idleSetting{def: 5 * time.Minute}

// SetHTTPIdleTimeout sets how long an http:// or https:// connection may
// send nothing before the call on it fails (-http-timeout).
func SetHTTPIdleTimeout(d time.Duration) { httpIdle.set(d) }

// httpListConns is how many requests the listings of one http:// backend
// have in flight at once, folder pages and HEADs together. A file's size
// and time cost a HEAD each: one at a time, 100,000 files at 20 ms took
// more than half an hour. Eight is what a browser opens to one server, and
// what the idle pool keeps (newHTTPBase).
const httpListConns = 8

// httpListTries is how many more times one request of a listing is sent
// when it fails with what another try can mend. A listing of a folder of
// 50,000 files sent all its HEADs again for one that failed. A var for
// tests.
var httpListTries = 3

const (
	// acceptListing asks for a listing in JSON first: Caddy answers so a
	// request that accepts it, and a JSON listing gives exact sizes and
	// times, with no HEAD for each file. Other servers send their page.
	acceptListing = "application/json, text/html;q=0.9, */*;q=0.8"
	acceptHTML    = "text/html, */*;q=0.8"
)

// errHTTPReadOnly refuses every write: sc reads a web server's folders,
// and changes none.
var errHTTPReadOnly = model.WithKind(model.ErrReadOnly, errors.New("sc only downloads over http and https"))

// HTTPBackend reads a tree of folders off a web server (http://, https://)
// from the listings it makes of a folder without an index file: an HTML
// page of links (parseIndexHTML), or a JSON list (parseIndexJSON). It only
// downloads: every write is refused (errHTTPReadOnly).
//
// A folder is one GET of its page, and a file's size and time, which no
// page gives exactly, a HEAD (sizeFiles), up to httpListConns at once. The
// listing of a folder reads the pages of the folders in it too, to find
// links back up (markLoops), and keeps them for their own listings: a scan
// reads each page once. A scan's recursive listing (PreloadRecursive) walks
// the tree with folders side by side, and gives the scan each folder as
// soon as it is listed.
type HTTPBackend struct {
	*httpBase
	display   string
	listCache *listCache
	slots     chan struct{} // one for each request of a listing in flight
	noHead    atomic.Bool   // the server refuses HEAD: a GET of one byte stands for it
	htmlOnly  atomic.Bool   // the server's JSON is none sc reads: its HTML pages are asked for
	printsMu  sync.Mutex
	prints    map[uint64]uint64 // a folder's page print (indexPage.print), by the xxh3 of its path
	keptMu    sync.Mutex
	seed      *listingSeed        // the last folder listed by itself
	pages     map[string]keptPage // folder pages read with the listing of their parent, for their own
	swept     time.Time           // when pages last lost the pages kept too long
}

// keepAge is how long a listing or a page is kept for the listing that
// comes next (listingSeed, keptPage). An older one is read again.
const keepAge = time.Minute

// listingSeed is the last folder listed by itself, not in a recursive
// listing. A scan lists the folder it opens so, and then has the tree under
// it listed recursively; so does a rescan of a folder (model.Scanner). The
// recursive listing listed the folder again, with a HEAD for each file:
// twice the requests of a flat folder of 50,000 files. It takes the seed
// instead (takeSeed): what the scan shows of the folder.
type listingSeed struct {
	dir     string
	entries []model.FileEntry
	at      time.Time
	fresh   bool // listed under model.WithFreshListing
}

// keptPage is a folder's page that the listing of its parent read
// (markLoops), kept for the folder's own listing, which a recursive listing
// does next. err: the page could not be read, and another try gets the
// same.
type keptPage struct {
	page indexPage
	err  error
	at   time.Time
}

// NewHTTPBackend checks that rawURL names a folder the server lists; a
// cancel of ctx stops the check. The URL is read as a browser shows it:
// its path and login escaped (%20), a query or a fragment left out.
func NewHTTPBackend(ctx context.Context, rawURL string, insecure bool, parallel int) (*HTTPBackend, error) {
	scheme, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if i := strings.IndexAny(remotePath, "?#"); i >= 0 {
		remotePath = remotePath[:i]
	}
	hb, err := newHTTPBase(scheme, scheme == "https", host, port, unescapeURLPart(remotePath),
		unescapeURLPart(user), unescapeURLPart(pass), insecure, parallel, httpIdle.get())
	if err != nil {
		return nil, err
	}
	b := &HTTPBackend{
		httpBase:  hb,
		display:   hb.displayURL(scheme),
		listCache: newListCache(),
		slots:     make(chan struct{}, httpListConns),
		prints:    make(map[uint64]uint64),
		pages:     make(map[string]keptPage),
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := b.checkBase(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

// unescapeURLPart is s with its %XX escapes undone, or s as it is when it
// holds a % that starts none, as in "100%".
func unescapeURLPart(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

func (b *HTTPBackend) BasePath() string { return b.display }

// folderPath is the server path of the folder rel, with its slash.
func (b *HTTPBackend) folderPath(rel string) string {
	return strings.TrimSuffix(b.pathFor(rel), "/") + "/"
}

// checkBase asks for the base folder's page with a HEAD, so a path that is
// not there, a login refused or a file fail the open, not the first
// listing. The page is not read here: the scan lists it. A server that
// refuses HEAD there is left to that listing.
func (b *HTTPBackend) checkBase(ctx context.Context) error {
	resp, err := b.do(ctx, http.MethodHead, b.urlFor("", true), nil, map[string]string{"Accept": acceptListing})
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented:
		return nil
	case resp.StatusCode/100 != 2:
		return statusErr(resp, "%s: %s", b.proto, b.folderPath(""))
	}
	if ct := resp.Header.Get("Content-Type"); listingType(ct) == "" {
		return &invalidAnswer{fmt.Sprintf("%s: %s is %s, not a folder the server lists", b.proto, b.folderPath(""), ct)}
	}
	return nil
}

func (b *HTTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.liveList)
}

// liveList lists relDir by itself, not in a recursive listing, and keeps
// the listing for a recursive listing of relDir that starts next
// (listingSeed).
func (b *HTTPBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := cleanRel(relDir)
	entries, err := b.listFolder(ctx, dir, false)
	if err == nil {
		b.keptMu.Lock()
		b.seed = &listingSeed{dir: dir, entries: entries, at: time.Now(), fresh: model.WantsFreshListing(ctx)}
		b.keptMu.Unlock()
	}
	return entries, err
}

// takeSeed takes the seed for the recursive listing of scope under ctx:
// when it is of scope, younger than keepAge, and listed fresh if ctx asks
// for a fresh listing, by the rescan that starts the recursive listing.
func (b *HTTPBackend) takeSeed(ctx context.Context, scope string) ([]model.FileEntry, bool) {
	b.keptMu.Lock()
	defer b.keptMu.Unlock()
	s := b.seed
	if s == nil || s.dir != scope {
		return nil, false
	}
	b.seed = nil
	if time.Since(s.at) > keepAge || model.WantsFreshListing(ctx) && !s.fresh {
		return nil, false
	}
	return s.entries, true
}

// keepPage keeps dir's page, or why it could not be read, for dir's own
// listing, and drops the pages kept longer than keepAge.
func (b *HTTPBackend) keepPage(dir string, p indexPage, err error) {
	b.keptMu.Lock()
	defer b.keptMu.Unlock()
	now := time.Now()
	if now.Sub(b.swept) > keepAge {
		for d, k := range b.pages {
			if now.Sub(k.at) > keepAge {
				delete(b.pages, d)
			}
		}
		b.swept = now
	}
	b.pages[dir] = keptPage{page: p, err: err, at: now}
}

// takePage takes the page kept for dir's listing. fromParent: the
// recursive listing that lists dir listed its parent, and read the page
// then. Else the page may be older than ctx wants: a fresh listing
// (model.WithFreshListing, a rescan) takes none, others one younger than
// keepAge.
func (b *HTTPBackend) takePage(ctx context.Context, dir string, fromParent bool) (keptPage, bool) {
	b.keptMu.Lock()
	defer b.keptMu.Unlock()
	k, ok := b.pages[dir]
	if !ok {
		return keptPage{}, false
	}
	delete(b.pages, dir)
	if !fromParent && (model.WantsFreshListing(ctx) || time.Since(k.at) > keepAge) {
		return keptPage{}, false
	}
	return k, true
}

// dropPage drops a page kept for dir.
func (b *HTTPBackend) dropPage(dir string) {
	b.keptMu.Lock()
	delete(b.pages, dir)
	b.keptMu.Unlock()
}

// readPage is dir's page, with a listing's tries (listRequest).
func (b *HTTPBackend) readPage(ctx context.Context, dir string) (indexPage, error) {
	var p indexPage
	err := b.listRequest(ctx, "GET "+b.folderPath(dir), func(ctx context.Context) (err error) {
		p, err = b.getIndex(ctx, dir)
		return err
	})
	return p, err
}

// listFolder lists the folder dir: its page, a HEAD of each file the page
// gives no exact size and time of, and the page of each folder in it, to
// find links back up (markLoops). It takes a page kept for it (takePage,
// fromParent).
func (b *HTTPBackend) listFolder(ctx context.Context, dir string, fromParent bool) ([]model.FileEntry, error) {
	var p indexPage
	if k, ok := b.takePage(ctx, dir, fromParent); ok {
		if k.err != nil {
			return nil, k.err
		}
		p = k.page
	} else {
		var err error
		if p, err = b.readPage(ctx, dir); err != nil {
			return nil, err
		}
	}
	if p.bad > 0 {
		Log.Add(b.proto, DirWarn, fmt.Sprintf("%s: %d entries left out: their names are no names of a file in a folder", b.folderPath(dir), p.bad))
	}
	// A link back up that the listing of the parent did not find: the page
	// it read failed, or no listing of the parent read it.
	anc, loop, err := b.loopOf(ctx, dir, p.print)
	if err != nil {
		return nil, err
	}
	if loop {
		err := &invalidAnswer{fmt.Sprintf("%s: %s: its listing is that of %s: a link on the server leads back up there, and sc does not follow it round",
			b.proto, b.folderPath(dir), b.folderPath(anc))}
		Log.Add(b.proto, DirErr, err.Error())
		return nil, err
	}
	b.notePrint(dir, p.print)
	if err := b.sizeFiles(ctx, dir, p.entries); err != nil {
		return nil, err
	}
	if err := b.markLoops(ctx, dir, p.entries); err != nil {
		return nil, err
	}
	entries := make([]model.FileEntry, 0, len(p.entries))
	for _, e := range p.entries {
		if e.gone {
			Log.Add(b.proto, DirWarn, fmt.Sprintf("%s: the page links it, but nothing is there: left out", b.pathFor(path.Join(dir, e.name))))
			continue
		}
		entries = append(entries, e.fileEntry(dir))
	}
	Log.Add(b.proto, DirIn, fmt.Sprintf("%s: %d entries", b.folderPath(dir), len(entries)))
	return entries, nil
}

// getIndex gets the folder dir's listing: in JSON when the server gives
// one sc reads, else its page.
func (b *HTTPBackend) getIndex(ctx context.Context, dir string) (indexPage, error) {
	if !b.htmlOnly.Load() {
		p, err := b.fetchIndex(ctx, dir, acceptListing)
		if !errors.Is(err, errOtherJSON) {
			return p, err
		}
		if !b.htmlOnly.Swap(true) {
			Log.Add(b.proto, DirWarn, err.Error()+": its HTML pages are asked for from now on")
		}
	}
	return b.fetchIndex(ctx, dir, acceptHTML)
}

// fetchIndex is one GET of the folder dir's listing, with accept.
func (b *HTTPBackend) fetchIndex(ctx context.Context, dir, accept string) (indexPage, error) {
	page := b.urlFor(dir, true)
	resp, err := b.do(ctx, http.MethodGet, page, nil, map[string]string{"Accept": accept})
	if err != nil {
		return indexPage{}, err
	}
	defer drainClose(resp.Body)
	what := b.proto + ": GET " + b.folderPath(dir)
	if resp.StatusCode/100 != 2 {
		return indexPage{}, statusErr(resp, "%s", what)
	}
	var p indexPage
	switch ct := resp.Header.Get("Content-Type"); listingType(ct) {
	case "json":
		p, err = parseIndexJSON(resp.Body)
	case "html":
		var folder *url.URL
		if folder, err = url.Parse(page); err == nil {
			p, err = parseIndexHTML(folder, resp.Body)
		}
	default:
		return indexPage{}, &invalidAnswer{fmt.Sprintf("%s: the answer is %s, not a folder listing", what, ct)}
	}
	if err != nil {
		return indexPage{}, fmt.Errorf("%s: %w", what, err)
	}
	// An answer of no stated length ends where its connection does: one
	// cut off looks whole but for the page's end. An empty answer is no
	// empty folder: a server's page of one has its end tags.
	if !p.closed && (closeDelimited(resp) || len(p.entries) == 0) {
		return indexPage{}, fmt.Errorf("%s: the page ends before its listing does: %w", what, io.ErrUnexpectedEOF)
	}
	return p, nil
}

// closeDelimited reports an answer whose body ends only where its
// connection does: HTTP/1 with no Content-Length and not chunked.
func closeDelimited(resp *http.Response) bool {
	return resp.ProtoMajor < 2 && resp.ContentLength < 0 && len(resp.TransferEncoding) == 0
}

// listRequest runs op, a request of a listing, in one of the backend's
// slots (httpListConns), and again, up to httpListTries more times, while
// it fails with what another try can mend. It waits for each try with no
// slot held.
func (b *HTTPBackend) listRequest(ctx context.Context, what string, op func(context.Context) error) error {
	for try := 0; ; try++ {
		err := b.inSlot(ctx, op)
		if err == nil || try == httpListTries || ctx.Err() != nil || isPermanentError(err) {
			return err
		}
		d := backoffDelay(try)
		Log.Add(b.proto, DirRetry, fmt.Sprintf("%s: %v (attempt %d/%d in %v)", what, err, try+2, httpListTries+1, d))
		if sleepCtx(ctx, d) != nil {
			return err
		}
	}
}

func (b *HTTPBackend) inSlot(ctx context.Context, op func(context.Context) error) error {
	select {
	case b.slots <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	defer func() { <-b.slots }()
	return op(ctx)
}

// sizeFiles gives each file of entries whose listing gave no exact size
// and time a HEAD's (fileInfo), up to httpListConns at once over the
// backend. The first that fails ends the others.
func (b *HTTPBackend) sizeFiles(ctx context.Context, dir string, entries []indexEntry) error {
	var todo []int
	for i, e := range entries {
		if !e.dir && !e.other && !e.known {
			todo = append(todo, i)
		}
	}
	if len(todo) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(len(todo), httpListConns) {
		wg.Go(func() {
			for ctx.Err() == nil {
				k := int(next.Add(1)) - 1
				if k >= len(todo) {
					return
				}
				e := &entries[todo[k]]
				rel := path.Join(dir, e.name)
				err := b.listRequest(ctx, "HEAD "+b.pathFor(rel), func(ctx context.Context) error {
					got, err := b.fileInfo(ctx, rel)
					if err == nil {
						got.name = e.name
						*e = got
					}
					return err
				})
				if err != nil {
					cancel(err)
				}
			}
		})
	}
	wg.Wait()
	return context.Cause(ctx)
}

// fileInfo asks for rel as a file with a HEAD: its size and time, or that
// the server takes it for a folder (a redirect to its path with a slash),
// or that nothing is there (404, 410). A server that refuses HEAD, or
// answers one with no size, is asked for the file's first byte instead
// (rangedInfo).
func (b *HTTPBackend) fileInfo(ctx context.Context, rel string) (indexEntry, error) {
	if b.noHead.Load() {
		return b.rangedInfo(ctx, rel)
	}
	resp, err := b.do(ctx, http.MethodHead, b.urlFor(rel, false), nil, nil)
	if err != nil {
		return folderAnswer(err)
	}
	drainClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		return indexEntry{gone: true}, nil
	case resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented:
		if !b.noHead.Swap(true) {
			Log.Add(b.proto, DirWarn, fmt.Sprintf("the server refuses HEAD (%s): a GET of each file's first byte gives its size and time", resp.Status))
		}
		return b.rangedInfo(ctx, rel)
	case resp.StatusCode/100 != 2:
		return indexEntry{}, statusErr(resp, "%s: HEAD %s", b.proto, b.pathFor(rel))
	case resp.ContentLength < 0:
		return b.rangedInfo(ctx, rel)
	}
	return answerEntry(resp, resp.ContentLength), nil
}

// rangedInfo asks for rel's first byte: the size is the whole in the
// Content-Range of a 206 (of a 416 for an empty file), or the length of a
// 200 from a server that sends the whole file, which is not read.
func (b *HTTPBackend) rangedInfo(ctx context.Context, rel string) (indexEntry, error) {
	resp, err := b.do(ctx, http.MethodGet, b.urlFor(rel, false), nil, map[string]string{"Range": "bytes=0-0"})
	if err != nil {
		return folderAnswer(err)
	}
	defer drainClose(resp.Body)
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		return indexEntry{gone: true}, nil
	case http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		if n, ok := rangeTotal(resp.Header.Get("Content-Range")); ok {
			return answerEntry(resp, n), nil
		}
	case http.StatusOK:
		if resp.ContentLength >= 0 {
			return answerEntry(resp, resp.ContentLength), nil
		}
	default:
		return indexEntry{}, statusErr(resp, "%s: GET %s", b.proto, b.pathFor(rel))
	}
	return indexEntry{}, &invalidAnswer{fmt.Sprintf("%s: GET %s: %s, and the server says not how big the file is", b.proto, b.pathFor(rel), resp.Status)}
}

// folderAnswer reads err, from a request of a path as a file's: a redirect
// to the same path with a slash says it is a folder.
func folderAnswer(err error) (indexEntry, error) {
	var se *statusError
	if errors.As(err, &se) && se.folder {
		return indexEntry{dir: true}, nil
	}
	return indexEntry{}, err
}

// answerEntry is a file of size bytes, with the time of the answer's
// Last-Modified: to the second, or none when it gives none.
func answerEntry(resp *http.Response, size int64) indexEntry {
	e := indexEntry{known: true, size: size, res: model.TimeNone}
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		e.mtime, e.res = t, model.TimeSecond
	}
	return e
}

// rangeTotal is the whole size a Content-Range gives: "bytes 0-0/1234",
// or "bytes */0" with a 416.
func rangeTotal(cr string) (int64, bool) {
	rest, ok := strings.CutPrefix(cr, "bytes ")
	if !ok {
		return 0, false
	}
	_, total, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(total), 10, 64)
	return n, err == nil && n >= 0
}

// fileEntry is e as an entry of the folder dir.
func (e indexEntry) fileEntry(dir string) model.FileEntry {
	fe := model.FileEntry{RelPath: path.Join(dir, e.name), Name: e.name, ModTime: e.mtime, TimeRes: e.res}
	if e.mtime.IsZero() {
		fe.TimeRes = model.TimeNone
	}
	switch {
	case e.link != "":
		// The size a local folder's listing gives a link: its target's length.
		fe.Size, fe.Mode, fe.Link = int64(len(e.link)), os.ModeSymlink|0777, model.LinkTo(e.link)
	case e.dir:
		fe.IsDir, fe.Mode = true, os.ModeDir|0755
	case e.other:
		fe.Mode = os.ModeIrregular | 0644
	default:
		fe.Size, fe.Mode = e.size, 0644
	}
	return fe
}

// invalidAnswer is an answer another try of the request gets again, as
// fs.ErrInvalid: final, never tried again. Its text is its own.
type invalidAnswer struct{ msg string }

func (e *invalidAnswer) Error() string        { return e.msg }
func (e *invalidAnswer) Is(target error) bool { return target == fs.ErrInvalid }

// markLoops reads the page of each folder in entries, the entries of dir,
// and makes a link of each whose listing is that of dir or of a folder
// above it (loopOf): a link on the server back up there, which the server
// follows (Apache's FollowSymLinks; ubuntu -> . on Ubuntu's mirrors).
// Listed as a folder, it leads round and round, each time with the whole
// tree under it; listed as a folder that fails, it stopped every copy of a
// folder above it. As a link, a copy makes the link again, or leaves it out
// where it makes no links. The page of each other folder is kept for its
// own listing (keepPage), which a recursive listing does next. One that
// cannot be read leaves its folder as it is: the folder's own listing
// shows why, or reads it again when another try can mend it.
func (b *HTTPBackend) markLoops(ctx context.Context, dir string, entries []indexEntry) error {
	var todo []int
	for i, e := range entries {
		if e.dir {
			todo = append(todo, i)
		}
	}
	if len(todo) == 0 {
		return nil
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(len(todo), httpListConns) {
		wg.Go(func() {
			for ctx.Err() == nil {
				k := int(next.Add(1)) - 1
				if k >= len(todo) {
					return
				}
				e := &entries[todo[k]]
				sub := path.Join(dir, e.name)
				b.dropPage(sub)
				p, err := b.readPage(ctx, sub)
				if err != nil {
					if isPermanentError(err) && ctx.Err() == nil {
						b.keepPage(sub, indexPage{}, err)
					}
					continue
				}
				anc, loop, err := b.loopOf(ctx, sub, p.print)
				switch {
				case err != nil:
				case loop:
					e.dir, e.link = false, linkUp(dir, anc)
					Log.Add(b.proto, DirWarn, fmt.Sprintf("%s: its listing is that of %s: a link on the server leads back up there, listed as a link to %s",
						b.folderPath(sub), b.folderPath(anc), e.link))
				default:
					b.keepPage(sub, p, nil)
				}
			}
		})
	}
	wg.Wait()
	return context.Cause(ctx)
}

// loopOf finds the folder above dir, anc, whose listing is dir's: dir's
// page print is print, and anc's that the listing of anc noted. That is a
// link on the server back up to anc, or two folders that each hold one
// folder of one name and nothing else, made in the same minute: x/x. So
// the folder on the path from anc down to dir is read below dir too:
// through a link back up it is that folder again, and x/x/x is not x/x.
// Four folders x/x/x/x, each with one folder of its name and nothing else,
// still pass for a link. err: the folder below dir could not be read.
func (b *HTTPBackend) loopOf(ctx context.Context, dir string, print uint64) (anc string, loop bool, err error) {
	b.printsMu.Lock()
	found := false
	for anc = dir; anc != "" && !found; {
		anc = parentDir(anc)
		f, ok := b.prints[xxh3.HashString(anc)]
		found = ok && f == print
	}
	if !found {
		b.printsMu.Unlock()
		return "", false, nil
	}
	rest := dir
	if anc != "" {
		rest = strings.TrimPrefix(dir, anc+"/")
	}
	first, _, _ := strings.Cut(rest, "/")
	step, want := path.Join(anc, first), print
	if step != dir {
		want, found = b.prints[xxh3.HashString(step)]
	}
	b.printsMu.Unlock()
	if !found {
		return "", false, nil
	}
	p, err := b.readPage(ctx, path.Join(dir, first))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return anc, p.print == want, nil
}

// notePrint notes the print of dir's page, for loopOf in the folders under
// dir. A folder's page is read before those of the folders in it, in a scan
// as in a recursive listing, so the prints above a folder are there.
func (b *HTTPBackend) notePrint(dir string, print uint64) {
	b.printsMu.Lock()
	b.prints[xxh3.HashString(dir)] = print
	b.printsMu.Unlock()
}

// linkUp is the target of a link in the folder dir to anc, dir itself or
// a folder above it: ".", "..", "../..".
func linkUp(dir, anc string) string {
	n := depthOf(dir) - depthOf(anc)
	if n <= 0 {
		return "."
	}
	return strings.TrimSuffix(strings.Repeat("../", n), "/")
}

// depthOf is how many folders deep rel is: "" 0, "a" 1, "a/b" 2.
func depthOf(rel string) int {
	if rel == "" {
		return 0
	}
	return strings.Count(rel, "/") + 1
}

// Stat asks for relPath alone, with a HEAD (fileInfo), never from a
// listing.
func (b *HTTPBackend) Stat(ctx context.Context, relPath string) (model.FileEntry, error) {
	rel := cleanRel(relPath)
	if rel == "" {
		return model.FileEntry{IsDir: true, Mode: os.ModeDir | 0755}, nil
	}
	e, err := b.fileInfo(ctx, rel)
	switch {
	case err != nil:
		return model.FileEntry{}, err
	case e.gone:
		return model.FileEntry{}, fmt.Errorf("%s: HEAD %s: %w", b.proto, b.pathFor(rel), fs.ErrNotExist)
	}
	e.name = path.Base(rel)
	return e.fileEntry(parentDir(rel)), nil
}

// PreloadRecursive lists the tree under scope in the background
// (runRecursiveList), for the scan's listings to take from the cache.
func (b *HTTPBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

// runRecursiveList lists scope and each folder under it, breadth first,
// httpListConns folders at once, and up to httpListConns requests at once
// over them all. A folder is whole once its page is read, its HEADs
// answered and the pages of its folders read: it goes to the cache then,
// complete. scope is the listing just made of it, when there is one
// (takeSeed); each folder under it the page its parent's listing read. A
// folder whose listing fails is left to a listing of its own, where its
// error shows (unreadDirs), with the folders under it. One that failed
// after all its tries with what another try could mend says the server is
// not there now: the walk stops, and leaves the rest so too.
func (b *HTTPBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry), complete func(string)) error {
	scope = cleanRel(scope)
	q := newFolderQueue(scope)
	defer context.AfterFunc(ctx, q.stop)()
	var (
		mu     sync.Mutex
		unread []string
		why    error
		wg     sync.WaitGroup
	)
	for range httpListConns {
		wg.Go(func() {
			for {
				dir, ok := q.next()
				if !ok {
					return
				}
				entries, seeded := []model.FileEntry(nil), false
				if dir == scope {
					entries, seeded = b.takeSeed(ctx, dir)
				}
				var err error
				if seeded {
					Log.Add(b.proto, DirIn, fmt.Sprintf("%s: %d entries, as listed just before", b.folderPath(dir), len(entries)))
				} else {
					entries, err = b.listFolder(ctx, dir, dir != scope)
				}
				if err != nil {
					if ctx.Err() == nil {
						mu.Lock()
						unread = append(unread, dir)
						if why == nil {
							why = err
						}
						mu.Unlock()
						Log.Add(b.proto, DirWarn, fmt.Sprintf("the recursive listing leaves %s to a listing of its own: %v", b.folderPath(dir), err))
						if !isPermanentError(err) {
							q.stop()
						}
					}
					q.done(nil)
					continue
				}
				emit(dir, entries)
				complete(dir)
				var dirs []string
				for _, e := range entries {
					if e.IsDir {
						dirs = append(dirs, e.RelPath)
					}
				}
				q.done(dirs)
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(unread) > 0 {
		return &unreadDirs{dirs: unread, why: why.Error()}
	}
	return nil
}

// folderQueue holds the folders a recursive listing has still to list,
// first in first listed, and counts those it lists now. It is empty for
// good when both are none, or once stopped.
type folderQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	dirs    []string
	busy    int
	stopped bool
}

func newFolderQueue(first string) *folderQueue {
	q := &folderQueue{dirs: []string{first}}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// next takes the next folder to list, and false when there is none for
// good. It waits while folders that are being listed may still add more.
func (q *folderQueue) next() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.dirs) == 0 && q.busy > 0 && !q.stopped {
		q.cond.Wait()
	}
	if q.stopped || len(q.dirs) == 0 {
		return "", false
	}
	dir := q.dirs[0]
	q.dirs = q.dirs[1:]
	q.busy++
	return dir, true
}

// done ends the listing of a folder next gave, with the folders found in it.
func (q *folderQueue) done(dirs []string) {
	q.mu.Lock()
	q.dirs = append(q.dirs, dirs...)
	q.busy--
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *folderQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

// A web server keeps no checksum it gives: a file's sum would cost the
// file's download, and sc sums no file by downloading it.
func (b *HTTPBackend) ChecksumAlgos(context.Context) ([]string, error) { return nil, nil }

func (b *HTTPBackend) Checksum(_ context.Context, relPath, algo string) (string, error) {
	return "", noSumError{algo: algo, path: b.pathFor(relPath)}
}

func (b *HTTPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(ctx, relPath, 0)
}

// OpenAt downloads relPath from offset, with a Range request past 0. A
// server that sends the whole file instead has its first offset bytes
// dropped (rangeBody).
func (b *HTTPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	var hdrs map[string]string
	if offset > 0 {
		hdrs = map[string]string{"Range": fmt.Sprintf("bytes=%d-", offset)}
	}
	resp, err := b.do(ctx, http.MethodGet, b.urlFor(relPath, false), nil, hdrs)
	if err != nil {
		return nil, err
	}
	return rangeBody(ctx, resp, fmt.Sprintf("%s: GET %s @%d", b.proto, b.pathFor(relPath), offset), offset)
}

// readOnly is the error of the write op on relPath.
func (b *HTTPBackend) readOnly(op, relPath string) error {
	return fmt.Errorf("%s: %s %s: %w", b.proto, op, b.pathFor(relPath), errHTTPReadOnly)
}

func (b *HTTPBackend) SetTimes(_ context.Context, relPath string, _, _, _ time.Time) error {
	return b.readOnly("touch", relPath)
}

func (b *HTTPBackend) CopyFrom(_ context.Context, relPath string, _ io.Reader, _ model.PutParams) error {
	return b.readOnly("upload", relPath)
}

func (b *HTTPBackend) Mkdir(_ context.Context, relPath string, _ os.FileMode) error {
	return b.readOnly("mkdir", relPath)
}

func (b *HTTPBackend) Rename(_ context.Context, oldRelPath, _ string) error {
	return b.readOnly("rename", oldRelPath)
}

func (b *HTTPBackend) Remove(_ context.Context, relPath string) error {
	return b.readOnly("delete", relPath)
}

func (b *HTTPBackend) RemoveAll(_ context.Context, relPath string) error {
	return b.readOnly("delete", relPath)
}
