package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"

	"sc/model"
)

// A web server shows a folder that has no index file as a page of links,
// one for each entry: Apache's mod_autoindex, nginx's autoindex, lighttpd's
// dir-listing, IIS's directory browsing, Caddy's browse, Python's
// http.server, Go's http.FileServer. sc reads the links and nothing else of
// the page: its columns and date formats differ from server to server, its
// sizes are rounded (Apache's "1.2M"), and its times are to the minute, in
// the server's own time zone. A HEAD of each file gives the file's size
// and time exactly (HTTPBackend.sizeFiles).
//
// nginx with autoindex_format json, and Caddy, which answers in JSON a
// request that accepts it, give each entry's exact size and time in the
// listing: then no HEAD is sent.

// indexEntry is one entry of a folder as its listing gives it.
type indexEntry struct {
	name  string
	dir   bool
	other bool // neither a file nor a folder (nginx's "other"): listed, never read
	// known: size, mtime and res are the listing's own, exact (JSON).
	// Else a file's come from a HEAD.
	known bool
	gone  bool // the HEAD says nothing is there: left out of the listing
	// link: a folder whose listing is that of the folder above it, or of
	// one above that: a link on the server back up there (markLoops). It is
	// listed as a link with this target, ".", "..", and never read.
	link  string
	size  int64
	mtime time.Time
	res   model.TimeRes
}

// indexPage is what a folder's listing gives.
type indexPage struct {
	entries []indexEntry
	// print sums the entries as the page shows them: names, kinds, and the
	// text of each entry's row (a date, a size) or its exact size and
	// time. Two folders with one print show one listing (checkLoop).
	print uint64
	// closed: the page ends as a page does, with the end tag of what
	// holds its entries. A page whose connection closed early, which an
	// answer of no stated length cannot tell from its end, has none.
	closed bool
	bad    int // entries left out: a name that is no name in a folder
}

// hrefKind is what a link on a folder's page points to.
type hrefKind int

const (
	hrefElsewhere hrefKind = iota // no entry of the folder
	hrefEntry                     // an entry of the folder
	hrefOtherCase                 // an entry, but its path spells the folder's in another letter case
)

// classifyLink reads href, a link on the page of folder, base the URL the
// page's links are read from: the name of the folder's entry the link
// points to, and whether that is a folder (its path ends with a slash). A
// link to the folder above, to the page itself, to a sort order (Apache's
// ?C=M;O=A), to another host or to a path deeper down is no entry.
func classifyLink(folder, base *url.URL, href string) (string, bool, hrefKind) {
	u, err := base.Parse(strings.TrimSpace(href))
	if err != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || !sameOrigin(folder, u) {
		return "", false, hrefElsewhere
	}
	kind := hrefEntry
	rest, ok := strings.CutPrefix(u.Path, folder.Path)
	if !ok {
		if len(u.Path) <= len(folder.Path) || !strings.EqualFold(u.Path[:len(folder.Path)], folder.Path) {
			return "", false, hrefElsewhere
		}
		rest, kind = u.Path[len(folder.Path):], hrefOtherCase
	}
	name, dir := strings.CutSuffix(rest, "/")
	if !model.IsFileName(name) {
		return "", false, hrefElsewhere // the page itself, or a path deeper down
	}
	return name, dir, kind
}

// sameOrigin reports whether a and b name one server: scheme, host and
// port, with the scheme's own port for none.
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Hostname(), b.Hostname()) && portOf(a) == portOf(b)
}

func portOf(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	if strings.EqualFold(u.Scheme, "https") {
		return "443"
	}
	return "80"
}

// maxRowText caps the text of one row that goes into a page's print.
const maxRowText = 256

// rowEnds are the elements whose start or end ends an entry's row: the
// rows of a table, the items of a list, a line break, and the blocks
// around the listing. A table's cells are not among them: Apache and
// lighttpd give the date and the size in the cells after the link's.
var rowEnds = map[atom.Atom]bool{
	atom.Address: true, atom.Article: true, atom.Aside: true, atom.Blockquote: true, atom.Body: true,
	atom.Br: true, atom.Dd: true, atom.Div: true, atom.Dl: true, atom.Dt: true, atom.Fieldset: true,
	atom.Figure: true, atom.Footer: true, atom.Form: true, atom.H1: true, atom.H2: true, atom.H3: true,
	atom.H4: true, atom.H5: true, atom.H6: true, atom.Header: true, atom.Hr: true, atom.Html: true,
	atom.Li: true, atom.Main: true, atom.Nav: true, atom.Ol: true, atom.P: true, atom.Pre: true,
	atom.Section: true, atom.Table: true, atom.Tbody: true, atom.Tfoot: true, atom.Thead: true,
	atom.Tr: true, atom.Ul: true,
}

// pageEnds are the elements whose end tag ends a listing: what holds its
// rows, or the page.
var pageEnds = map[atom.Atom]bool{
	atom.Table: true, atom.Pre: true, atom.Ul: true, atom.Ol: true, atom.Dl: true, atom.Body: true, atom.Html: true,
}

// errOtherSpelling: a page that links no entry of its folder, but entries
// of the folder spelled in another letter case. Read as it is, the folder
// would be empty, and a mirror from it would delete the other side.
var errOtherSpelling error = &invalidAnswer{"the page links its entries under the folder's path in another letter case: open the folder with its path spelled as the server spells it"}

// parseIndexHTML reads the entries of folder from its page, a stream of
// HTML read token by token: no tree of the page is built, so a folder of a
// million files costs its entries, not its page. A name linked twice, as
// by its icon and by its name, is one entry.
func parseIndexHTML(folder *url.URL, r io.Reader) (indexPage, error) {
	z := html.NewTokenizer(r)
	base := folder
	var (
		p      indexPage
		seen   = make(map[string]int)
		cur    = -1 // the entry whose row is open
		row    []byte
		others int
		print  = xxh3.New()
	)
	endRow := func() {
		if cur < 0 {
			return
		}
		e := &p.entries[cur]
		print.WriteString(e.name)
		print.Write([]byte{0, kindByte(*e), 0})
		print.WriteString(strings.Join(strings.Fields(string(row)), " "))
		print.Write([]byte{0})
		cur, row = -1, row[:0]
	}
	for {
		switch z.Next() {
		case html.ErrorToken:
			if err := z.Err(); err != io.EOF {
				return indexPage{}, err
			}
			endRow()
			if len(p.entries) == 0 && others > 0 {
				return indexPage{}, errOtherSpelling
			}
			p.print = print.Sum64()
			return p, nil
		case html.TextToken:
			if cur >= 0 && len(row) < maxRowText {
				row = append(row, z.Text()...)
				row = row[:min(len(row), maxRowText)]
			}
		case html.StartTagToken, html.SelfClosingTagToken:
			tag, hasAttr := z.TagName()
			a := atom.Lookup(tag)
			if a != atom.A && a != atom.Base {
				if rowEnds[a] {
					endRow()
				}
				continue
			}
			href, ok := attrValue(z, hasAttr, "href")
			if !ok {
				continue
			}
			if a == atom.Base {
				if u, err := folder.Parse(strings.TrimSpace(href)); err == nil {
					base = u
				}
				continue
			}
			name, dir, kind := classifyLink(folder, base, href)
			switch kind {
			case hrefOtherCase:
				others++
			case hrefEntry:
				p.closed = false
				if i, ok := seen[name]; ok {
					p.entries[i].dir = p.entries[i].dir || dir
					continue
				}
				endRow()
				seen[name] = len(p.entries)
				p.entries = append(p.entries, indexEntry{name: name, dir: dir})
				cur = len(p.entries) - 1
			}
		case html.EndTagToken:
			tag, _ := z.TagName()
			a := atom.Lookup(tag)
			if rowEnds[a] {
				endRow()
			}
			if pageEnds[a] {
				p.closed = true
			}
		}
	}
}

// kindByte names an entry's kind in a page's print.
func kindByte(e indexEntry) byte {
	switch {
	case e.dir:
		return 'd'
	case e.other:
		return 'o'
	}
	return 'f'
}

// attrValue is the value of the current tag's attribute key.
func attrValue(z *html.Tokenizer, more bool, key string) (string, bool) {
	for more {
		var k, v []byte
		k, v, more = z.TagAttr()
		if string(k) == key {
			return string(v), true
		}
	}
	return "", false
}

// jsonIndexItem is one entry of a JSON listing: nginx's
// (autoindex_format json: name, type, mtime, size) or Caddy's (name, size,
// mod_time, is_dir, is_symlink).
type jsonIndexItem struct {
	Name      *string `json:"name"`
	Type      string  `json:"type"`
	MTime     string  `json:"mtime"`
	Size      *int64  `json:"size"`
	IsDir     *bool   `json:"is_dir"`
	ModTime   string  `json:"mod_time"`
	IsSymlink bool    `json:"is_symlink"`
}

// errOtherJSON: the server answered the listing in a JSON sc does not
// read. It is asked for HTML pages instead.
var errOtherJSON error = &invalidAnswer{"the server answers in a JSON that is no folder listing sc reads"}

// entry is the item as an entry, and false for an item that is neither
// nginx's nor Caddy's.
func (it jsonIndexItem) entry() (indexEntry, bool) {
	if it.Name == nil {
		return indexEntry{}, false
	}
	e := indexEntry{name: strings.TrimSuffix(*it.Name, "/")} // Caddy ends a folder's name with a slash
	var err error
	switch {
	case it.IsDir != nil: // Caddy
		e.dir = *it.IsDir
		e.mtime, err = time.Parse(time.RFC3339Nano, it.ModTime)
		e.res = model.TimeSecond
		if e.mtime.Nanosecond() != 0 {
			e.res = model.TimeExact
		}
		// A link's size and time may be the link's own, not its file's.
		e.known = err == nil && it.Size != nil && !it.IsSymlink
	case it.Type != "": // nginx
		switch it.Type {
		case "directory":
			e.dir = true
		case "file":
		default:
			e.other = true
		}
		e.mtime, err = http.ParseTime(it.MTime)
		e.res = model.TimeSecond
		e.known = err == nil && (it.Size != nil || e.dir || e.other)
	default:
		return indexEntry{}, false
	}
	if err != nil {
		e.mtime, e.res = time.Time{}, model.TimeNone
	}
	if e.known && it.Size != nil && !e.dir {
		e.size = max(*it.Size, 0)
	}
	return e, true
}

// parseIndexJSON reads a JSON listing: a list of items, read one by one.
// The list is whole only from its [ to its ]: one cut short is an error
// another try can mend, one of another shape is errOtherJSON.
func parseIndexJSON(r io.Reader) (indexPage, error) {
	src := &jsonSource{r: r}
	dec := json.NewDecoder(src)
	t, err := dec.Token()
	if err != nil || t != json.Delim('[') {
		var syntax *json.SyntaxError
		if err == nil || err == io.EOF || errors.As(err, &syntax) {
			return indexPage{}, errOtherJSON
		}
		return indexPage{}, err
	}
	var p indexPage
	print := xxh3.New()
	for dec.More() {
		var it jsonIndexItem
		if err := dec.Decode(&it); err != nil {
			return indexPage{}, src.jsonShape(err)
		}
		e, ok := it.entry()
		if !ok {
			return indexPage{}, errOtherJSON
		}
		if !model.IsFileName(e.name) {
			p.bad++
			continue
		}
		print.WriteString(e.name)
		print.Write([]byte{0, kindByte(e), 0})
		print.WriteString(strconv.FormatInt(e.size, 10) + " " + strconv.FormatInt(e.mtime.UnixNano(), 10))
		print.Write([]byte{0})
		p.entries = append(p.entries, e)
	}
	if t, err = dec.Token(); err != nil {
		return indexPage{}, src.jsonShape(err)
	}
	if t != json.Delim(']') {
		return indexPage{}, errOtherJSON
	}
	p.print, p.closed = print.Sum64(), true
	return p, nil
}

// jsonSource counts what a JSON listing reads, and sees its end.
type jsonSource struct {
	r   io.Reader
	n   int64
	eof bool
}

func (c *jsonSource) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += int64(n)
	c.eof = c.eof || err == io.EOF
	return n, err
}

// jsonShape reads err, from reading the items of a JSON list: a list that
// ends before its ] was cut short, which another try can mend. The
// decoder says "unexpected end of JSON input" for it, a syntax error at
// the end of all that came. Any other syntax error, or an item of
// another type, is JSON sc does not read (errOtherJSON).
func (c *jsonSource) jsonShape(err error) error {
	var syntax *json.SyntaxError
	var typ *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntax) && c.eof && syntax.Offset >= c.n:
		return fmt.Errorf("the list ends before its ]: %w", io.ErrUnexpectedEOF)
	case errors.As(err, &syntax), errors.As(err, &typ):
		return errOtherJSON
	case err == io.EOF:
		return fmt.Errorf("the list ends before its ]: %w", io.ErrUnexpectedEOF)
	}
	return err
}

// listingType is the kind of listing a Content-Type says an answer is:
// "html", "json", or "" for neither. None said is taken for HTML.
func listingType(contentType string) string {
	if contentType == "" {
		return "html"
	}
	mt, _, err := mime.ParseMediaType(contentType)
	switch {
	case err != nil:
		return ""
	case mt == "text/html" || mt == "application/xhtml+xml":
		return "html"
	case mt == "application/json" || strings.HasSuffix(mt, "+json"):
		return "json"
	}
	return ""
}
