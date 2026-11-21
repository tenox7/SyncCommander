package transport

import (
	"errors"
	"io"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// Pages as the servers make them, for a folder /files/ with a folder docs
// and a file or two.
const (
	apacheTablePage = `<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /files</title>
 </head>
 <body>
<h1>Index of /files</h1>
  <table>
   <tr><th valign="top"><img src="/icons/blank.gif" alt="[ICO]"></th><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>
   <tr><th colspan="5"><hr></th></tr>
<tr><td valign="top"><img src="/icons/back.gif" alt="[PARENTDIR]"></td><td><a href="/">Parent Directory</a></td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/folder.gif" alt="[DIR]"></td><td><a href="docs/">docs/</a></td><td align="right">2023-05-01 12:34  </td><td align="right">  - </td><td>&nbsp;</td></tr>
<tr><td valign="top"><a href="my%20file.tar.gz"><img src="/icons/compressed.gif" alt="[   ]"></a></td><td><a href="my%20file.tar.gz">my file.tar.gz</a></td><td align="right">2023-05-01 12:35  </td><td align="right">1.2M</td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/text.gif" alt="[TXT]"></td><td><a href="a%26b.txt">a&amp;b.txt</a></td><td align="right">2023-05-02 08:00  </td><td align="right"> 45 </td><td>&nbsp;</td></tr>
   <tr><th colspan="5"><hr></th></tr>
</table>
<address>Apache/2.4.57 (Debian) Server at example.com Port 80</address>
</body></html>
`
	apachePrePage = `<html><head><title>Index of /files</title></head><body>
<h1>Index of /files</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                    <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/back.gif" alt="[PARENTDIR]"> <a href="/">Parent Directory</a>                             -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="docs/">docs/</a>                   2023-05-01 12:34    -
<img src="/icons/text.gif" alt="[TXT]"> <a href="readme.txt">readme.txt</a>              2023-05-01 12:35  123
<hr></pre>
<address>Apache/2.2.22 Server at example.com Port 80</address>
</body></html>
`
	nginxPage = `<html>
<head><title>Index of /files/</title></head>
<body>
<h1>Index of /files/</h1><hr><pre><a href="../">../</a>
<a href="docs/">docs/</a>                                              01-May-2023 12:34                   -
<a href="very-long-file-name-that-nginx-cuts-short-in-its-text.iso">very-long-file-name-that-nginx-cuts-short-in-..&gt;</a> 01-May-2023 12:35            12345678
<a href="%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82.txt">привет.txt</a>                                         02-May-2023 08:00                  45
</pre><hr></body>
</html>
`
	lighttpdPage = `<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head><title>Index of /files/</title></head>
<body>
<h2>Index of /files/</h2>
<div class="list">
<table summary="Directory Listing" cellpadding="0" cellspacing="0">
<thead><tr><th class="n">Name</th><th class="m">Last Modified</th><th class="s">Size</th><th class="t">Type</th></tr></thead>
<tbody>
<tr class="d"><td class="n"><a href="../">..</a>/</td><td class="m">&nbsp;</td><td class="s">- &nbsp;</td><td class="t">Directory</td></tr>
<tr class="d"><td class="n"><a href="docs/">docs</a>/</td><td class="m">2023-May-01 12:34:56</td><td class="s">- &nbsp;</td><td class="t">Directory</td></tr>
<tr><td class="n"><a href="readme.txt">readme.txt</a></td><td class="m">2023-May-01 12:35:00</td><td class="s">0.1K</td><td class="t">text/plain</td></tr>
</tbody>
</table>
</div>
<div class="foot">lighttpd/1.4.69</div>
</body>
</html>
`
	iisPage = `<html><head><title>example.com - /files/</title></head><body><H1>example.com - /files/</H1><hr>

<pre><A HREF="/">[To Parent Directory]</A><br><br>     Monday, May 1, 2023 12:34 PM        &lt;dir&gt; <A HREF="/files/docs/">docs</A><br>     Monday, May 1, 2023 12:35 PM          123 <A HREF="/files/readme.txt">readme.txt</A><br></pre><hr></body></html>
`
	pythonPage = `<!DOCTYPE HTML>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Directory listing for /files/</title>
</head>
<body>
<h1>Directory listing for /files/</h1>
<hr>
<ul>
<li><a href="docs/">docs/</a></li>
<li><a href="readme.txt">readme.txt</a></li>
</ul>
<hr>
</body>
</html>
`
	goPage = `<!doctype html>
<meta name="viewport" content="width=device-width">
<pre>
<a href="docs/">docs/</a>
<a href="readme.txt">readme.txt</a>
</pre>
`
)

var filesURL, _ = url.Parse("http://example.com:80/files/")

// pageRows is the entries of a page as "name" or "name/".
func pageRows(p indexPage) []string {
	var out []string
	for _, e := range p.entries {
		if e.dir {
			out = append(out, e.name+"/")
		} else {
			out = append(out, e.name)
		}
	}
	return out
}

func parsePage(t *testing.T, folder *url.URL, page string) indexPage {
	t.Helper()
	p, err := parseIndexHTML(folder, strings.NewReader(page))
	if err != nil {
		t.Fatalf("parseIndexHTML: %v", err)
	}
	return p
}

// The pages of each server give the folder's entries: not the parent, not
// the sort orders, a name linked twice once, and names unescaped.
func TestParseIndexPagesOfServers(t *testing.T) {
	for _, tc := range []struct {
		server, page string
		want         []string
	}{
		{"apache table", apacheTablePage, []string{"docs/", "my file.tar.gz", "a&b.txt"}},
		{"apache pre", apachePrePage, []string{"docs/", "readme.txt"}},
		{"nginx", nginxPage, []string{"docs/", "very-long-file-name-that-nginx-cuts-short-in-its-text.iso", "привет.txt"}},
		{"lighttpd", lighttpdPage, []string{"docs/", "readme.txt"}},
		{"iis", iisPage, []string{"docs/", "readme.txt"}},
		{"python", pythonPage, []string{"docs/", "readme.txt"}},
		{"go", goPage, []string{"docs/", "readme.txt"}},
	} {
		p := parsePage(t, filesURL, tc.page)
		if got := pageRows(p); !slices.Equal(got, tc.want) {
			t.Errorf("%s: entries %q, want %q", tc.server, got, tc.want)
		}
		if !p.closed {
			t.Errorf("%s: the page reads as cut short", tc.server)
		}
		for _, e := range p.entries {
			if e.known {
				t.Errorf("%s: %s: a page's size taken as exact", tc.server, e.name)
			}
		}
	}
}

// L7: a page names what it likes. Only a link to one entry of the folder
// itself is one: none that leads out of it, deeper into it, to another
// server, or holds a query or a fragment.
func TestParseIndexKeepsToTheFolder(t *testing.T) {
	page := `<html><body><pre>
<a href="../secret">x</a>
<a href="../../etc/passwd">x</a>
<a href="a/../../b">x</a>
<a href="sub/deeper.txt">x</a>
<a href="http://other.example/files/x">x</a>
<a href="https://example.com/files/x">x</a>
<a href="http://example.com:8080/files/x">x</a>
<a href="x?download=1">x</a>
<a href="?C=M;O=A">x</a>
<a href="#top">x</a>
<a href="x#part">x</a>
<a href="mailto:a@example.com">x</a>
<a href="javascript:alert(1)">x</a>
<a href="a%2Fb">x</a>
<a href="..%2Fx">x</a>
<a href="%2E%2E/">x</a>
<a href="..">x</a>
<a href="./">x</a>
<a href="">x</a>
<a name="anchor">x</a>
<a href="ok.txt">ok</a>
<a href=" spaced.txt ">s</a>
<a href="//example.com/files/proto-relative">x</a>
<a href="http://EXAMPLE.com/files/upper-host">x</a>
<a href="/files/absolute/">x</a>
<a href="b/./">x</a>
<a href="back%5C..%5Cslash">x</a>
</pre></body></html>`
	got := pageRows(parsePage(t, filesURL, page))
	want := []string{"ok.txt", "spaced.txt", "proto-relative", "upper-host", "absolute/", "b/"}
	if !slices.Equal(got, want) {
		t.Errorf("entries %q, want %q", got, want)
	}
}

// A <base> moves where the page's links lead from, not the folder they must
// lead into.
func TestParseIndexBaseTag(t *testing.T) {
	page := `<html><head><base href="/files/sub/"></head><body><pre>
<a href="x">x</a>
<a href="../y">y</a>
</pre></body></html>`
	if got, want := pageRows(parsePage(t, filesURL, page)), []string{"y"}; !slices.Equal(got, want) {
		t.Errorf("entries %q, want %q", got, want)
	}
}

// A page whose links spell the folder in another letter case (IIS, asked
// for /Files/) has no entry as asked: it is no empty folder.
func TestParseIndexOtherCaseIsNoEmptyFolder(t *testing.T) {
	folder, _ := url.Parse("http://example.com/Files/")
	_, err := parseIndexHTML(folder, strings.NewReader(iisPage))
	if !errors.Is(err, errOtherSpelling) {
		t.Fatalf("err = %v, want errOtherSpelling", err)
	}
	if !isPermanentError(err) {
		t.Error("tried again: the same page comes again")
	}
}

// A page cut short ends with no end tag of its list.
func TestParseIndexClosed(t *testing.T) {
	for page, want := range map[string]bool{
		"<pre><a href=\"x\">x</a>\n":                           false,
		"<pre><a href=\"x\">x</a>\n</pre>":                     true,
		"<table><tr><td><a href=\"x\">x</a></td></tr>":         false,
		"<table><tr><td><a href=\"x\">x</a></td></tr></table>": true,
		"<pre>\n</pre>": true,
		"":              false,
	} {
		if got := parsePage(t, filesURL, page).closed; got != want {
			t.Errorf("%q: closed = %v, want %v", page, got, want)
		}
	}
}

// One folder's page at two URLs, as a link back up shows it, has one
// print; a page with one date changed has another.
func TestParseIndexPrint(t *testing.T) {
	loop, _ := url.Parse("http://example.com/files/loop/")
	for _, page := range []string{apacheTablePage, apachePrePage, nginxPage, lighttpdPage, pythonPage, goPage} {
		a, b := parsePage(t, filesURL, page), parsePage(t, loop, page)
		if a.print != b.print {
			t.Errorf("one page at two URLs has two prints:\n%s", page)
		}
	}
	a := parsePage(t, filesURL, apacheTablePage)
	b := parsePage(t, filesURL, strings.Replace(apacheTablePage, "2023-05-02 08:00", "2023-05-02 08:01", 1))
	if a.print == b.print {
		t.Error("two pages that differ in a date have one print")
	}
	// IIS links by absolute path: the names are the page's.
	iisLoop := strings.ReplaceAll(iisPage, "/files/", "/files/loop/")
	if parsePage(t, filesURL, iisPage).print != parsePage(t, loop, iisLoop).print {
		t.Error("an IIS page at two URLs has two prints")
	}
}

func TestParseIndexJSON(t *testing.T) {
	nginx := `[
{ "name":"docs", "type":"directory", "mtime":"Mon, 01 May 2023 12:34:56 GMT" },
{ "name":"readme.txt", "type":"file", "mtime":"Mon, 01 May 2023 12:35:00 GMT", "size":123 },
{ "name":"sock", "type":"other", "mtime":"Mon, 01 May 2023 12:35:00 GMT" },
{ "name":"../evil", "type":"file", "mtime":"Mon, 01 May 2023 12:35:00 GMT", "size":1 },
{ "name":"nosize.bin", "type":"file", "mtime":"Mon, 01 May 2023 12:35:00 GMT" }
]
`
	caddy := `[{"name":"docs/","size":4096,"url":"./docs/","mod_time":"2023-05-01T12:34:56.123456789Z","mode":2147484141,"is_dir":true,"is_symlink":false},` +
		`{"name":"readme.txt","size":123,"url":"./readme.txt","mod_time":"2023-05-01T12:35:00Z","mode":420,"is_dir":false,"is_symlink":false},` +
		`{"name":"link.txt","size":7,"url":"./link.txt","mod_time":"2023-05-01T12:35:00Z","mode":134218239,"is_dir":false,"is_symlink":true}]`
	readme := time.Date(2023, 5, 1, 12, 35, 0, 0, time.UTC)

	p, err := parseIndexJSON(strings.NewReader(nginx))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := pageRows(p), []string{"docs/", "readme.txt", "sock", "nosize.bin"}; !slices.Equal(got, want) {
		t.Fatalf("nginx: entries %q, want %q", got, want)
	}
	if p.bad != 1 {
		t.Errorf("nginx: bad = %d, want 1 (../evil)", p.bad)
	}
	if e := p.entries[1]; !e.known || e.size != 123 || !e.mtime.Equal(readme) || e.res != model.TimeSecond {
		t.Errorf("nginx: readme.txt = %+v, want 123 bytes at %v to the second", e, readme)
	}
	if e := p.entries[2]; !e.other {
		t.Errorf("nginx: sock = %+v, want a special entry", e)
	} else if fe := e.fileEntry(""); !fe.IsSpecial() {
		t.Errorf("nginx: sock = %+v, want a special entry", e)
	}
	if p.entries[3].known {
		t.Error("nginx: a file with no size taken as known")
	}

	p, err = parseIndexJSON(strings.NewReader(caddy))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := pageRows(p), []string{"docs/", "readme.txt", "link.txt"}; !slices.Equal(got, want) {
		t.Fatalf("caddy: entries %q, want %q", got, want)
	}
	if e := p.entries[1]; !e.known || e.size != 123 || !e.mtime.Equal(readme) || e.res != model.TimeSecond {
		t.Errorf("caddy: readme.txt = %+v", e)
	}
	if e := p.entries[0]; e.res != model.TimeExact || e.mtime.Nanosecond() != 123456789 {
		t.Errorf("caddy: docs = %+v, want its time to the nanosecond", e)
	}
	if p.entries[2].known {
		t.Error("caddy: a link's size taken as its file's")
	}

	for _, other := range []string{`{"files":[]}`, `[{"foo":1}]`, `[1,2]`, `"x"`, ``, `<html>`} {
		if _, err := parseIndexJSON(strings.NewReader(other)); !errors.Is(err, errOtherJSON) {
			t.Errorf("%q: err = %v, want errOtherJSON", other, err)
		}
	}
	const item = `{"name":"a","type":"file","size":1,"mtime":"Mon, 01 May 2023 12:35:00 GMT"}`
	for _, cut := range []string{`[`, `[` + item, `[` + item + `,`, `[` + item + `, {"na`} {
		if _, err := parseIndexJSON(strings.NewReader(cut)); err == nil || errors.Is(err, errOtherJSON) || !errors.Is(err, io.ErrUnexpectedEOF) || isPermanentError(err) {
			t.Errorf("%q, a list cut short: err = %v, want one another try can mend", cut, err)
		}
	}
	if _, err := parseIndexJSON(strings.NewReader(`[` + item + ` x]`)); !errors.Is(err, errOtherJSON) {
		t.Errorf("a list with a syntax error: err = %v, want errOtherJSON", err)
	}
}

func TestListingType(t *testing.T) {
	for ct, want := range map[string]string{
		"":                                "html",
		"text/html":                       "html",
		"text/html; charset=utf-8":        "html",
		"application/xhtml+xml":           "html",
		"application/json":                "json",
		"application/json; charset=utf-8": "json",
		"application/vnd.x+json":          "json",
		"application/octet-stream":        "",
		"text/plain":                      "",
		"text/html; charset=\"":           "",
	} {
		if got := listingType(ct); got != want {
			t.Errorf("listingType(%q) = %q, want %q", ct, got, want)
		}
	}
}
