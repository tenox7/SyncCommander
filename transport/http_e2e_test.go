package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"sc/model"
)

// Real web servers list a tree with hard names, links back up and a big
// folder: the listing over http:// is the tree on disk.

// e2eWebNames are names a server must escape in its links.
var e2eWebNames = []string{
	"a b.txt", "a&b.txt", "100%.txt", "q?.txt", "hash#.txt", "семь.txt", "x;y.txt", "plus+.txt",
	"quote'.txt", `dq".txt`, "<lt>.txt", "tab\tname.txt", ".hidden", "ü-nfc.txt",
}

// e2eWebTree makes the tree a real server lists: every name of
// e2eWebNames, folders deep and empty, a folder of 1,500 files, and two
// links back up. It gives the folder and the tree as "rel size" and "rel/".
func e2eWebTree(t *testing.T) (string, []string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "www")
	var want []string
	add := func(rel, body string) {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		os.Chtimes(full, webTime, webTime)
		want = append(want, fmt.Sprintf("%s %d", rel, len(body)))
	}
	for i, n := range e2eWebNames {
		add(n, strings.Repeat("x", i))
		add("d i r/"+n, strings.Repeat("y", i+1))
	}
	add("deep/er/still/file.bin", strings.Repeat("z", 100_000))
	for i := range 1500 {
		add(fmt.Sprintf("big/f%04d.dat", i), strings.Repeat("b", i%7))
	}
	for _, d := range []string{"empty", "d i r", "deep", "deep/er", "deep/er/still", "big"} {
		os.MkdirAll(filepath.Join(root, d), 0o755)
		want = append(want, d+"/")
	}
	if err := os.Symlink(".", filepath.Join(root, "loop")); err != nil {
		t.Skip("no symbolic links here:", err)
	}
	if err := os.Symlink("..", filepath.Join(root, "deep", "up")); err != nil {
		t.Fatal(err)
	}
	slices.Sort(want)
	return root, want
}

// webWalk lists the tree under b, as a scan does: a recursive listing,
// then each folder. Folders that fail are given by their path.
func webWalk(t *testing.T, b model.Backend) (rows []string, failed map[string]error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	b.(model.RecursivePreloader).PreloadRecursive(ctx, "")
	failed = map[string]error{}
	var walk func(dir string)
	walk = func(dir string) {
		entries, err := b.List(ctx, dir)
		if err != nil {
			failed[dir] = err
			return
		}
		for _, e := range entries {
			if e.IsDir {
				rows = append(rows, e.RelPath+"/")
				walk(e.RelPath)
				continue
			}
			if target, ok := e.Target(); ok {
				rows = append(rows, e.RelPath+" -> "+target)
				continue
			}
			rows = append(rows, fmt.Sprintf("%s %d", e.RelPath, e.Size))
			if !e.ModTime.Equal(webTime) || e.TimeRes != model.TimeSecond {
				t.Errorf("%s: %v (%v), want %v to the second", e.RelPath, e.ModTime, e.TimeRes, webTime)
			}
		}
	}
	walk("")
	slices.Sort(rows)
	return rows, failed
}

// checkWebWalk checks a walk of the tree e2eWebTree made: every entry,
// with the links back up shown as links, and no folder that fails.
func checkWebWalk(t *testing.T, server string, b model.Backend, want []string) {
	t.Helper()
	start := time.Now()
	rows, failed := webWalk(t, b)
	want = append(slices.Clone(want), "loop -> .", "deep/up -> ..")
	slices.Sort(want)
	if !slices.Equal(rows, want) {
		t.Errorf("%s: the listing is not the tree", server)
		for _, r := range rows {
			if !slices.Contains(want, r) {
				t.Errorf("  more: %q", r)
			}
		}
		for _, r := range want {
			if !slices.Contains(rows, r) {
				t.Errorf("  missing: %q", r)
			}
		}
	}
	for dir, err := range failed {
		t.Errorf("%s: %s: %v", server, dir, err)
	}
	t.Logf("%s: %d entries in %v", server, len(rows), time.Since(start).Round(time.Millisecond))

	rc, err := b.(model.SeekableOpener).OpenAt(context.Background(), "deep/er/still/file.bin", 99_990)
	if err != nil {
		t.Fatalf("%s: OpenAt: %v", server, err)
	}
	defer rc.Close()
	if got, err := io.ReadAll(rc); err != nil || string(got) != strings.Repeat("z", 10) {
		t.Errorf("%s: OpenAt = %q, %v", server, got, err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startServer runs cmd until the test ends, and waits for port to answer.
// The server gets a process group of its own: Apache stops by sending
// SIGTERM to its whole group, which was the test's and its shell's. The
// end is a SIGTERM to that group, children and all.
func startServer(t *testing.T, cmd *exec.Cmd, port int) {
	t.Helper()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s never answered on port %d", cmd.Path, port)
}

// apacheModules finds the folder of Apache's modules.
func apacheModules() (bin, mods string) {
	for _, c := range []struct{ bin, mods string }{
		{"/usr/sbin/httpd", "/usr/libexec/apache2"},
		{"/usr/sbin/apache2", "/usr/lib/apache2/modules"},
		{"/usr/sbin/httpd", "/usr/lib64/httpd/modules"},
	} {
		if _, err := os.Stat(path.Join(c.mods, "mod_autoindex.so")); err == nil {
			if _, err := os.Stat(c.bin); err == nil {
				return c.bin, c.mods
			}
		}
	}
	return "", ""
}

func TestHTTPApacheAutoindexE2E(t *testing.T) {
	bin, mods := apacheModules()
	if bin == "" {
		t.Skip("no Apache with mod_autoindex")
	}
	root, want := e2eWebTree(t)
	for _, options := range []string{"FancyIndexing HTMLTable VersionSort", "FancyIndexing IconsAreLinks", ""} {
		t.Run(strings.ReplaceAll(cmpOr(options, "plain"), " ", "_"), func(t *testing.T) {
			dir := t.TempDir()
			port := freePort(t)
			indexOptions := ""
			if options != "" {
				indexOptions = "IndexOptions " + options
			}
			conf := fmt.Sprintf(`ServerRoot "%s"
ServerName 127.0.0.1
Listen 127.0.0.1:%d
LoadModule mpm_prefork_module %[3]s/mod_mpm_prefork.so
LoadModule authz_core_module %[3]s/mod_authz_core.so
LoadModule unixd_module %[3]s/mod_unixd.so
LoadModule mime_module %[3]s/mod_mime.so
LoadModule dir_module %[3]s/mod_dir.so
LoadModule autoindex_module %[3]s/mod_autoindex.so
PidFile "%[1]s/httpd.pid"
DefaultRuntimeDir "%[1]s"
ErrorLog "%[1]s/error.log"
TypesConfig /dev/null
StartServers 10
MinSpareServers 10
MaxSpareServers 20
KeepAlive On
DocumentRoot "%[4]s"
<Directory "%[4]s">
    Options Indexes FollowSymLinks
    AllowOverride None
    Require all granted
    %[5]s
</Directory>
`, dir, port, mods, root, indexOptions)
			confPath := filepath.Join(dir, "httpd.conf")
			if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
				t.Fatal(err)
			}
			startServer(t, exec.Command(bin, "-D", "FOREGROUND", "-f", confPath), port)
			b, err := TryOpenBackend(fmt.Sprintf("http://127.0.0.1:%d/", port), false, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			checkWebWalk(t, "apache "+options, b, want)
		})
	}
}

// cmpOr is s, or def when s is empty.
func cmpOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func TestHTTPPythonServerE2E(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("no python3")
	}
	root, want := e2eWebTree(t)
	port := freePort(t)
	cmd := exec.Command(py, "-m", "http.server", "--bind", "127.0.0.1", "--directory", root, fmt.Sprint(port))
	startServer(t, cmd, port)
	b, err := TryOpenBackend(fmt.Sprintf("http://127.0.0.1:%d/", port), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	checkWebWalk(t, "python", b, want)
}
