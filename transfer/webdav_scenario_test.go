package transfer

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sc/model"
	"sc/transport"
)

// startOneLevelWebDAV serves root with rclone behind a proxy that answers
// Depth: infinity with one level, as sabre/dav does by default.
func startOneLevelWebDAV(t *testing.T, root string) string {
	t.Helper()
	bin, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone not available")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, "serve", "webdav", root, "--addr", fmt.Sprintf("127.0.0.1:%d", port), "--user", "u", "--pass", "p")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	for i := 0; ; i++ {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			break
		}
		if i == 200 {
			t.Fatal("rclone webdav did not come up")
		}
		time.Sleep(50 * time.Millisecond)
	}
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	proxy := httptest.NewServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		if strings.EqualFold(r.In.Header.Get("Depth"), "infinity") {
			r.Out.Header.Set("Depth", "1")
		}
	}})
	t.Cleanup(proxy.Close)
	return fmt.Sprintf("webdav://u:p@%s/", proxy.Listener.Addr())
}

// W-C1, end to end: against a server that answers a deep listing with one
// level, the folders below are not taken as empty, so a mirror from the
// server to an identical local copy deletes nothing.
func TestScenarioWebDAVOneLevelAnswerMirror(t *testing.T) {
	remote, local := t.TempDir(), t.TempDir()
	files := []string{"docs/plan.txt", "docs/report.txt", "photos/a.jpg", "photos/2019/b.jpg", "top.txt"}
	for _, side := range []string{remote, local} {
		for _, f := range files {
			writeFile(t, filepath.Join(side, f), "same on both sides: "+f)
		}
	}
	src, err := transport.TryOpenBackend(startOneLevelWebDAV(t, remote), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	dst := transport.NewLocalBackend(local)
	sc := model.NewScanner(src, dst, 4, 8, true)
	sc.Scan(context.Background(), scenarioOpts)
	s := &scenario{right: local, lb: src, rb: dst, sc: sc}
	if p := s.copy(t, "", true, true); p.Failed.Load() != 0 {
		t.Errorf("failed=%d", p.Failed.Load())
	}
	wantTree(t, "local side after a mirror from the server", local,
		"docs/", "docs/plan.txt", "docs/report.txt", "photos/", "photos/2019/", "photos/2019/b.jpg", "photos/a.jpg", "top.txt")
	for _, f := range files {
		if got := readFile(t, filepath.Join(local, f)); got != "same on both sides: "+f {
			t.Errorf("%s = %q", f, got)
		}
	}
}
