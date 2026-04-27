package transport

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"
	"github.com/gokrazy/rsync/rsynccmd"

	"sc/model"
)

type RsyncBackend struct {
	host        string
	user        string
	pass        string
	module      string
	base        string
	display     string
	useChecksum bool
	cksumAlgo   string
	md4mu       sync.Mutex
	md4cache    map[string]string
}

func NewRsyncBackend(rawURL string) (*RsyncBackend, error) {
	_, user, pass, host, port, remotePath := parseRemoteURL(rawURL)
	if port == "" {
		port = "873"
	}

	p := strings.TrimPrefix(remotePath, "/")
	parts := strings.SplitN(p, "/", 2)
	module := parts[0]
	if module == "" {
		return nil, fmt.Errorf("rsync: module name required in URL")
	}
	base := ""
	if len(parts) > 1 {
		base = parts[1]
	}

	displayHost := host
	if port != "873" {
		displayHost = net.JoinHostPort(host, port)
	}
	display := "rsync://" + displayHost + "/" + module
	if base != "" {
		display += "/" + base
	}

	return &RsyncBackend{
		host:    net.JoinHostPort(host, port),
		user:    user,
		pass:    pass,
		module:  module,
		base:    base,
		display: display,
	}, nil
}

func (b *RsyncBackend) BasePath() string { return b.display }

func (b *RsyncBackend) OwnsCopyProgress() bool { return true }

func (b *RsyncBackend) remoteURL(relPath string) string {
	p := b.module
	if b.base != "" {
		p = p + "/" + b.base
	}
	if relPath != "" {
		p = p + "/" + relPath
	}
	hostPart := b.host
	if b.user != "" {
		userinfo := url.PathEscape(b.user)
		if b.pass != "" {
			userinfo += ":" + url.PathEscape(b.pass)
		}
		hostPart = userinfo + "@" + b.host
	}
	return "rsync://" + hostPart + "/" + p
}

func (b *RsyncBackend) rsyncRun(ctx context.Context, args ...string) (string, error) {
	cmd := rsynccmd.Command("rsync", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	_, err := cmd.Run(ctx)
	if err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg != "" {
			return "", fmt.Errorf("%v: %s", err, errMsg)
		}
		return "", err
	}
	return stdout.String(), nil
}

func (b *RsyncBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	u := b.remoteURL(relDir) + "/"
	Log.Add("rsync", ">>>", "LIST "+u)
	args := []string{}
	if b.useChecksum {
		args = append(args, "-c")
	}
	args = append(args, u)
	out, err := b.rsyncRun(ctx, args...)
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
		return nil, err
	}

	var entries []model.FileEntry
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		entry, ok := parseRsyncListLine(line, relDir)
		if !ok || entry.Name == "." || entry.Name == ".." {
			continue
		}
		entries = append(entries, entry)
	}
	Log.Add("rsync", "<<<", fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

func parseRsyncListLine(line, relDir string) (model.FileEntry, bool) {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return model.FileEntry{}, false
	}
	modeStr := fields[0]
	size, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return model.FileEntry{}, false
	}
	modTime, err := time.ParseInLocation("2006/01/02 15:04:05", fields[2]+" "+fields[3], time.Local)
	if err != nil {
		return model.FileEntry{}, false
	}
	nameIdx := strings.Index(line, fields[3]) + len(fields[3])
	if nameIdx >= len(line) {
		return model.FileEntry{}, false
	}
	name := strings.TrimLeft(line[nameIdx:], " ")
	if len(modeStr) > 0 && modeStr[0] == 'l' {
		return model.FileEntry{}, false
	}
	isDir := len(modeStr) > 0 && modeStr[0] == 'd'

	return model.FileEntry{
		RelPath: path.Join(relDir, name),
		Name:    name,
		Size:    size,
		ModTime: modTime,
		IsDir:   isDir,
		Mode:    parseRsyncMode(modeStr),
	}, true
}

func parseRsyncMode(s string) os.FileMode {
	if len(s) < 10 {
		return 0644
	}
	var mode os.FileMode
	if s[0] == 'd' {
		mode |= os.ModeDir
	}
	if s[0] == 'l' {
		mode |= os.ModeSymlink
	}
	bits := [9]os.FileMode{0400, 0200, 0100, 0040, 0020, 0010, 0004, 0002, 0001}
	for i, b := range bits {
		if s[1+i] != '-' {
			mode |= b
		}
	}
	return mode
}

func (b *RsyncBackend) ProbeChecksums() []string {
	return []string{"md4", "rsync"}
}

func (b *RsyncBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
	b.useChecksum = algo == "rsync" || algo == "md4"
}

func (b *RsyncBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "md4" {
		b.md4mu.Lock()
		sum, ok := b.md4cache[relPath]
		b.md4mu.Unlock()
		if ok {
			return sum, nil
		}
		got, err := b.fetchMD4(ctx, relPath, false)
		if err != nil {
			return "", err
		}
		sum, ok = got[relPath]
		if !ok {
			return "", fmt.Errorf("md4: no checksum for %s", relPath)
		}
		return sum, nil
	}
	if !b.useChecksum {
		return "", fmt.Errorf("rsync: checksum not enabled")
	}
	return "rsync_internal", nil
}

func (b *RsyncBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	if b.cksumAlgo != "md4" {
		return nil
	}
	_, err := b.fetchMD4(ctx, scope, recursive)
	return err
}

func (b *RsyncBackend) fetchMD4(ctx context.Context, scope string, recursive bool) (map[string]string, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-md4-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	args := []string{"-c"}
	if recursive {
		args = append(args, "-r")
	}
	client, err := rsyncclient.New(args, rsyncclient.WithStderr(io.Discard), rsyncclient.WithoutNegotiate(), rsyncclient.DontRestrict())
	if err != nil {
		return nil, err
	}

	remotePath := b.module + "/"
	if b.base != "" {
		remotePath += b.base + "/"
	}
	if scope != "" {
		remotePath += scope
		if recursive {
			remotePath += "/"
		}
	}

	if b.user != "" {
		os.Setenv("RSYNC_USERNAME", b.user)
	}
	if b.pass != "" {
		os.Setenv("RSYNC_PASSWORD", b.pass)
	}

	conn, err := net.DialTimeout("tcp", b.host, 30*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	Log.Add("rsync", ">>>", "MD4 "+remotePath)
	result, err := client.RunDaemon(ctx, conn, remotePath, []string{tmpDir + "/"})
	if err != nil {
		return nil, err
	}

	prefix := ""
	if recursive {
		prefix = scope
	} else if scope != "" {
		prefix = path.Dir(scope)
		if prefix == "." {
			prefix = ""
		}
	}

	got := make(map[string]string, len(result.FileList))
	var zero [16]byte
	for _, fi := range result.FileList {
		if fi.Checksum == zero {
			continue
		}
		key := fi.Name
		if prefix != "" {
			key = path.Join(prefix, fi.Name)
		}
		got[key] = hex.EncodeToString(fi.Checksum[:])
	}
	Log.Add("rsync", "<<<", fmt.Sprintf("MD4 %d checksums", len(got)))

	b.md4mu.Lock()
	if b.md4cache == nil {
		b.md4cache = make(map[string]string, len(got))
	}
	for k, v := range got {
		b.md4cache[k] = v
	}
	b.md4mu.Unlock()
	return got, nil
}

func (b *RsyncBackend) SetTimes(_ context.Context, _ string, _, _, _ time.Time) error {
	return fmt.Errorf("rsync: set times not supported")
}

func (b *RsyncBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	tmpDir, err := os.MkdirTemp("", "rsync-upload-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	tmpFile := filepath.Join(tmpDir, filepath.Base(relPath))
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	counter := progressFromContext(ctx)
	fileSize := fileSizeFromContext(ctx)
	var copyDst io.Writer = f
	if counter != nil && fileSize > 0 && !IsPreCounted(src) {
		copyDst = &CountingWriter{W: f, Adder: NewCappedAdder(counter, fileSize)}
	}
	if _, err := io.Copy(copyDst, src); err != nil {
		f.Close()
		return err
	}
	f.Close()

	dest := b.remoteURL(path.Dir(relPath)) + "/"
	Log.Add("rsync", ">>>", "SEND "+relPath)
	sendArgs := []string{"-t"}
	if b.useChecksum {
		sendArgs = append(sendArgs, "-c")
	}
	sendArgs = append(sendArgs, tmpFile, dest)
	_, err = b.rsyncRun(ctx, sendArgs...)
	if err != nil {
		Log.Add("rsync", "ERR", err.Error())
		return err
	}
	Log.Add("rsync", "<<<", "OK")
	return nil
}

func (b *RsyncBackend) Rename(_ context.Context, _, _ string) error {
	return fmt.Errorf("rsync: rename not supported")
}

func (b *RsyncBackend) Remove(_ context.Context, _ string) error {
	return fmt.Errorf("rsync: remove not supported")
}

func (b *RsyncBackend) RemoveAll(_ context.Context, _ string) error {
	return fmt.Errorf("rsync: remove not supported")
}

func (b *RsyncBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-download-*")
	if err != nil {
		return nil, err
	}

	counter := progressFromContext(ctx)
	var stop chan struct{}
	if counter != nil {
		size := fileSizeFromContext(ctx)
		if size <= 0 {
			size = 1 << 62
		}
		stop = make(chan struct{})
		go tailDirSize(stop, tmpDir, filepath.Base(relPath), NewCappedAdder(counter, size))
	}

	u := b.remoteURL(relPath)
	Log.Add("rsync", ">>>", "RECV "+relPath)
	recvArgs := []string{}
	if b.useChecksum {
		recvArgs = append(recvArgs, "-c")
	}
	recvArgs = append(recvArgs, u, tmpDir+"/")
	_, err = b.rsyncRun(ctx, recvArgs...)
	if stop != nil {
		close(stop)
	}
	if err != nil {
		os.RemoveAll(tmpDir)
		Log.Add("rsync", "ERR", err.Error())
		return nil, err
	}
	Log.Add("rsync", "<<<", "OK")

	f, err := os.Open(filepath.Join(tmpDir, filepath.Base(relPath)))
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	rc := &tempReadCloser{File: f, tmpDir: tmpDir}
	if counter != nil {
		return WrapPreCounted(rc), nil
	}
	return rc, nil
}

type tempReadCloser struct {
	*os.File
	tmpDir string
}

func (t *tempReadCloser) Close() error {
	t.File.Close()
	return os.RemoveAll(t.tmpDir)
}
