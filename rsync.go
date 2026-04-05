package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gokrazy/rsync/rsynccmd"
)

type RsyncBackend struct {
	host        string
	user        string
	pass        string
	module      string
	base        string
	display     string
	useChecksum bool
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

func (b *RsyncBackend) List(ctx context.Context, relDir string) ([]FileEntry, error) {
	url := b.remoteURL(relDir) + "/"
	remoteLog.Add("rsync", ">>>", "LIST "+url)
	args := []string{}
	if b.useChecksum {
		args = append(args, "-c")
	}
	args = append(args, url)
	out, err := b.rsyncRun(ctx, args...)
	if err != nil {
		remoteLog.Add("rsync", "ERR", err.Error())
		return nil, err
	}

	var entries []FileEntry
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
	remoteLog.Add("rsync", "<<<", fmt.Sprintf("%d entries", len(entries)))
	return entries, nil
}

func parseRsyncListLine(line, relDir string) (FileEntry, bool) {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return FileEntry{}, false
	}
	modeStr := fields[0]
	size, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return FileEntry{}, false
	}
	modTime, err := time.ParseInLocation("2006/01/02 15:04:05", fields[2]+" "+fields[3], time.Local)
	if err != nil {
		return FileEntry{}, false
	}
	nameIdx := strings.Index(line, fields[3]) + len(fields[3])
	if nameIdx >= len(line) {
		return FileEntry{}, false
	}
	name := strings.TrimLeft(line[nameIdx:], " ")
	if len(modeStr) > 0 && modeStr[0] == 'l' {
		return FileEntry{}, false
	}
	isDir := len(modeStr) > 0 && modeStr[0] == 'd'

	return FileEntry{
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

func (b *RsyncBackend) probeChecksums() []string {
	return []string{"rsync"}
}

func (b *RsyncBackend) setChecksumAlgo(algo string) {
	b.useChecksum = algo == "rsync"
}

func (b *RsyncBackend) Checksum(_ context.Context, _ string) (string, error) {
	if !b.useChecksum {
		return "", fmt.Errorf("rsync: checksum not enabled")
	}
	return "rsync_internal", nil
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
	if _, err := io.Copy(f, src); err != nil {
		f.Close()
		return err
	}
	f.Close()

	dest := b.remoteURL(path.Dir(relPath)) + "/"
	remoteLog.Add("rsync", ">>>", "SEND "+relPath)
	sendArgs := []string{"-t"}
	if b.useChecksum {
		sendArgs = append(sendArgs, "-c")
	}
	sendArgs = append(sendArgs, tmpFile, dest)
	_, err = b.rsyncRun(ctx, sendArgs...)
	if err != nil {
		remoteLog.Add("rsync", "ERR", err.Error())
		return err
	}
	remoteLog.Add("rsync", "<<<", "OK")
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

	url := b.remoteURL(relPath)
	remoteLog.Add("rsync", ">>>", "RECV "+relPath)
	recvArgs := []string{}
	if b.useChecksum {
		recvArgs = append(recvArgs, "-c")
	}
	recvArgs = append(recvArgs, url, tmpDir+"/")
	_, err = b.rsyncRun(ctx, recvArgs...)
	if err != nil {
		os.RemoveAll(tmpDir)
		remoteLog.Add("rsync", "ERR", err.Error())
		return nil, err
	}
	remoteLog.Add("rsync", "<<<", "OK")

	f, err := os.Open(filepath.Join(tmpDir, filepath.Base(relPath)))
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	return &tempReadCloser{File: f, tmpDir: tmpDir}, nil
}

type tempReadCloser struct {
	*os.File
	tmpDir string
}

func (t *tempReadCloser) Close() error {
	t.File.Close()
	return os.RemoveAll(t.tmpDir)
}
