package transport

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gokrazy/rsync/rsyncclient"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

type RsyncSSHBackend struct {
	base      string
	client    *ssh.Client
	display   string
	cksumAlgo string
	cksumCmds map[string]string
	md4mu     sync.Mutex
	md4cache  map[string]string
}

func NewRsyncSSHBackend(rawURL string) (*RsyncSSHBackend, error) {
	conn, err := dialSSH(rawURL)
	if err != nil {
		return nil, err
	}

	b := &RsyncSSHBackend{client: conn.client}

	remotePath := conn.basePath
	switch {
	case remotePath == "" || remotePath == "/" || remotePath == "/~":
		if out, err := b.sshRun("echo $HOME"); err == nil {
			remotePath = strings.TrimSpace(out)
		} else {
			remotePath = "/"
		}
	case strings.HasPrefix(remotePath, "/~/"):
		if out, err := b.sshRun("echo $HOME"); err == nil {
			remotePath = path.Join(strings.TrimSpace(out), remotePath[3:])
		}
	}
	b.base = remotePath

	if _, err := b.sshRun("which rsync"); err != nil {
		conn.client.Close()
		return nil, fmt.Errorf("rsync+ssh: remote rsync not found")
	}

	display := sshDisplayURL(conn, remotePath)
	b.display = "rsync+" + display

	return b, nil
}

func (b *RsyncSSHBackend) BasePath() string { return b.display }
func (b *RsyncSSHBackend) Close() error     { return b.client.Close() }

func (b *RsyncSSHBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := shellQuote(path.Join(b.base, relDir))
	cmd := fmt.Sprintf("find %s -maxdepth 1 -mindepth 1 -printf '%%f\\t%%s\\t%%T@\\t%%A@\\t%%C@\\t%%m\\t%%y\\n'", dir)
	out, err := b.sshRun(cmd)
	if err != nil {
		return nil, err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return nil, nil
	}
	var result []model.FileEntry
	for _, line := range strings.Split(out, "\n") {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		entry, ok := parseFindLine(line, relDir)
		if !ok {
			continue
		}
		result = append(result, entry)
	}
	return result, nil
}

func (b *RsyncSSHBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "" {
		return "", fmt.Errorf("no checksum algorithm configured")
	}
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
	cmd := fmt.Sprintf("%s %s", b.cksumCmds[b.cksumAlgo], shellQuote(path.Join(b.base, relPath)))
	out, err := b.sshRun(cmd)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum output")
	}
	return fields[0], nil
}

func (b *RsyncSSHBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	if b.cksumAlgo != "md4" {
		return nil
	}
	_, err := b.fetchMD4(ctx, scope, recursive)
	return err
}

func (b *RsyncSSHBackend) fetchMD4(ctx context.Context, scope string, recursive bool) (map[string]string, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-md4-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	remotePath := b.base
	if scope != "" {
		remotePath = path.Join(remotePath, scope)
	}
	if recursive {
		remotePath += "/"
	}

	args := []string{"-c"}
	if recursive {
		args = append(args, "-r")
	}
	client, err := rsyncclient.New(args, rsyncclient.WithStderr(io.Discard), rsyncclient.DontRestrict())
	if err != nil {
		return nil, err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remotePath))
	Log.Add("rsync+ssh", ">>>", "MD4 "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		return nil, err
	}

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		session.Close()
		return nil, err
	}

	if err := session.Start(serverCmd); err != nil {
		session.Close()
		return nil, err
	}

	result, err := client.Run(ctx, rw, []string{tmpDir + "/"})
	session.Close()
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
	Log.Add("rsync+ssh", "<<<", fmt.Sprintf("MD4 %d checksums", len(got)))

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

func (b *RsyncSSHBackend) ProbeChecksums() []string {
	if b.cksumCmds == nil {
		run := func(cmd string) (string, error) { return b.sshRun(cmd) }
		var algos []string
		algos, b.cksumCmds = probeSSHChecksums(run)
		return append(algos, "md4")
	}
	var algos []string
	seen := make(map[string]bool)
	for _, p := range cksumProbes {
		if _, ok := b.cksumCmds[p.algo]; ok && !seen[p.algo] {
			algos = append(algos, p.algo)
			seen[p.algo] = true
		}
	}
	return append(algos, "md4")
}

func (b *RsyncSSHBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
}

func (b *RsyncSSHBackend) SetTimes(_ context.Context, relPath string, mtime, atime, _ time.Time) error {
	fp := shellQuote(path.Join(b.base, relPath))
	cmd := fmt.Sprintf("touch -m -d @%d.%09d %s && touch -a -d @%d.%09d %s",
		mtime.Unix(), mtime.Nanosecond(), fp,
		atime.Unix(), atime.Nanosecond(), fp)
	_, err := b.sshRun(cmd)
	return err
}

func (b *RsyncSSHBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	_, err := b.sshRun(fmt.Sprintf("mv %s %s",
		shellQuote(path.Join(b.base, oldRelPath)),
		shellQuote(path.Join(b.base, newRelPath))))
	return err
}

func (b *RsyncSSHBackend) Remove(_ context.Context, relPath string) error {
	_, err := b.sshRun(fmt.Sprintf("rm %s", shellQuote(path.Join(b.base, relPath))))
	return err
}

func (b *RsyncSSHBackend) RemoveAll(_ context.Context, relPath string) error {
	_, err := b.sshRun(fmt.Sprintf("rm -rf %s", shellQuote(path.Join(b.base, relPath))))
	return err
}

func (b *RsyncSSHBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-ssh-dl-*")
	if err != nil {
		return nil, err
	}

	remotePath := path.Join(b.base, relPath)
	client, err := rsyncclient.New(nil, rsyncclient.WithStderr(io.Discard))
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remotePath))
	Log.Add("rsync+ssh", ">>>", serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		session.Close()
		os.RemoveAll(tmpDir)
		return nil, err
	}

	if err := session.Start(serverCmd); err != nil {
		session.Close()
		os.RemoveAll(tmpDir)
		Log.Add("rsync+ssh", "ERR", err.Error())
		return nil, err
	}

	_, err = client.Run(ctx, rw, []string{tmpDir + "/"})
	session.Close()
	if err != nil {
		os.RemoveAll(tmpDir)
		Log.Add("rsync+ssh", "ERR", err.Error())
		return nil, err
	}
	Log.Add("rsync+ssh", "<<<", "OK")

	f, err := os.Open(filepath.Join(tmpDir, filepath.Base(relPath)))
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	return &tempReadCloser{File: f, tmpDir: tmpDir}, nil
}

func (b *RsyncSSHBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	tmpDir, err := os.MkdirTemp("", "rsync-ssh-ul-*")
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

	remoteDest := path.Join(b.base, path.Dir(relPath))
	b.sshRun(fmt.Sprintf("mkdir -p %s", shellQuote(remoteDest)))

	client, err := rsyncclient.New([]string{"-t"}, rsyncclient.WithSender(), rsyncclient.WithStderr(io.Discard))
	if err != nil {
		return err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remoteDest + "/"))
	Log.Add("rsync+ssh", ">>>", "SEND "+relPath+" via "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		session.Close()
		return err
	}

	if err := session.Start(serverCmd); err != nil {
		session.Close()
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}

	_, err = client.Run(ctx, rw, []string{tmpFile})
	session.Close()
	if err != nil {
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}
	Log.Add("rsync+ssh", "<<<", "OK")
	return nil
}

func (b *RsyncSSHBackend) buildServerCmd(args []string) string {
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellQuote(a)
	}
	return "rsync " + strings.Join(quoted, " ")
}

func (b *RsyncSSHBackend) sessionReadWriter(session *ssh.Session) (io.ReadWriter, error) {
	stdin, err := session.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Writer
	}{stdout, stdin}, nil
}

func (b *RsyncSSHBackend) sshRun(cmd string) (string, error) {
	Log.Add("rsync+ssh", ">>>", cmd)
	session, err := b.client.NewSession()
	if err != nil {
		Log.Add("rsync+ssh", "ERR", err.Error())
		return "", err
	}
	defer session.Close()
	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if err := session.Run(cmd); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			Log.Add("rsync+ssh", "ERR", msg)
			return "", fmt.Errorf("%s", msg)
		}
		Log.Add("rsync+ssh", "ERR", err.Error())
		return "", err
	}
	out := stdout.String()
	if out != "" {
		Log.Add("rsync+ssh", "<<<", strings.TrimRight(out, "\n"))
	}
	return out, nil
}

func parseFindLine(line, relDir string) (model.FileEntry, bool) {
	fields := strings.SplitN(line, "\t", 7)
	if len(fields) < 7 {
		return model.FileEntry{}, false
	}
	if fields[6] == "l" {
		return model.FileEntry{}, false
	}
	size, _ := strconv.ParseInt(fields[1], 10, 64)
	modeVal, _ := strconv.ParseUint(fields[5], 8, 32)
	isDir := fields[6] == "d"
	mode := os.FileMode(modeVal)
	if isDir {
		mode |= os.ModeDir
	}
	return model.FileEntry{
		RelPath: path.Join(relDir, fields[0]),
		Name:    fields[0],
		Size:    size,
		ModTime: parseEpoch(fields[2]),
		ATime:   parseEpoch(fields[3]),
		CTime:   parseEpoch(fields[4]),
		IsDir:   isDir,
		Mode:    mode,
	}, true
}
