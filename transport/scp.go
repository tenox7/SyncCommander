package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"sc/model"
)

type SCPBackend struct {
	base      string
	client    *ssh.Client
	display   string
	cksumAlgo string
	cksumCmds map[string]string
}

func NewSCPBackend(rawURL string) (*SCPBackend, error) {
	conn, err := dialSSH(rawURL)
	if err != nil {
		return nil, err
	}

	b := &SCPBackend{client: conn.client}

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
	b.display = sshDisplayURL(conn, remotePath)

	return b, nil
}

func (b *SCPBackend) BasePath() string { return b.display }

func (b *SCPBackend) Close() error { return b.client.Close() }

func (b *SCPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
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

func (b *SCPBackend) Checksum(_ context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "" {
		return "", fmt.Errorf("no checksum algorithm configured")
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

func (b *SCPBackend) ProbeChecksums() []string {
	if b.cksumCmds == nil {
		run := func(cmd string) (string, error) { return b.sshRun(cmd) }
		var algos []string
		algos, b.cksumCmds = probeSSHChecksums(run)
		return algos
	}
	var algos []string
	seen := make(map[string]bool)
	for _, p := range cksumProbes {
		if _, ok := b.cksumCmds[p.algo]; ok && !seen[p.algo] {
			algos = append(algos, p.algo)
			seen[p.algo] = true
		}
	}
	return algos
}

func (b *SCPBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
}

func (b *SCPBackend) SetTimes(_ context.Context, relPath string, mtime, atime, _ time.Time) error {
	fp := shellQuote(path.Join(b.base, relPath))
	cmd := fmt.Sprintf("touch -m -d @%d.%09d %s && touch -a -d @%d.%09d %s",
		mtime.Unix(), mtime.Nanosecond(), fp,
		atime.Unix(), atime.Nanosecond(), fp)
	_, err := b.sshRun(cmd)
	return err
}

func (b *SCPBackend) CopyFrom(_ context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	fullPath := path.Join(b.base, relPath)
	b.sshRun(fmt.Sprintf("mkdir -p %s", shellQuote(path.Dir(fullPath))))

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	session.Stdin = src
	return session.Run(fmt.Sprintf("cat > %s && chmod %04o %s",
		shellQuote(fullPath), mode.Perm(), shellQuote(fullPath)))
}

func (b *SCPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	_, err := b.sshRun(fmt.Sprintf("mv %s %s",
		shellQuote(path.Join(b.base, oldRelPath)),
		shellQuote(path.Join(b.base, newRelPath))))
	return err
}

func (b *SCPBackend) Remove(_ context.Context, relPath string) error {
	_, err := b.sshRun(fmt.Sprintf("rm %s", shellQuote(path.Join(b.base, relPath))))
	return err
}

func (b *SCPBackend) RemoveAll(_ context.Context, relPath string) error {
	_, err := b.sshRun(fmt.Sprintf("rm -rf %s", shellQuote(path.Join(b.base, relPath))))
	return err
}

func (b *SCPBackend) Open(_ context.Context, relPath string) (io.ReadCloser, error) {
	session, err := b.client.NewSession()
	if err != nil {
		return nil, err
	}
	rd, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		return nil, err
	}
	if err := session.Start(fmt.Sprintf("cat %s", shellQuote(path.Join(b.base, relPath)))); err != nil {
		session.Close()
		return nil, err
	}
	return &sshReadCloser{session: session, Reader: rd}, nil
}

func (b *SCPBackend) sshRun(cmd string) (string, error) {
	Log.Add("scp", ">>>", cmd)
	session, err := b.client.NewSession()
	if err != nil {
		Log.Add("scp", "ERR", err.Error())
		return "", err
	}
	defer session.Close()
	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if err := session.Run(cmd); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			Log.Add("scp", "ERR", msg)
			return "", fmt.Errorf("%s", msg)
		}
		Log.Add("scp", "ERR", err.Error())
		return "", err
	}
	out := stdout.String()
	if out != "" {
		Log.Add("scp", "<<<", strings.TrimRight(out, "\n"))
	}
	return out, nil
}

type sshReadCloser struct {
	session *ssh.Session
	io.Reader
}

func (r *sshReadCloser) Close() error {
	return r.session.Close()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func parseEpoch(s string) time.Time {
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		sec, _ := strconv.ParseInt(s, 10, 64)
		return time.Unix(sec, 0)
	}
	sec, _ := strconv.ParseInt(s[:dot], 10, 64)
	frac := s[dot+1:]
	for len(frac) < 9 {
		frac += "0"
	}
	nsec, _ := strconv.ParseInt(frac[:9], 10, 64)
	return time.Unix(sec, nsec)
}
