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
	listCache *rsyncListCache
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
	if b.listCache != nil {
		entries, hit, active, done := b.listCache.lookup(relDir)
		if hit {
			return entries, nil
		}
		if active && !done {
			if e, ok := b.listCache.await(relDir); ok {
				return e, nil
			}
		}
	}
	return b.liveList(ctx, relDir)
}

func (b *SCPBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := shellQuote(path.Join(b.base, relDir))
	cmd := fmt.Sprintf("find %s -maxdepth 1 -mindepth 1 -printf '%%f\\t%%s\\t%%T@\\t%%A@\\t%%C@\\t%%m\\t%%y\\n'", dir)
	out, err := b.sshRunCtx(ctx, cmd)
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

func (b *SCPBackend) PreloadRecursive(ctx context.Context, scope string) error {
	if b.listCache == nil {
		b.listCache = newRsyncListCache()
	}
	c := b.listCache
	c.mu.Lock()
	if c.active && !c.done {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	c.reset(ctx, scope)

	go func() {
		_ = b.runRecursiveList(ctx, scope, c.emit)
		c.finish()
	}()
	return nil
}

func (b *SCPBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	absPrefix := path.Join(b.base, scope)
	dir := shellQuote(absPrefix)
	cmd := fmt.Sprintf("find %s -mindepth 1 -printf '%%p\\t%%s\\t%%T@\\t%%A@\\t%%C@\\t%%m\\t%%y\\n'", dir)
	Log.Add("scp", ">>>", "RLIST "+cmd)

	session, err := b.client.NewSession()
	if err != nil {
		Log.Add("scp", "ERR", "RLIST: "+err.Error())
		return err
	}
	defer session.Close()
	defer cancelCloser(ctx, session)()

	var current string
	var haveCurrent bool
	var batch []model.FileEntry
	var seenDirs []string
	flush := func() {
		if !haveCurrent {
			return
		}
		emit(current, batch)
		batch = nil
		haveCurrent = false
	}

	lw := newLineWriter(func(line string) {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			return
		}
		if i := strings.IndexByte(line, '\t'); i > 0 {
			rel := strings.TrimPrefix(line[:i], absPrefix)
			rel = strings.TrimPrefix(rel, "/")
			if rel == "" {
				return
			}
			line = rel + line[i:]
		}
		parent, entry, ok := parseFindRecursiveLine(line, scope)
		if !ok {
			return
		}
		if !haveCurrent || parent != current {
			flush()
			current = parent
			haveCurrent = true
		}
		batch = append(batch, entry)
		if entry.IsDir {
			seenDirs = append(seenDirs, entry.RelPath)
		}
	})

	var stderr bytes.Buffer
	session.Stdout = lw
	session.Stderr = &stderr
	runErr := session.Run(cmd)
	lw.flush()
	flush()
	for _, d := range seenDirs {
		emit(d, nil)
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			runErr = fmt.Errorf("%v: %s", runErr, msg)
		}
		Log.Add("scp", "ERR", "RLIST: "+runErr.Error())
		return runErr
	}
	Log.Add("scp", "<<<", "RLIST done")
	return nil
}

func parseFindRecursiveLine(line, scope string) (parent string, entry model.FileEntry, ok bool) {
	fields := strings.SplitN(line, "\t", 7)
	if len(fields) < 7 {
		return "", model.FileEntry{}, false
	}
	if fields[6] == "l" {
		return "", model.FileEntry{}, false
	}
	relFromScope := fields[0]
	if relFromScope == "" {
		return "", model.FileEntry{}, false
	}
	size, _ := strconv.ParseInt(fields[1], 10, 64)
	modeVal, _ := strconv.ParseUint(fields[5], 8, 32)
	isDir := fields[6] == "d"
	mode := os.FileMode(modeVal)
	if isDir {
		mode |= os.ModeDir
	}
	parentLocal := path.Dir(relFromScope)
	if parentLocal == "." {
		parentLocal = ""
	}
	return path.Join(scope, parentLocal), model.FileEntry{
		RelPath: path.Join(scope, relFromScope),
		Name:    path.Base(relFromScope),
		Size:    size,
		ModTime: parseEpoch(fields[2]),
		ATime:   parseEpoch(fields[3]),
		CTime:   parseEpoch(fields[4]),
		IsDir:   isDir,
		Mode:    mode,
	}, true
}

func (b *SCPBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "" {
		return "", fmt.Errorf("no checksum algorithm configured")
	}
	cmd := fmt.Sprintf("%s %s", b.cksumCmds[b.cksumAlgo], shellQuote(path.Join(b.base, relPath)))
	out, err := b.sshRunCtx(ctx, cmd)
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

func (b *SCPBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, _ time.Time) error {
	fp := shellQuote(path.Join(b.base, relPath))
	cmd := fmt.Sprintf("touch -m -d @%d.%09d %s && touch -a -d @%d.%09d %s",
		mtime.Unix(), mtime.Nanosecond(), fp,
		atime.Unix(), atime.Nanosecond(), fp)
	_, err := b.sshRunCtx(ctx, cmd)
	return err
}

func (b *SCPBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	fullPath := path.Join(b.base, relPath)
	b.sshRunCtx(ctx, fmt.Sprintf("mkdir -p %s", shellQuote(path.Dir(fullPath))))

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	defer cancelCloser(ctx, session)()
	session.Stdin = src
	return session.Run(fmt.Sprintf("cat > %s && chmod %04o %s",
		shellQuote(fullPath), mode.Perm(), shellQuote(fullPath)))
}

func (b *SCPBackend) AppendFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode, offset int64) error {
	fullPath := path.Join(b.base, relPath)
	b.sshRunCtx(ctx, fmt.Sprintf("mkdir -p %s", shellQuote(path.Dir(fullPath))))

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	defer cancelCloser(ctx, session)()
	session.Stdin = src
	cmd := fmt.Sprintf("truncate -s %d %s && cat >> %s && chmod %04o %s",
		offset, shellQuote(fullPath),
		shellQuote(fullPath),
		mode.Perm(), shellQuote(fullPath))
	Log.Add("scp", ">>>", cmd)
	return session.Run(cmd)
}

func (b *SCPBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	session, err := b.client.NewSession()
	if err != nil {
		return nil, err
	}
	rd, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		return nil, err
	}
	cmd := fmt.Sprintf("tail -c +%d %s", offset+1, shellQuote(path.Join(b.base, relPath)))
	Log.Add("scp", ">>>", cmd)
	if err := session.Start(cmd); err != nil {
		session.Close()
		return nil, err
	}
	stop := cancelCloser(ctx, session)
	return &sshReadCloser{session: session, Reader: rd, stop: stop}, nil
}

func (b *SCPBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	fullPath := path.Join(b.base, relPath)
	cmd := fmt.Sprintf("mkdir -p %s", shellQuote(fullPath))
	if mode != 0 {
		cmd = fmt.Sprintf("%s && chmod %04o %s", cmd, mode.Perm(), shellQuote(fullPath))
	}
	_, err := b.sshRunCtx(ctx, cmd)
	return err
}

func (b *SCPBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	_, err := b.sshRunCtx(ctx, fmt.Sprintf("mv %s %s",
		shellQuote(path.Join(b.base, oldRelPath)),
		shellQuote(path.Join(b.base, newRelPath))))
	return err
}

func (b *SCPBackend) Remove(ctx context.Context, relPath string) error {
	_, err := b.sshRunCtx(ctx, fmt.Sprintf("rm %s", shellQuote(path.Join(b.base, relPath))))
	return err
}

func (b *SCPBackend) RemoveAll(ctx context.Context, relPath string) error {
	_, err := b.sshRunCtx(ctx, fmt.Sprintf("rm -rf %s", shellQuote(path.Join(b.base, relPath))))
	return err
}

func (b *SCPBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
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
	stop := cancelCloser(ctx, session)
	return &sshReadCloser{session: session, Reader: rd, stop: stop}, nil
}

func (b *SCPBackend) sshRun(cmd string) (string, error) {
	return b.sshRunCtx(nil, cmd)
}

func (b *SCPBackend) sshRunCtx(ctx context.Context, cmd string) (string, error) {
	Log.Add("scp", ">>>", cmd)
	session, err := b.client.NewSession()
	if err != nil {
		Log.Add("scp", "ERR", err.Error())
		return "", err
	}
	defer session.Close()
	defer cancelCloser(ctx, session)()
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
	stop func()
}

func (r *sshReadCloser) Close() error {
	if r.stop != nil {
		r.stop()
	}
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
