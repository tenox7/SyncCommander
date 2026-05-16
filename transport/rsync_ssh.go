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

	rsyncpkg "github.com/gokrazy/rsync"
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
	listCache *rsyncListCache
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

	b.display = sshDisplayURL(conn, remotePath)

	return b, nil
}

func (b *RsyncSSHBackend) BasePath() string { return b.display }

func (b *RsyncSSHBackend) OwnsCopyProgress() bool { return true }
func (b *RsyncSSHBackend) Close() error           { return b.client.Close() }

func (b *RsyncSSHBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
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

func (b *RsyncSSHBackend) liveList(ctx context.Context, relDir string) ([]model.FileEntry, error) {
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

func (b *RsyncSSHBackend) PreloadRecursive(ctx context.Context, scope string) error {
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

func (b *RsyncSSHBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	remotePath := b.base
	if scope != "" {
		remotePath = path.Join(remotePath, scope)
	}
	remotePath += "/"

	client, err := rsyncclient.New([]string{"-n", "-r"},
		rsyncclient.WithStdout(io.Discard),
		rsyncclient.WithStderr(io.Discard),
		rsyncclient.DontRestrict())
	if err != nil {
		return err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remotePath))
	Log.Add("rsync+ssh", ">>>", "RLIST "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	defer cancelCloser(ctx, session)()

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		return err
	}

	if err := session.Start(serverCmd); err != nil {
		Log.Add("rsync+ssh", "ERR", "RLIST: "+err.Error())
		return err
	}

	tmpDir, err := os.MkdirTemp("", "rsync-rlist-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	result, err := client.Run(ctx, rw, []string{tmpDir + "/"})
	if err != nil {
		Log.Add("rsync+ssh", "ERR", "RLIST: "+err.Error())
		return err
	}

	byParent := make(map[string][]model.FileEntry)
	seenDirs := make(map[string]struct{})
	for _, fi := range result.FileList {
		if fi.Name == "" || fi.Name == "." {
			continue
		}
		if fi.Mode&rsyncpkg.S_IFMT == rsyncpkg.S_IFLNK {
			continue
		}
		parent := path.Dir(fi.Name)
		if parent == "." {
			parent = ""
		}
		parentRel := path.Join(scope, parent)
		relPath := path.Join(scope, fi.Name)
		mode := rsyncModeToFileMode(fi.Mode)
		isDir := mode&os.ModeDir != 0
		byParent[parentRel] = append(byParent[parentRel], model.FileEntry{
			RelPath: relPath,
			Name:    path.Base(fi.Name),
			Size:    fi.Length,
			ModTime: fi.ModTime,
			IsDir:   isDir,
			Mode:    mode,
		})
		if isDir {
			seenDirs[relPath] = struct{}{}
		}
	}
	for parent, entries := range byParent {
		emit(parent, entries)
	}
	for d := range seenDirs {
		if _, ok := byParent[d]; !ok {
			emit(d, nil)
		}
	}
	Log.Add("rsync+ssh", "<<<", fmt.Sprintf("RLIST %d entries", len(result.FileList)))
	return nil
}

func rsyncModeToFileMode(m int32) os.FileMode {
	mode := os.FileMode(m) & os.ModePerm
	switch m & rsyncpkg.S_IFMT {
	case rsyncpkg.S_IFDIR:
		mode |= os.ModeDir
	case rsyncpkg.S_IFLNK:
		mode |= os.ModeSymlink
	}
	return mode
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
	out, err := b.sshRunCtx(ctx, cmd)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum output")
	}
	return strings.TrimPrefix(fields[0], "\\"), nil
}

func (b *RsyncSSHBackend) PrefetchChecksums(ctx context.Context, scope string, recursive bool) error {
	if b.cksumAlgo != "md4" {
		return nil
	}
	_, err := b.fetchMD4(ctx, scope, recursive)
	return err
}

// invalidateMD4 drops one path's cached MD4 hash. md4cache is populated in
// bulk by PrefetchChecksums and has no TTL, so writes that change a file's
// content must clear its entry or a follow-up Checksum returns the pre-write
// hash.
func (b *RsyncSSHBackend) invalidateMD4(relPath string) {
	b.md4mu.Lock()
	delete(b.md4cache, relPath)
	b.md4mu.Unlock()
}

// invalidateMD4Tree drops all cached MD4 hashes at or under prefix. Used by
// RemoveAll and dir Rename.
func (b *RsyncSSHBackend) invalidateMD4Tree(prefix string) {
	b.md4mu.Lock()
	defer b.md4mu.Unlock()
	for k := range b.md4cache {
		if k == prefix || strings.HasPrefix(k, prefix+"/") {
			delete(b.md4cache, k)
		}
	}
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

	// -n (--dry-run): sender computes and ships checksums in the filelist
	// even when no data is transferred. Without it rsync downloads each
	// file body just to discard it.
	args := []string{"-c", "-n"}
	if recursive {
		args = append(args, "-r")
	}
	client, err := rsyncclient.New(args, rsyncclient.WithStdout(io.Discard), rsyncclient.WithStderr(io.Discard), rsyncclient.DontRestrict())
	if err != nil {
		return nil, err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remotePath))
	Log.Add("rsync+ssh", ">>>", "MD4 "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		return nil, err
	}
	stop := cancelCloser(ctx, session)

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		stop()
		session.Close()
		return nil, err
	}

	if err := session.Start(serverCmd); err != nil {
		stop()
		session.Close()
		return nil, err
	}

	result, err := client.Run(ctx, rw, []string{tmpDir + "/"})
	stop()
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

func (b *RsyncSSHBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, _ time.Time) error {
	fp := shellQuote(path.Join(b.base, relPath))
	mt := shellQuote(mtime.UTC().Format("2006-01-02T15:04:05.000000000Z"))
	at := shellQuote(atime.UTC().Format("2006-01-02T15:04:05.000000000Z"))
	cmd := fmt.Sprintf("touch -m -d %s %s && touch -a -d %s %s", mt, fp, at, fp)
	_, err := b.sshRunCtx(ctx, cmd)
	b.listCache.invalidate(parentDir(relPath))
	return err
}

func (b *RsyncSSHBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	fullPath := path.Join(b.base, relPath)
	cmd := fmt.Sprintf("mkdir -p %s", shellQuote(fullPath))
	if mode != 0 {
		cmd = fmt.Sprintf("%s && chmod %04o %s", cmd, mode.Perm(), shellQuote(fullPath))
	}
	_, err := b.sshRunCtx(ctx, cmd)
	b.listCache.invalidateAncestors(relPath)
	return err
}

func (b *RsyncSSHBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	_, err := b.sshRunCtx(ctx, fmt.Sprintf("mv %s %s",
		shellQuote(path.Join(b.base, oldRelPath)),
		shellQuote(path.Join(b.base, newRelPath))))
	b.listCache.invalidateTree(oldRelPath)
	b.listCache.invalidate(parentDir(oldRelPath))
	b.listCache.invalidate(parentDir(newRelPath))
	b.invalidateMD4Tree(oldRelPath)
	b.invalidateMD4(newRelPath)
	return err
}

func (b *RsyncSSHBackend) Remove(ctx context.Context, relPath string) error {
	_, err := b.sshRunCtx(ctx, fmt.Sprintf("rm %s", shellQuote(path.Join(b.base, relPath))))
	b.listCache.invalidate(parentDir(relPath))
	b.invalidateMD4(relPath)
	return err
}

func (b *RsyncSSHBackend) RemoveAll(ctx context.Context, relPath string) error {
	_, err := b.sshRunCtx(ctx, fmt.Sprintf("rm -rf %s", shellQuote(path.Join(b.base, relPath))))
	b.listCache.invalidateTree(relPath)
	b.listCache.invalidate(parentDir(relPath))
	b.invalidateMD4Tree(relPath)
	return err
}

func (b *RsyncSSHBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "rsync-ssh-dl-*")
	if err != nil {
		return nil, err
	}

	counter := progressFromContext(ctx)
	base := baseProgressFromContext(ctx)
	var stop chan struct{}
	if counter != nil {
		size := fileSizeFromContext(ctx)
		if size <= 0 {
			size = 1 << 62
		}
		var baseAdder *CappedAdder
		if base != nil {
			baseAdder = NewCappedAdder(base, size)
		}
		stop = make(chan struct{})
		go tailDirSize(stop, tmpDir, filepath.Base(relPath), NewCappedAdder(counter, size), baseAdder)
	}

	remotePath := path.Join(b.base, relPath)
	client, err := rsyncclient.New(rsyncFlagsForCtx(ctx), rsyncclient.WithStderr(io.Discard))
	if err != nil {
		if stop != nil {
			close(stop)
		}
		os.RemoveAll(tmpDir)
		return nil, err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remotePath))
	Log.Add("rsync+ssh", ">>>", serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		if stop != nil {
			close(stop)
		}
		os.RemoveAll(tmpDir)
		return nil, err
	}
	sStop := cancelCloser(ctx, session)

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		sStop()
		session.Close()
		if stop != nil {
			close(stop)
		}
		os.RemoveAll(tmpDir)
		return nil, err
	}

	if err := session.Start(serverCmd); err != nil {
		sStop()
		session.Close()
		if stop != nil {
			close(stop)
		}
		os.RemoveAll(tmpDir)
		Log.Add("rsync+ssh", "ERR", err.Error())
		return nil, err
	}

	_, err = client.Run(ctx, rw, []string{tmpDir + "/"})
	sStop()
	session.Close()
	if stop != nil {
		close(stop)
	}
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
	rc := &tempReadCloser{File: f, tmpDir: tmpDir}
	if counter != nil {
		return WrapPreCounted(rc), nil
	}
	return rc, nil
}

// SendLocalFile sends an existing local file via rsync-over-SSH directly,
// bypassing the per-call tmp dir. Progress: rw is wrapped so bytes written to
// the SSH session credit progress.Bytes during the push — without this, the
// stall guard fires after StallTimeout for any file taking longer than that.
// On error the in-band credit is rolled back so a retry doesn't double-count.
// On success a final cap-respecting top-up brings the total to fileSize even
// when rsync ran in delta mode and sent less than the full body.
func (b *RsyncSSHBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, _ os.FileMode) error {
	remoteDest := path.Join(b.base, path.Dir(relPath))
	b.sshRunCtx(ctx, fmt.Sprintf("mkdir -p %s", shellQuote(remoteDest)))

	client, err := rsyncclient.New(rsyncFlagsForCtx(ctx), rsyncclient.WithSender(), rsyncclient.WithStderr(io.Discard))
	if err != nil {
		return err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remoteDest + "/"))
	Log.Add("rsync+ssh", ">>>", "SEND "+srcPath+" via "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	stop := cancelCloser(ctx, session)
	rw, err := b.sessionReadWriter(session)
	if err != nil {
		stop()
		session.Close()
		return err
	}
	if err := session.Start(serverCmd); err != nil {
		stop()
		session.Close()
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}

	counter := progressFromContext(ctx)
	fileSize := fileSizeFromContext(ctx)
	var adder *CappedAdder
	var rwForRun io.ReadWriter = rw
	if counter != nil && fileSize > 0 {
		adder = NewCappedAdder(counter, fileSize)
		rwForRun = &CountingReadWriter{RW: rw, Adder: adder}
	}

	_, err = client.Run(ctx, rwForRun, []string{srcPath})
	stop()
	session.Close()
	b.listCache.invalidateAncestors(relPath)
	b.invalidateMD4(relPath)
	if err != nil {
		if adder != nil {
			counter.Add(-adder.Used())
		}
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}
	if adder != nil {
		adder.Add(fileSize)
	}
	Log.Add("rsync+ssh", "<<<", "OK")
	return nil
}

// RecvToLocalFile downloads via rsync-over-SSH directly to dstPath. Any
// existing prefix is reused for delta-sync resume.
func (b *RsyncSSHBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	parent := filepath.Dir(dstPath)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	if fi, err := os.Stat(dstPath); err == nil && fi.IsDir() {
		return fmt.Errorf("rsync+ssh: refuse to receive into existing directory %s", dstPath)
	}

	counter := progressFromContext(ctx)
	base := baseProgressFromContext(ctx)
	var stop chan struct{}
	if counter != nil {
		size := fileSizeFromContext(ctx)
		if size <= 0 {
			size = 1 << 62
		}
		var baseAdder *CappedAdder
		if base != nil {
			baseAdder = NewCappedAdder(base, size)
		}
		stop = make(chan struct{})
		go tailDirSize(stop, parent, filepath.Base(dstPath), NewCappedAdder(counter, size), baseAdder)
	}

	remotePath := path.Join(b.base, relPath)
	client, err := rsyncclient.New(rsyncFlagsForCtx(ctx), rsyncclient.WithStderr(io.Discard))
	if err != nil {
		if stop != nil {
			close(stop)
		}
		return err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remotePath))
	Log.Add("rsync+ssh", ">>>", "RECV "+remotePath+" -> "+dstPath+" via "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		if stop != nil {
			close(stop)
		}
		return err
	}
	sStop := cancelCloser(ctx, session)
	rw, err := b.sessionReadWriter(session)
	if err != nil {
		sStop()
		session.Close()
		if stop != nil {
			close(stop)
		}
		return err
	}
	if err := session.Start(serverCmd); err != nil {
		sStop()
		session.Close()
		if stop != nil {
			close(stop)
		}
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}

	_, err = client.Run(ctx, rw, []string{parent + "/"})
	sStop()
	session.Close()
	if stop != nil {
		close(stop)
	}
	if err != nil {
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}
	if fi, statErr := os.Stat(dstPath); statErr != nil {
		Log.Add("rsync+ssh", "ERR", "RECV "+relPath+": dst missing after rsync: "+statErr.Error())
		return fmt.Errorf("rsync+ssh: dst missing after recv: %w", statErr)
	} else if fi.IsDir() {
		Log.Add("rsync+ssh", "ERR", "RECV "+relPath+": dst is a directory after rsync")
		return fmt.Errorf("rsync+ssh: dst became a directory after recv: %s", dstPath)
	} else {
		Log.Add("rsync+ssh", "<<<", fmt.Sprintf("RECV %s OK (%d bytes)", relPath, fi.Size()))
	}
	return nil
}

// AppendFrom resumes a partial upload by running rsync with --append. Remote
// rsync inspects its existing dst size and only sends the missing tail. With
// --inplace --partial -W on the transfer flags, the dst is written in place
// and a partial upload survives for the next retry. -W is dropped automatically
// by gorsync's serveroptions when --append is set (real rsync rejects the
// combination).
//
// When src exposes a local filesystem path, rsync reads directly from it (no
// tmpfile copy). Otherwise the source is streamed into a tmpfile first.
//
// The dst file is pre-truncated to offset on the remote — callers compute
// offset from a recent dst stat, so this is normally a no-op, but it defends
// against the remote shrinking under us between size probe and upload.
func (b *RsyncSSHBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	fullPath := path.Join(b.base, relPath)
	if _, err := b.sshRunCtx(ctx, fmt.Sprintf("mkdir -p %s && truncate -s %d %s",
		shellQuote(path.Dir(fullPath)), offset, shellQuote(fullPath))); err != nil {
		return err
	}

	srcPath := src.LocalPath()
	var tmpDir string
	if srcPath == "" {
		var err error
		tmpDir, err = os.MkdirTemp("", "rsync-ssh-append-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)
		tmpPath := filepath.Join(tmpDir, filepath.Base(relPath))
		f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return err
		}
		rd, err := src.Open(ctx)
		if err != nil {
			f.Close()
			return err
		}
		_, copyErr := io.Copy(f, rd)
		rd.Close()
		f.Close()
		if copyErr != nil {
			return copyErr
		}
		srcPath = tmpPath
	}

	counter := progressFromContext(ctx)
	tailSize := src.Size() - offset
	var pushAdder *CappedAdder
	if counter != nil && tailSize > 0 {
		pushAdder = NewCappedAdder(counter, tailSize)
	}

	remoteDest := path.Join(b.base, path.Dir(relPath))
	flags := append(rsyncFlagsForCtx(ctx), "--append")
	client, err := rsyncclient.New(flags, rsyncclient.WithSender(), rsyncclient.WithStderr(io.Discard))
	if err != nil {
		return err
	}
	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remoteDest + "/"))
	Log.Add("rsync+ssh", ">>>", fmt.Sprintf("APPEND %s @%d/%d via %s", relPath, offset, src.Size(), serverCmd))

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	stop := cancelCloser(ctx, session)
	rw, rwErr := b.sessionReadWriter(session)
	if rwErr != nil {
		stop()
		session.Close()
		return rwErr
	}
	if err := session.Start(serverCmd); err != nil {
		stop()
		session.Close()
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}

	var rwForRun io.ReadWriter = rw
	if pushAdder != nil {
		rwForRun = &CountingReadWriter{RW: rw, Adder: pushAdder}
	}
	_, err = client.Run(ctx, rwForRun, []string{srcPath})
	stop()
	session.Close()
	b.listCache.invalidateAncestors(relPath)
	b.invalidateMD4(relPath)
	if err != nil {
		if pushAdder != nil {
			counter.Add(-pushAdder.Used())
		}
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}
	if pushAdder != nil {
		pushAdder.Add(tailSize)
	}
	Log.Add("rsync+ssh", "<<<", fmt.Sprintf("APPEND %s OK (%d bytes appended)", relPath, tailSize))
	return nil
}

func (b *RsyncSSHBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
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
	Log.Add("rsync+ssh", ">>>", cmd)
	if err := session.Start(cmd); err != nil {
		session.Close()
		return nil, err
	}
	stop := cancelCloser(ctx, session)
	return &sshReadCloser{session: session, Reader: rd, stop: stop}, nil
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

	counter := progressFromContext(ctx)
	fileSize := fileSizeFromContext(ctx)
	var readAdder, pushAdder *CappedAdder
	var readBudget, pushBudget int64
	if counter != nil && fileSize > 0 && !IsPreCounted(src) {
		readBudget = fileSize / 2
		pushBudget = fileSize - readBudget
		readAdder = NewCappedAdder(counter, readBudget)
		pushAdder = NewCappedAdder(counter, pushBudget)
	}
	var copyDst io.Writer = f
	if readAdder != nil {
		copyDst = &CountingWriter{W: f, Adder: readAdder}
	}
	if _, err := io.Copy(copyDst, src); err != nil {
		f.Close()
		if readAdder != nil {
			counter.Add(-readAdder.Used())
		}
		return err
	}
	f.Close()

	remoteDest := path.Join(b.base, path.Dir(relPath))
	b.sshRunCtx(ctx, fmt.Sprintf("mkdir -p %s", shellQuote(remoteDest)))

	client, err := rsyncclient.New(rsyncFlagsForCtx(ctx), rsyncclient.WithSender(), rsyncclient.WithStderr(io.Discard))
	if err != nil {
		return err
	}

	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remoteDest + "/"))
	Log.Add("rsync+ssh", ">>>", "SEND "+relPath+" via "+serverCmd)

	session, err := b.client.NewSession()
	if err != nil {
		return err
	}
	stop := cancelCloser(ctx, session)

	rw, err := b.sessionReadWriter(session)
	if err != nil {
		stop()
		session.Close()
		return err
	}

	if err := session.Start(serverCmd); err != nil {
		stop()
		session.Close()
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}

	var rwForRun io.ReadWriter = rw
	if pushAdder != nil {
		rwForRun = &CountingReadWriter{RW: rw, Adder: pushAdder}
	}
	_, err = client.Run(ctx, rwForRun, []string{tmpFile})
	stop()
	session.Close()
	b.listCache.invalidateAncestors(relPath)
	b.invalidateMD4(relPath)
	if err != nil {
		if pushAdder != nil {
			counter.Add(-pushAdder.Used())
		}
		Log.Add("rsync+ssh", "ERR", err.Error())
		return err
	}
	if pushAdder != nil {
		pushAdder.Add(pushBudget)
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
	return &sessionRW{r: stdout, w: stdin, session: session}, nil
}

type sessionRW struct {
	r       io.Reader
	w       io.Writer
	session *ssh.Session
}

func (s *sessionRW) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *sessionRW) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *sessionRW) Close() error                { return s.session.Close() }

func (b *RsyncSSHBackend) sshRun(cmd string) (string, error) {
	return b.sshRunCtx(nil, cmd)
}

func (b *RsyncSSHBackend) sshRunCtx(ctx context.Context, cmd string) (string, error) {
	Log.Add("rsync+ssh", ">>>", cmd)
	session, err := b.client.NewSession()
	if err != nil {
		Log.Add("rsync+ssh", "ERR", err.Error())
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
			Log.Add("rsync+ssh", "ERR", msg)
			return "", fmt.Errorf("%s", msg)
		}
		Log.Add("rsync+ssh", "ERR", err.Error())
		return "", err
	}
	out := stdout.String()
	if out != "" {
		trimmed := strings.TrimRight(out, "\n")
		if n := strings.Count(trimmed, "\n"); n > 0 {
			Log.Add("rsync+ssh", "<<<", fmt.Sprintf("%d lines", n+1))
		} else {
			Log.Add("rsync+ssh", "<<<", trimmed)
		}
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
