package transport

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"sc/model"
)

type SFTPBackend struct {
	base      string
	client    *sftp.Client
	sshClient *ssh.Client
	display   string
	cksumAlgo string
	cksumCmds map[string]string
}

func NewSFTPBackend(rawURL string) (*SFTPBackend, error) {
	conn, err := dialSSH(rawURL)
	if err != nil {
		return nil, err
	}

	sftpClient, err := sftp.NewClient(conn.client)
	if err != nil {
		conn.client.Close()
		return nil, fmt.Errorf("sftp: %v", err)
	}

	remotePath := conn.basePath
	switch {
	case remotePath == "" || remotePath == "/" || remotePath == "/~":
		if wd, err := sftpClient.Getwd(); err == nil {
			remotePath = wd
		} else {
			remotePath = "/"
		}
	case strings.HasPrefix(remotePath, "/~/"):
		if wd, err := sftpClient.Getwd(); err == nil {
			remotePath = path.Join(wd, remotePath[3:])
		}
	}

	return &SFTPBackend{
		base:      remotePath,
		client:    sftpClient,
		sshClient: conn.client,
		display:   sshDisplayURL(conn, remotePath),
	}, nil
}

func (b *SFTPBackend) BasePath() string { return b.display }

func (b *SFTPBackend) Close() error {
	b.client.Close()
	return b.sshClient.Close()
}

func (b *SFTPBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	dir := path.Join(b.base, relDir)
	Log.Add("sftp", ">>>", "READDIR "+dir)
	entries, err := b.client.ReadDir(dir)
	if err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return nil, err
	}
	Log.Add("sftp", "<<<", fmt.Sprintf("%d entries", len(entries)))
	result := make([]model.FileEntry, 0, len(entries))
	for _, info := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		entry := model.FileEntry{
			RelPath: path.Join(relDir, info.Name()),
			Name:    info.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
			Mode:    info.Mode(),
		}
		if stat, ok := info.Sys().(*sftp.FileStat); ok && stat != nil {
			entry.ATime = time.Unix(int64(stat.Atime), 0)
		}
		result = append(result, entry)
	}
	return result, nil
}

func (b *SFTPBackend) Checksum(_ context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "" {
		return "", fmt.Errorf("no checksum algorithm configured")
	}
	cmd := fmt.Sprintf("%s %s", b.cksumCmds[b.cksumAlgo], shellQuote(path.Join(b.base, relPath)))
	out, err := runSSHCmd(b.sshClient, "sftp", cmd)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum output")
	}
	return fields[0], nil
}

func (b *SFTPBackend) ProbeChecksums() []string {
	if b.cksumCmds == nil {
		run := func(cmd string) (string, error) { return runSSHCmd(b.sshClient, "sftp", cmd) }
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

func (b *SFTPBackend) SetChecksumAlgo(algo string) {
	b.cksumAlgo = algo
}

func (b *SFTPBackend) SetTimes(_ context.Context, relPath string, mtime, atime, _ time.Time) error {
	err := b.client.Chtimes(path.Join(b.base, relPath), atime, mtime)
	if err != nil {
		Log.Add("sftp", "ERR", "CHTIMES "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) CopyFrom(_ context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	Log.Add("sftp", ">>>", "STOR "+relPath)
	fullPath := path.Join(b.base, relPath)
	if err := b.client.MkdirAll(path.Dir(fullPath)); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	f, err := b.client.Create(fullPath)
	if err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, src); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	if err := b.client.Chmod(fullPath, mode); err != nil {
		Log.Add("sftp", "ERR", err.Error())
		return err
	}
	return nil
}

func (b *SFTPBackend) Rename(_ context.Context, oldRelPath, newRelPath string) error {
	err := b.client.Rename(path.Join(b.base, oldRelPath), path.Join(b.base, newRelPath))
	if err != nil {
		Log.Add("sftp", "ERR", "RENAME "+oldRelPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) Remove(_ context.Context, relPath string) error {
	err := b.client.Remove(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("sftp", "ERR", "REMOVE "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) RemoveAll(_ context.Context, relPath string) error {
	err := b.removeAll(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("sftp", "ERR", "REMOVEALL "+relPath+": "+err.Error())
	}
	return err
}

func (b *SFTPBackend) removeAll(fullPath string) error {
	info, err := b.client.Stat(fullPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return b.client.Remove(fullPath)
	}
	entries, err := b.client.ReadDir(fullPath)
	if err != nil {
		return err
	}
	for _, e := range entries {
		child := path.Join(fullPath, e.Name())
		if e.IsDir() {
			if err := b.removeAll(child); err != nil {
				return err
			}
			continue
		}
		if err := b.client.Remove(child); err != nil {
			return err
		}
	}
	return b.client.RemoveDirectory(fullPath)
}

func (b *SFTPBackend) Open(_ context.Context, relPath string) (io.ReadCloser, error) {
	rc, err := b.client.Open(path.Join(b.base, relPath))
	if err != nil {
		Log.Add("sftp", "ERR", "OPEN "+relPath+": "+err.Error())
	}
	return rc, err
}
