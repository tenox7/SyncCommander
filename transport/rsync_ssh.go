package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gokrazy/rsync/rsyncclient"

	"sc/model"
)

type RsyncSSHBackend struct {
	sshShell
	md4 md4Cache
}

func NewRsyncSSHBackend(rawURL string, insecure bool) (*RsyncSSHBackend, error) {
	conn, err := dialSSH(rawURL, insecure)
	if err != nil {
		return nil, err
	}
	b := &RsyncSSHBackend{sshShell: sshShell{client: conn.client, proto: "rsync+ssh", listCache: newListCache()}}
	b.base = expandHome(conn.basePath, b.home)
	if _, err := b.run(context.Background(), "command -v rsync"); err != nil {
		conn.client.Close()
		return nil, errors.New("rsync+ssh: remote rsync not found")
	}
	b.display = sshDisplayURL(conn, b.base)
	return b, nil
}

func (b *RsyncSSHBackend) OwnsCopyProgress() bool { return true }
func (b *RsyncSSHBackend) Close() error           { return b.client.Close() }

func (b *RsyncSSHBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	return b.listCache.serve(ctx, relDir, b.findList)
}

func (b *RsyncSSHBackend) PreloadRecursive(ctx context.Context, scope string) error {
	b.listCache.start(ctx, scope, b.runRecursiveList)
	return nil
}

func (b *RsyncSSHBackend) runRecursiveList(ctx context.Context, scope string, emit func(string, []model.FileEntry)) error {
	client, err := newRsyncClient([]string{"-n", "-r"}, rsyncclient.DontRestrict())
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "rsync-rlist-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	result, err := b.runRsync(ctx, "RLIST", client, b.abs(scope)+"/", []string{tmpDir + "/"}, nil)
	if err != nil {
		return err
	}
	emitFileList(scope, result.FileList, emit)
	Log.Add(b.proto, "<<<", fmt.Sprintf("RLIST %d entries", len(result.FileList)))
	return nil
}

// runRsync starts the remote rsync server for remote over a fresh session and
// drives client against it; wrap may decorate the stream to credit progress.
func (b *RsyncSSHBackend) runRsync(ctx context.Context, label string, client *rsyncclient.Client, remote string, local []string, wrap func(io.ReadWriter) io.ReadWriter) (*rsyncclient.Result, error) {
	serverCmd := b.buildServerCmd(client.ServerCommandOptions(remote))
	Log.Add(b.proto, ">>>", label+" "+serverCmd)
	session, err := b.client.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	defer CancelCloser(ctx, session)()
	stdin, err := session.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := session.Start(serverCmd); err != nil {
		Log.Add(b.proto, "ERR", err.Error())
		return nil, err
	}
	var rw io.ReadWriter = &sessionRW{r: stdout, w: stdin}
	if wrap != nil {
		rw = wrap(rw)
	}
	result, err := client.Run(ctx, rw, local)
	if err != nil {
		Log.Add(b.proto, "ERR", err.Error())
	}
	return result, err
}

func (b *RsyncSSHBackend) buildServerCmd(args []string) string {
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellQuote(a)
	}
	return "rsync " + strings.Join(quoted, " ")
}

type sessionRW struct {
	r io.Reader
	w io.Writer
}

func (s *sessionRW) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *sessionRW) Write(p []byte) (int, error) { return s.w.Write(p) }

func (b *RsyncSSHBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if b.cksumAlgo == "md4" {
		return b.md4.lookup(ctx, relPath, b.fetchMD4)
	}
	return b.sshShell.Checksum(ctx, relPath)
}

func (b *RsyncSSHBackend) ProbeChecksums() []string {
	return append(b.sshShell.ProbeChecksums(), "md4")
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
	client, err := newRsyncClient(md4ListFlags(recursive), rsyncclient.DontRestrict())
	if err != nil {
		return nil, err
	}
	remote := b.abs(scope)
	if recursive {
		remote += "/"
	}
	result, err := b.runRsync(ctx, "MD4", client, remote, []string{tmpDir + "/"}, nil)
	if err != nil {
		return nil, err
	}
	got := b.md4.fromFileList(scope, recursive, result.FileList)
	Log.Add(b.proto, "<<<", fmt.Sprintf("MD4 %d checksums", len(got)))
	return got, nil
}

func (b *RsyncSSHBackend) Rename(ctx context.Context, oldRel, newRel string) error {
	err := b.sshShell.Rename(ctx, oldRel, newRel)
	b.md4.invalidateTree(oldRel)
	b.md4.invalidate(newRel)
	return err
}

func (b *RsyncSSHBackend) Remove(ctx context.Context, relPath string) error {
	err := b.sshShell.Remove(ctx, relPath)
	b.md4.invalidate(relPath)
	return err
}

func (b *RsyncSSHBackend) RemoveAll(ctx context.Context, relPath string) error {
	err := b.sshShell.RemoveAll(ctx, relPath)
	b.md4.invalidateTree(relPath)
	return err
}

func (b *RsyncSSHBackend) invalidateAfterTreeSend(relPath string) {
	b.listCache.invalidateTree(relPath)
	b.listCache.invalidateAncestors(relPath)
	b.md4.invalidateTree(relPath)
}

func (b *RsyncSSHBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return rsyncOpenViaTemp(ctx, relPath, func(dstDir string) error {
		client, err := newRsyncClient(transferFlags())
		if err != nil {
			return err
		}
		_, err = b.runRsync(ctx, "RECV", client, b.abs(relPath), []string{dstDir}, nil)
		return err
	})
}

func (b *RsyncSSHBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	return b.stream(ctx, b.client, b.tailCmd(relPath, offset), func() {})
}

// RecvToLocalFile downloads straight to dstPath; an existing prefix there is
// reused by rsync's resume.
func (b *RsyncSSHBackend) RecvToLocalFile(ctx context.Context, relPath, dstPath string) error {
	return rsyncRecvToLocal(ctx, b.proto, relPath, dstPath, func(dstDir string) error {
		client, err := newRsyncClient(transferFlags())
		if err != nil {
			return err
		}
		_, err = b.runRsync(ctx, "RECV "+relPath+" -> "+dstPath+" via", client, b.abs(relPath), []string{dstDir}, nil)
		return err
	})
}

// SendLocalTree uploads everything under srcRoot/relPath in one rsync
// session. -c stands in for --ignore-times: rsync then skips only files whose
// size, mtime and checksum all match, so a content-different file is never
// dropped while a mostly identical tree is not resent. --progress makes the
// sender print each filename as it starts, which feeds onFile.
func (b *RsyncSSHBackend) SendLocalTree(ctx context.Context, srcRoot, relPath string, onFile func(name string)) error {
	srcPath := filepath.Join(srcRoot, relPath) + string(filepath.Separator)
	remoteDest := b.abs(relPath) + "/"
	if _, err := b.run(ctx, "mkdir -p "+shellQuote(remoteDest)); err != nil {
		return err
	}
	client, err := newRsyncClient([]string{"-t", "--inplace", "--partial", "-W", "-r", "-c", "--progress"},
		rsyncclient.WithSender(), rsyncclient.WithStdout(&filenameLineParser{onFile: onFile}))
	if err != nil {
		return err
	}
	var wrap func(io.ReadWriter) io.ReadWriter
	if counter := progressFromContext(ctx); counter != nil {
		wrap = countingRW(NewCappedAdder(counter, 1<<62))
	}
	_, err = b.runRsync(ctx, "BATCH "+srcPath+" via", client, remoteDest, []string{srcPath}, wrap)
	b.invalidateAfterTreeSend(relPath)
	if err == nil {
		Log.Add(b.proto, "<<<", "BATCH OK")
	}
	return err
}

// filenameLineParser parses gorsync's sender --progress stdout. Per file the
// sender writes "<name>\n" followed by progress lines built with \r as the
// in-line separator and a final \n. Lines containing \r are progress; lines
// without \r are filenames.
type filenameLineParser struct {
	buf    []byte
	onFile func(name string)
}

func (p *filenameLineParser) Write(b []byte) (int, error) {
	p.buf = append(p.buf, b...)
	for {
		i := bytes.IndexByte(p.buf, '\n')
		if i < 0 {
			return len(b), nil
		}
		line := p.buf[:i]
		p.buf = p.buf[i+1:]
		if bytes.IndexByte(line, '\r') >= 0 {
			continue
		}
		if name := strings.TrimSpace(string(line)); name != "" && p.onFile != nil {
			p.onFile(name)
		}
	}
}

// sendFile pushes one local file into relPath's directory. prep runs after the
// mkdir in the same round trip; the push credits adder as bytes flow and is
// settled against budget afterwards.
func (b *RsyncSSHBackend) sendFile(ctx context.Context, label, srcPath, relPath, prep string, extraFlags []string, adder *CappedAdder, budget int64) error {
	remoteDest := b.abs(parentDir(relPath))
	cmd := "mkdir -p " + shellQuote(remoteDest)
	if prep != "" {
		cmd += " && " + prep
	}
	if _, err := b.run(ctx, cmd); err != nil {
		return err
	}
	client, err := newRsyncClient(append(transferFlags(), extraFlags...), rsyncclient.WithSender())
	if err != nil {
		return err
	}
	_, err = b.runRsync(ctx, label, client, remoteDest+"/", []string{srcPath}, countingRW(adder))
	b.listCache.invalidateAncestors(relPath)
	b.md4.invalidate(relPath)
	settlePush(ctx, adder, budget, err)
	return err
}

// SendLocalFile sends an existing local file directly, no tmp copy. The push
// credits progress as data flows so the stall guard sees movement, and the
// final top-up covers whatever delta-sync did not send.
func (b *RsyncSSHBackend) SendLocalFile(ctx context.Context, srcPath, relPath string, _ os.FileMode) error {
	var adder *CappedAdder
	fileSize, _ := fileSizeFromContext(ctx)
	if counter := progressFromContext(ctx); counter != nil && fileSize > 0 {
		adder = NewCappedAdder(counter, fileSize)
	}
	return b.sendFile(ctx, "SEND "+srcPath+" via", srcPath, relPath, "", nil, adder, fileSize)
}

// AppendFrom resumes a partial upload with rsync --append: the remote inspects
// its dst size and only the missing tail is sent. The dst is first sized to
// offset so a remote that shrank between the size probe and the upload cannot
// leave a hole. A source without a local path is spooled to a tmpfile.
func (b *RsyncSSHBackend) AppendFrom(ctx context.Context, relPath string, src model.RangeOpener, mode os.FileMode, offset int64) error {
	srcPath := src.LocalPath()
	if srcPath == "" {
		tmpDir, err := os.MkdirTemp("", "rsync-ssh-append-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)
		srcPath = filepath.Join(tmpDir, filepath.Base(relPath))
		if err := spoolTo(ctx, src, srcPath, mode); err != nil {
			return err
		}
	}
	tailSize := src.Size() - offset
	var adder *CappedAdder
	if counter := progressFromContext(ctx); counter != nil && tailSize > 0 {
		adder = NewCappedAdder(counter, tailSize)
	}
	label := fmt.Sprintf("APPEND %s @%d/%d via", relPath, offset, src.Size())
	return b.sendFile(ctx, label, srcPath, relPath, truncateCmd(b.abs(relPath), offset), []string{"--append"}, adder, tailSize)
}

func (b *RsyncSSHBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	tmpDir, err := os.MkdirTemp("", "rsync-ssh-ul-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	tmpFile := filepath.Join(tmpDir, filepath.Base(relPath))
	pushAdder, pushBudget, err := stageUpload(ctx, src, tmpFile, mode)
	if err != nil {
		return err
	}
	return b.sendFile(ctx, "SEND "+relPath+" via", tmpFile, relPath, "", nil, pushAdder, pushBudget)
}
