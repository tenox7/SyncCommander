package transport

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"sc/model"
)

// startArgLoggingDaemon runs an rsync daemon whose pre-xfer exec writes down
// the arguments the client sent for each call, RSYNC_ARG0, RSYNC_ARG1 and on.
// calls reads them back, one list per call, in order.
func startArgLoggingDaemon(t *testing.T) (port int, moduleDir string, calls func() [][]string) {
	t.Helper()
	var logPath string
	port, root := startRsyncDaemonConf(t, func(root string) string {
		logPath = filepath.Join(root, "args.log")
		script := filepath.Join(root, "args.sh")
		body := "#!/bin/sh\n{ echo CALL; env | grep '^RSYNC_ARG'; } >> '" + logPath + "'\n"
		if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
		return "[testmod]\n  path = " + filepath.Join(root, "module") + "\n  read only = no\n  pre-xfer exec = " + script + "\n"
	})
	calls = func() [][]string {
		data, err := os.ReadFile(logPath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		var out [][]string
		var cur map[int]string
		flush := func() {
			if cur == nil {
				return
			}
			args := make([]string, len(cur))
			for i, v := range cur {
				if i >= 0 && i < len(args) {
					args[i] = v
				}
			}
			out = append(out, args)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line == "CALL" {
				flush()
				cur = map[int]string{}
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			n, err := strconv.Atoi(strings.TrimPrefix(k, "RSYNC_ARG"))
			if ok && err == nil && cur != nil {
				cur[n] = v
			}
		}
		flush()
		return out
	}
	return port, filepath.Join(root, "module"), calls
}

// shortFlags are the one-letter options of a call as the daemon got them.
// The options end at "."; a group's ".xyz" tail, after an "e", holds
// protocol capabilities, not options.
func shortFlags(args []string) string {
	var flags string
	for _, a := range args {
		if a == "." {
			break
		}
		if !strings.HasPrefix(a, "-") || strings.HasPrefix(a, "--") {
			continue
		}
		f := a[1:]
		if i := strings.IndexByte(f, '.'); i >= 0 {
			f = strings.TrimSuffix(f[:i], "e")
		}
		flags += f
	}
	return flags
}

// compareWithMD4 does to an rsync daemon what a checksum compare does: it
// picks md4 and sums relPath on the server.
func compareWithMD4(t *testing.T, b *RsyncBackend, relPath string) {
	t.Helper()
	ctx := context.Background()
	algos, err := b.ChecksumAlgos(ctx)
	if err != nil || !slices.Contains(algos, "md4") {
		t.Fatalf("the daemon offers %q (%v), want md4", algos, err)
	}
	if sum, err := b.Checksum(ctx, relPath, "md4"); err != nil || sum == "" {
		t.Fatalf("md4 of %s: %q, %v", relPath, sum, err)
	}
}

// R10: once a checksum compare had picked md4, every call of the rsync
// daemon backend that moves file data asked for -c. The sender then reads a
// file whole, to sum it, before its first byte moves, which the stall guard
// takes for a hung transfer, and a file whose content matches is skipped:
// sc copies a file only when told to, so all of it must go. A dry run, as
// the md4 listing is, moves nothing and may sum. The daemon here says what
// each call asked for.
func TestRsyncDaemonTransfersAfterAnMD4CompareAskForNoC(t *testing.T) {
	port, moduleDir, calls := startArgLoggingDaemon(t)
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	const full = "0123456789abcdefghij"
	for name, body := range map[string]string{"down.bin": "download me", "part.bin": full[:10]} {
		if err := os.WriteFile(filepath.Join(moduleDir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	local := filepath.Join(t.TempDir(), "up.bin")
	if err := os.WriteFile(local, []byte("upload me"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	compareWithMD4(t, b, "down.bin")

	read := func(r io.ReadCloser, err error) error {
		if err != nil {
			return err
		}
		defer r.Close()
		_, err = io.ReadAll(r)
		return err
	}
	moves := []struct {
		name string
		do   func() error
	}{
		{"SendLocalFile", func() error {
			return b.SendLocalFile(ctx, local, "up.bin", model.PutParams{Size: 9, Mode: 0o644})
		}},
		{"CopyFrom", func() error {
			return b.CopyFrom(ctx, "copy.bin", strings.NewReader("copy me"), model.PutParams{Size: 7, Mode: 0o644})
		}},
		{"AppendFrom", func() error {
			return b.AppendFrom(ctx, "part.bin", memRangeOpener([]byte(full)), model.PutParams{Size: int64(len(full)), Mode: 0o644}, 10)
		}},
		{"RecvToLocalFile", func() error {
			return b.RecvToLocalFile(ctx, "down.bin", filepath.Join(t.TempDir(), "down.bin"))
		}},
		{"Open", func() error { return read(b.Open(ctx, "down.bin")) }},
		{"OpenAt", func() error { return read(b.OpenAt(ctx, "down.bin", 5)) }},
	}
	for _, m := range moves {
		before := len(calls())
		if err := m.do(); err != nil {
			t.Errorf("%s: %v", m.name, err)
			continue
		}
		moved := 0
		for _, c := range calls()[before:] {
			f := shortFlags(c)
			if strings.ContainsRune(f, 'n') {
				continue
			}
			moved++
			if strings.ContainsRune(f, 'c') {
				t.Errorf("%s asked the daemon for -c after an md4 compare: %q", m.name, c[1:])
			}
		}
		if moved == 0 {
			t.Errorf("%s made no call to the daemon that moves data", m.name)
		}
	}
}
