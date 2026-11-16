package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sc/model"
)

// D1, D2: the ask for a server's checksums ran under context.Background(),
// so a cancel, as when the user stops a checksum pass, did not stop it: it
// went on against a server that does not answer. A WebDAV server is asked
// with a listing, an ssh server with a command per algorithm.
func TestChecksumProbeStopsWhenCancelled(t *testing.T) {
	for name, open := range map[string]func(t *testing.T) model.Backend{
		// Answers the open check, never the listing that looks for sums.
		"webdav": func(t *testing.T) model.Backend {
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Depth") == "0" {
					w.WriteHeader(http.StatusMultiStatus)
					io.WriteString(w, wdCollectionBody)
					return
				}
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			t.Cleanup(func() { close(release); srv.CloseClientConnections(); srv.Close() })
			b, err := OpenBackend("webdav://u:p@"+srv.Listener.Addr().String()+"/", false, 1)
			if err != nil {
				t.Fatal(err)
			}
			return b
		},
		// The first command the probe runs never ends.
		"sftp": func(t *testing.T) model.Backend {
			bin := t.TempDir()
			if err := os.WriteFile(filepath.Join(bin, "sha256sum"), []byte("#!/bin/sh\nexec sleep 10\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			b, err := OpenBackend("sftp://x:pw@"+startExecSFTPServer(t)+t.TempDir(), true, 1)
			if err != nil {
				t.Fatal(err)
			}
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := open(t)
			defer CloseBackend(b)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := b.ChecksumAlgos(ctx)
				done <- err
			}()
			time.Sleep(300 * time.Millisecond)
			cancel()
			select {
			case err := <-done:
				// Known as a stop, the scanner keeps nothing and logs nothing.
				if !errors.Is(err, context.Canceled) {
					t.Errorf("a cancelled ask ended with %v; want the cancel", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the ask went on 3 s after it was cancelled")
			}
		})
	}
}

// The rsync daemon offered an "rsync" checksum that was the same text,
// "rsync_internal", for every file: any two files compared equal by it.
// md4 ranks first, so no compare picked it, but only the ranking stood in
// the way. Every checksum a daemon offers now differs for different files.
func TestRsyncChecksumsAreOfTheContent(t *testing.T) {
	ctx := context.Background()
	port, mod := startRsyncDaemon(t)
	writeFile(t, filepath.Join(mod, "a.txt"), "alpha")
	writeFile(t, filepath.Join(mod, "b.txt"), "beta")
	b, err := NewRsyncBackend(fmt.Sprintf("rsync://127.0.0.1:%d/testmod", port))
	if err != nil {
		t.Fatal(err)
	}
	algos, err := b.ChecksumAlgos(ctx)
	if err != nil || len(algos) == 0 {
		t.Fatalf("checksum algorithms = %q, %v", algos, err)
	}
	for _, algo := range algos {
		a, aerr := b.Checksum(ctx, "a.txt", algo)
		bb, berr := b.Checksum(ctx, "b.txt", algo)
		if aerr != nil || berr != nil || a == bb {
			t.Errorf("%s: a.txt %q, %v; b.txt %q, %v; want two sums that differ", algo, a, aerr, bb, berr)
		}
	}
}

// A rescan of one file with checksums on, before any checksum pass had
// picked an algorithm, summed with whatever each side held: sha256 on the
// local disk, nothing over ssh, which failed. It picks one first now.
func TestRescanOfOneFilePicksTheAlgorithm(t *testing.T) {
	ctx := context.Background()
	left, right := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(left, "f.txt"), "the same bytes")
	writeFile(t, filepath.Join(right, "f.txt"), "the same bytes")
	remote, err := OpenBackend("sftp://x:pw@"+startExecSFTPServer(t)+right, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseBackend(remote)
	s := model.NewScanner(NewLocalBackend(left), remote, 1, 1, true)
	s.Scan(ctx, model.CompareOpts{})
	var node *model.TreeNode
	for _, c := range s.Tree().Children {
		if c.Name == "f.txt" {
			node = c
		}
	}
	if node == nil {
		t.Fatal("f.txt is not in the tree")
	}
	s.RescanNode(ctx, node, model.CompareOpts{Checksum: true}, nil)
	if node.Compare.Checksum != model.AttrEqual {
		t.Errorf("after a rescan with checksums the file's checksum compare is %v, want equal", node.Compare.Checksum)
	}
}
