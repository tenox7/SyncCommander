package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	cksum := flag.Bool("cksum", false, "enable checksum comparison")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: sc [--cksum] [--insecure] [<left-path> <right-path>]\n")
		fmt.Fprintf(os.Stderr, "  paths: /local/dir or {sftp,ssh,scp,ftp,ftps,ftpes,rsync,rsync+ssh}://[user[:pass]@]host/path\n")
	}
	flag.Parse()

	var leftPath, rightPath string
	switch flag.NArg() {
	case 0:
		cwd, err := os.Getwd()
		if err != nil {
			cwd = "."
		}
		leftPath = cwd
		rightPath = cwd
	case 2:
		leftPath = flag.Arg(0)
		rightPath = flag.Arg(1)
	default:
		flag.Usage()
		os.Exit(1)
	}

	left := openBackendLazy(leftPath, *insecure)
	right := openBackendLazy(rightPath, *insecure)
	defer closeBackend(left)
	defer closeBackend(right)

	model := NewModel(left, right, *cksum, *insecure)
	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func openBackendLazy(arg string, insecure bool) Backend {
	if !isRemote(arg) {
		info, err := os.Stat(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %v\n", arg, err)
			os.Exit(1)
		}
		if !info.IsDir() {
			fmt.Fprintf(os.Stderr, "error: %s: not a directory\n", arg)
			os.Exit(1)
		}
		return NewLocalBackend(arg)
	}
	return &lazyBackend{
		factory: func() (Backend, error) {
			return openBackend(arg, insecure)
		},
		display: arg,
	}
}

func isRemote(arg string) bool {
	for _, p := range []string{"sftp://", "ssh://", "scp://", "ftp://", "ftps://", "ftpes://", "rsync+ssh://", "rsync://"} {
		if strings.HasPrefix(arg, p) {
			return true
		}
	}
	return false
}

func openBackend(arg string, insecure bool) (Backend, error) {
	if strings.HasPrefix(arg, "sftp://") {
		return NewSFTPBackend(arg)
	}
	if strings.HasPrefix(arg, "ssh://") || strings.HasPrefix(arg, "scp://") {
		return NewSCPBackend(arg)
	}
	if strings.HasPrefix(arg, "ftp://") || strings.HasPrefix(arg, "ftps://") || strings.HasPrefix(arg, "ftpes://") {
		return NewFTPBackend(arg, insecure)
	}
	if strings.HasPrefix(arg, "rsync+ssh://") {
		return NewRsyncSSHBackend(arg)
	}
	if strings.HasPrefix(arg, "rsync://") {
		return NewRsyncBackend(arg)
	}
	return NewLocalBackend(arg), nil
}

func tryOpenBackend(arg string, insecure bool) (Backend, error) {
	if !isRemote(arg) {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", arg, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s: not a directory", arg)
		}
		return NewLocalBackend(arg), nil
	}
	return &lazyBackend{
		factory: func() (Backend, error) {
			return openBackend(arg, insecure)
		},
		display: arg,
	}, nil
}

func closeBackend(b Backend) {
	if c, ok := b.(interface{ Close() error }); ok {
		c.Close()
	}
}
