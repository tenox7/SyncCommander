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
		fmt.Fprintf(os.Stderr, "usage: sc [--cksum] [--insecure] <left-path> <right-path>\n")
		fmt.Fprintf(os.Stderr, "  paths: /local/dir or {sftp,ssh,scp,ftp,ftps,ftpes,rsync,rsync+ssh}://[user[:pass]@]host/path\n")
	}
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	left, err := openBackend(flag.Arg(0), *insecure)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	right, err := openBackend(flag.Arg(1), *insecure)
	if err != nil {
		closeBackend(left)
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer closeBackend(left)
	defer closeBackend(right)

	model := NewModel(left, right, *cksum)
	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
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
	info, err := os.Stat(arg)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", arg, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s: not a directory", arg)
	}
	return NewLocalBackend(arg), nil
}

func closeBackend(b Backend) {
	if c, ok := b.(interface{ Close() error }); ok {
		c.Close()
	}
}
