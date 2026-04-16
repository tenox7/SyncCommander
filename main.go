package main

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"

	"sc/transport"
	"sc/ui"
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

	left := transport.OpenBackendLazy(leftPath, *insecure)
	right := transport.OpenBackendLazy(rightPath, *insecure)
	defer transport.CloseBackend(left)
	defer transport.CloseBackend(right)

	model := ui.NewModel(left, right, *cksum, *insecure)
	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
