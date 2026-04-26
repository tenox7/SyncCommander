package main

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
	"sc/ui"
)

func main() {
	size := flag.Bool("size", true, "compare file size")
	modtime := flag.Bool("modtime", true, "compare modify time")
	atime := flag.Bool("atime", false, "compare access time")
	ctime := flag.Bool("ctime", false, "compare change time")
	btime := flag.Bool("btime", false, "compare birth time")
	mode := flag.Bool("mode", false, "compare permissions")
	cksum := flag.Bool("cksum", false, "compare checksums")
	subsec := flag.Bool("subsec", false, "sub-second time precision")
	grace := flag.Bool("grace", true, "allow ±1s time grace")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: sc [flags] [<left-path> <right-path>]\n")
		fmt.Fprintf(os.Stderr, "  paths: /local/dir or {sftp,ssh,scp,ftp,ftps,ftpes,rsync,rsync+ssh}://[user[:pass]@]host/path\n")
		fmt.Fprintf(os.Stderr, "compare flags (use --flag=false to disable defaults):\n")
		flag.PrintDefaults()
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

	opts := &model.CompareOpts{
		Size:      *size,
		ModTime:   *modtime,
		ATime:     *atime,
		CTime:     *ctime,
		BTime:     *btime,
		Mode:      *mode,
		Checksum:  *cksum,
		SubSecond: *subsec,
		TimeGrace: *grace,
	}

	left := transport.OpenBackendLazy(leftPath, *insecure)
	right := transport.OpenBackendLazy(rightPath, *insecure)
	defer transport.CloseBackend(left)
	defer transport.CloseBackend(right)

	mdl := ui.NewModel(left, right, opts, *insecure)
	p := tea.NewProgram(mdl, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
