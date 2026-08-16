package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sc/model"
	"sc/transport"
	"sc/ui"
)

var version = "dev"

func main() {
	size := flag.Bool("size", true, "compare file size")
	modtime := flag.Bool("modtime", true, "compare modify time")
	atime := flag.Bool("atime", false, "compare access time")
	ctime := flag.Bool("ctime", false, "compare change time")
	btime := flag.Bool("btime", false, "compare birth time")
	mode := flag.Bool("mode", false, "compare permissions")
	cksum := flag.Bool("checksum", false, "compare checksums")
	subsec := flag.Bool("subsec", false, "sub-second time precision")
	grace := flag.Bool("grace", true, "allow ±1s time grace")
	tzdst := flag.Bool("tzdst", true, "ignore TZ/DST differences (hour-modulo)")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	maxRetries := flag.Int("max-retries", 5, "max retry attempts for remote ops")
	webdavTimeout := flag.Duration("webdav-timeout", 5*time.Minute, "webdav idle timeout: abort a listing/transfer only after this long with no bytes (0 = never)")
	resticTimeout := flag.Duration("restic-timeout", 5*time.Minute, "restic idle timeout: abort a listing/transfer only after this long with no bytes (0 = never)")
	parallel := flag.Int("parallel", 4, "max concurrent file transfers during copy")
	scanParallel := flag.Int("scan-parallel", 8, "max directories listed concurrently during a scan")
	batch := flag.Bool("batch", true, "batch rsync+ssh dir transfers in a single session (off: per-file parallel)")
	deepScan := flag.Bool("deep-scan", true, "scan recursively at startup (false: list root + top level only, expand on demand)")
	pprofAddr := flag.String("pprof", "", "serve net/http/pprof on this address (e.g. localhost:6060)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: sc [flags] [<left-path> <right-path>]\n")
		fmt.Fprintf(os.Stderr, "  paths: /local/dir or {sftp,ssh,scp,ftp,ftps,ftpes,rsync,rsync+ssh,webdav,webdavs,restic,restics}://[user[:pass]@]host/path\n")
		fmt.Fprintf(os.Stderr, "  fake://{tiny,small,medium,large,huge,insane}[?dirs=8&files=25&depth=5&seed=1&diff=0.1&drop=0.02&latency=2ms] synthetic tree\n")
		fmt.Fprintf(os.Stderr, "compare flags (use --flag=false to disable defaults):\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Println("sc", version)
		return
	}

	if *pprofAddr != "" {
		go func() {
			// Errors go to the in-app log: stderr would corrupt the TUI.
			transport.Log.Add("pprof", "<<<", "listening on "+*pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				transport.Log.Add("pprof", "ERR", err.Error())
			}
		}()
	}

	transport.SetMaxRetries(*maxRetries)
	transport.SetWebDAVIdleTimeout(*webdavTimeout)
	transport.SetResticIdleTimeout(*resticTimeout)

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
		Size:        *size,
		ModTime:     *modtime,
		ATime:       *atime,
		CTime:       *ctime,
		BTime:       *btime,
		Mode:        *mode,
		Checksum:    *cksum,
		SubSecond:   *subsec,
		TimeGrace:   *grace,
		IgnoreTZDST: *tzdst,
	}

	if *parallel < 1 {
		*parallel = 1
	}
	left := transport.OpenBackendLazy(leftPath, *insecure, *parallel)
	right := transport.OpenBackendLazy(rightPath, *insecure, *parallel)
	defer transport.CloseBackend(left)
	defer transport.CloseBackend(right)
	mdl := ui.NewModel(left, right, opts, *insecure, *deepScan, *parallel, *scanParallel, *batch)
	p := tea.NewProgram(mdl, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
