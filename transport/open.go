package transport

import (
	"fmt"
	"os"
	"strings"

	"sc/model"
)

func IsRemote(arg string) bool {
	for _, p := range []string{"sftp://", "ssh://", "scp://", "ftp://", "ftps://", "ftpes://", "rsync+ssh://", "rsync://"} {
		if strings.HasPrefix(arg, p) {
			return true
		}
	}
	return false
}

func OpenBackend(arg string, insecure bool) (model.Backend, error) {
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

func OpenBackendLazy(arg string, insecure bool) model.Backend {
	if !IsRemote(arg) {
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
	return NewLazyBackend(arg, func() (model.Backend, error) {
		return OpenBackend(arg, insecure)
	})
}

func TryOpenBackend(arg string, insecure bool) (model.Backend, error) {
	if !IsRemote(arg) {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", arg, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s: not a directory", arg)
		}
		return NewLocalBackend(arg), nil
	}
	return NewLazyBackend(arg, func() (model.Backend, error) {
		return OpenBackend(arg, insecure)
	}), nil
}

func CloseBackend(b model.Backend) {
	if c, ok := b.(interface{ Close() error }); ok {
		c.Close()
	}
}
