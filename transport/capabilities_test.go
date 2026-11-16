package transport

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"sc/model"
)

// methodCaps is what b's methods offer, OwnsProgress aside.
func methodCaps(b model.Backend) model.Caps {
	_, resume := b.(model.Resumer)
	_, parts := b.(model.PartWriter)
	_, region := b.(model.RegionChecksummer)
	_, openAt := b.(model.SeekableOpener)
	_, send := b.(model.LocalSender)
	_, recv := b.(model.LocalReceiver)
	_, batch := b.(model.BatchSender)
	_, preload := b.(model.RecursivePreloader)
	_, prefetch := b.(model.ChecksumPrefetcher)
	return model.Caps{Resume: resume, Parts: parts, RegionSums: region, OpenAt: openAt, SendLocal: send,
		RecvLocal: recv, Batch: batch, Preload: preload, PrefetchSums: prefetch}
}

// Each backend's Caps says what its methods do (D2): a flag without the
// method is a call no caller can make, a method without the flag a feature
// no caller uses. Only the rsync backends count their own upload progress.
func TestCapsMatchTheMethods(t *testing.T) {
	for name, b := range map[string]model.Backend{
		"local":     (*LocalBackend)(nil),
		"sftp":      (*SFTPBackend)(nil),
		"scp":       (*SCPBackend)(nil),
		"rsync+ssh": (*RsyncSSHBackend)(nil),
		"rsync":     (*RsyncBackend)(nil),
		"ftp":       (*FTPBackend)(nil),
		"webdav":    (*WebDAVBackend)(nil),
		"restic":    (*ResticBackend)(nil),
		"rclone":    (*RcloneBackend)(nil),
		"fake":      (*FakeBackend)(nil),
	} {
		caps, err := b.Capabilities(context.Background())
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if owns := name == "rsync" || name == "rsync+ssh"; caps.OwnsProgress != owns {
			t.Errorf("%s: OwnsProgress = %v, want %v", name, caps.OwnsProgress, owns)
		}
		caps.OwnsProgress = false
		if want := methodCaps(b); caps != want {
			t.Errorf("%s: Caps %+v; its methods give %+v", name, caps, want)
		}
	}
}

// D2: the wrapper that holds every remote has the methods of every optional
// interface, so it says what the backend it connects to can do; when it
// cannot connect, it says why rather than nothing, so no caller guesses.
func TestWrapperSaysWhatItsBackendCan(t *testing.T) {
	ctx := context.Background()
	inner := NewLocalBackend(t.TempDir())
	b := NewLazyBackend("sftp://host/x", func(context.Context) (model.Backend, error) { return inner, nil })
	defer CloseBackend(b)
	want, _ := inner.Capabilities(ctx)
	if got, err := b.Capabilities(ctx); err != nil || got != want {
		t.Errorf("the wrapper's Caps = %+v, %v; want its backend's, %+v", got, err, want)
	}
	if got := methodCaps(b); got == want {
		t.Fatalf("the wrapper's methods give %+v, its backend's Caps: the test shows nothing", got)
	}

	refused := NewLazyBackend("sftp://host/y", func(context.Context) (model.Backend, error) {
		return nil, fmt.Errorf("login: %w", model.ErrAuth)
	})
	defer CloseBackend(refused)
	if got, err := refused.Capabilities(ctx); !errors.Is(err, model.ErrAuth) {
		t.Errorf("a wrapper that cannot connect gave Caps %+v, %v; want the login error", got, err)
	}
}
