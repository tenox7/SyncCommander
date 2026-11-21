package transport

import (
	"context"

	"sc/model"
)

// What each backend can do beyond model.Backend, one value per backend (D2).
// Each flag names an optional interface the backend implements; the lines
// below the methods make the compiler check the methods, and
// TestCapsMatchTheMethods checks that the flags say the same. None depends
// on the server: a server that refuses a call fails it, and the copy falls
// back as on any failure.

// A local root says how its file system matches names. A remote cannot
// tell yet: its Names stay unknown. It makes links, but not on Windows
// (localLinks).
func (b *LocalBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, Parts: true, RegionSums: true, OpenAt: true, Names: b.nameRule(), Links: localLinks}, nil
}

// The ssh shell sums many files with one command (S19).
// The ssh shells remove a folder's entries with one script (R16).
// SFTP makes links with its SYMLINK request.
func (b *SFTPBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, Parts: true, RegionSums: true, OpenAt: true, Preload: true, BatchSums: true,
		BatchRemove: true, Links: true}, nil
}

// The ssh shells make links with ln -s.
func (b *SCPBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, Parts: true, RegionSums: true, OpenAt: true, Preload: true, BatchSums: true,
		BatchRemove: true, Links: true}, nil
}

// An rsync destination counts its own upload progress (OwnsProgress). The
// region checksums of rsync+ssh are its shell's, and so are its links.
func (b *RsyncSSHBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, RegionSums: true, OpenAt: true, SendLocal: true, RecvLocal: true, Batch: true,
		Preload: true, PrefetchSums: true, BatchSums: true, OwnsProgress: true, BatchRemove: true, Links: true}, nil
}

// A daemon deletes a folder's entries in one run (R16). Each call dials
// the daemon by itself, so a lost connection fails the call it met, never
// the backend (RedialsItself): stock rsyncd resets the connection when a
// path is not there, and the backend was dropped with its listing cache,
// its running listing left to fill it, and the mtimes uploads sent (R8).
// A link is sent to it from one made here (localLinks).
func (b *RsyncBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, OpenAt: true, SendLocal: true, RecvLocal: true, Preload: true,
		PrefetchSums: true, BatchRemove: true, OwnsProgress: true, RedialsItself: true, Links: localLinks}, nil
}

func (b *FTPBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, Parts: true, OpenAt: true}, nil
}

// An HTTP backend, and rclone's, make their own connections again: a lost
// one fails a call, never the backend (RedialsItself).
func (b *WebDAVBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{OpenAt: true, Preload: true, RedialsItself: true}, nil
}

// restic names each object by the SHA-256 of its content and gives that
// name as its checksum: its sums are names (NamedSums, W-M3).
func (b *ResticBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{OpenAt: true, Preload: true, RedialsItself: true, NamedSums: true}, nil
}

// A web server's folders over http:// are read only: listed, side by side
// for a scan (Preload), and downloaded, from an offset too.
func (b *HTTPBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{OpenAt: true, Preload: true, RedialsItself: true}, nil
}

// A remote whose sums come with its listings gives a folder's in one
// listing (L20); on one whose sums read the file, a fetch gives none.
func (b *RcloneBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{OpenAt: true, Preload: true, RedialsItself: true, PrefetchSums: true, ListedSums: b.listsSums()}, nil
}

// The fake takes copies as a local disk does (L22).
func (b *FakeBackend) Capabilities(context.Context) (model.Caps, error) {
	return model.Caps{Resume: true, Parts: true, RegionSums: true, OpenAt: true}, nil
}

// The optional interfaces are reached by type assertion once Caps says yes,
// so a renamed or mistyped method would drop a feature without a word.
// These lines make the compiler check every backend against what it is
// meant to offer.
var (
	_ model.Backend            = (*LocalBackend)(nil)
	_ model.Resumer            = (*LocalBackend)(nil)
	_ model.SeekableOpener     = (*LocalBackend)(nil)
	_ model.LocalFS            = (*LocalBackend)(nil)
	_ model.PartWriter         = (*LocalBackend)(nil)
	_ model.RegionChecksummer  = (*LocalBackend)(nil)
	_ model.Backend            = (*SFTPBackend)(nil)
	_ model.RecursivePreloader = (*SFTPBackend)(nil)
	_ model.Resumer            = (*SFTPBackend)(nil)
	_ model.SeekableOpener     = (*SFTPBackend)(nil)
	_ ParallelSetter           = (*SFTPBackend)(nil)
	_ model.PartWriter         = (*SFTPBackend)(nil)
	_ model.RegionChecksummer  = (*SFTPBackend)(nil)
	_ model.ChecksumBatcher    = (*SFTPBackend)(nil)
	_ model.Backend            = (*SCPBackend)(nil)
	_ model.Resumer            = (*SCPBackend)(nil)
	_ model.SeekableOpener     = (*SCPBackend)(nil)
	_ model.RecursivePreloader = (*SCPBackend)(nil)
	_ ParallelSetter           = (*SCPBackend)(nil)
	_ model.PartWriter         = (*SCPBackend)(nil)
	_ model.RegionChecksummer  = (*SCPBackend)(nil)
	_ model.ChecksumBatcher    = (*SCPBackend)(nil)
	_ model.Backend            = (*RsyncSSHBackend)(nil)
	_ model.ChecksumPrefetcher = (*RsyncSSHBackend)(nil)
	_ model.ChecksumBatcher    = (*RsyncSSHBackend)(nil)
	_ model.Resumer            = (*RsyncSSHBackend)(nil)
	_ model.RegionChecksummer  = (*RsyncSSHBackend)(nil)
	_ model.SeekableOpener     = (*RsyncSSHBackend)(nil)
	_ model.LocalSender        = (*RsyncSSHBackend)(nil)
	_ model.LocalReceiver      = (*RsyncSSHBackend)(nil)
	_ model.BatchSender        = (*RsyncSSHBackend)(nil)
	_ model.RecursivePreloader = (*RsyncSSHBackend)(nil)
	_ ParallelSetter           = (*RsyncSSHBackend)(nil)
	_ model.Backend            = (*RsyncBackend)(nil)
	_ model.ChecksumPrefetcher = (*RsyncBackend)(nil)
	_ model.Resumer            = (*RsyncBackend)(nil)
	_ model.SeekableOpener     = (*RsyncBackend)(nil)
	_ model.LocalSender        = (*RsyncBackend)(nil)
	_ model.LocalReceiver      = (*RsyncBackend)(nil)
	_ model.RecursivePreloader = (*RsyncBackend)(nil)
	_ model.BatchRemover       = (*RsyncBackend)(nil)
	_ model.BatchRemover       = (*RsyncSSHBackend)(nil)
	_ model.BatchRemover       = (*SCPBackend)(nil)
	_ model.Linker             = (*SCPBackend)(nil)
	_ model.Linker             = (*RsyncSSHBackend)(nil)
	_ model.BatchRemover       = (*SFTPBackend)(nil)
	_ model.Linker             = (*SFTPBackend)(nil)
	_ model.Backend            = (*FTPBackend)(nil)
	_ model.Resumer            = (*FTPBackend)(nil)
	_ model.SeekableOpener     = (*FTPBackend)(nil)
	_ ParallelSetter           = (*FTPBackend)(nil)
	_ model.PartWriter         = (*FTPBackend)(nil)
	_ model.Backend            = (*WebDAVBackend)(nil)
	_ model.SeekableOpener     = (*WebDAVBackend)(nil)
	_ model.RecursivePreloader = (*WebDAVBackend)(nil)
	_ model.Backend            = (*ResticBackend)(nil)
	_ model.SeekableOpener     = (*ResticBackend)(nil)
	_ model.RecursivePreloader = (*ResticBackend)(nil)
	_ model.Backend            = (*HTTPBackend)(nil)
	_ model.SeekableOpener     = (*HTTPBackend)(nil)
	_ model.RecursivePreloader = (*HTTPBackend)(nil)
	_ model.Backend            = (*RcloneBackend)(nil)
	_ model.SeekableOpener     = (*RcloneBackend)(nil)
	_ model.RecursivePreloader = (*RcloneBackend)(nil)
	_ model.ChecksumPrefetcher = (*RcloneBackend)(nil)
	_ model.Backend            = (*FakeBackend)(nil)
	_ model.Resumer            = (*FakeBackend)(nil)
	_ model.PartWriter         = (*FakeBackend)(nil)
	_ model.RegionChecksummer  = (*FakeBackend)(nil)
	_ model.SeekableOpener     = (*FakeBackend)(nil)
	_ model.Backend            = (*lazyBackend)(nil)
	_ model.LivenessManaged    = (*lazyBackend)(nil)
	_ model.Resumer            = (*lazyBackend)(nil)
	_ model.BatchSender        = (*lazyBackend)(nil)
	_ model.BatchRemover       = (*lazyBackend)(nil)
	_ model.Linker             = (*lazyBackend)(nil)
	_ ParallelSetter           = (*lazyBackend)(nil)
	_ model.PartWriter         = (*lazyBackend)(nil)
	_ model.RegionChecksummer  = (*lazyBackend)(nil)
	_ model.ChecksumPrefetcher = (*lazyBackend)(nil)
	_ model.ChecksumBatcher    = (*lazyBackend)(nil)
	_ model.SeekableOpener     = (*lazyBackend)(nil)
	_ model.LocalSender        = (*lazyBackend)(nil)
	_ model.LocalReceiver      = (*lazyBackend)(nil)
	_ model.RecursivePreloader = (*lazyBackend)(nil)
)
