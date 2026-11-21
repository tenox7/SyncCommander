//go:build !unix

package transport

// localLinks: Windows makes a link only with a privilege, and takes a
// target written on Unix for another path. sc skips links there, as where
// a server has none; an rsync daemon gets none either, as each is sent
// from a link made here.
const localLinks = false
