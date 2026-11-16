//go:build !unix

package transport

// oNoFollow: no such open flag here; the write checks the name with Lstat.
const oNoFollow = 0
