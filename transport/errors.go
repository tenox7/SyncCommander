package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/pkg/sftp"

	"sc/model"
)

// errNotConnecting is what a backend returns, at once, while it tries no
// connect: its connects failed in a row for a whole retry budget (F7).
// Never tried again.
var errNotConnecting = errors.New("not connecting again yet")

// errClosed is what a call returns once its backend is closed: it makes no
// connect, and no try after it would either (F23).
var errClosed = errors.New("the location is closed")

// GivesUp reports an error after which no other file of a copy can get
// through: a refused login, a side that stopped connecting after its
// connects failed in a row, a closed location, a destination that is full
// or read-only. A copy stops on it rather than fail every file the same
// way, each after its own tries (F7).
func GivesUp(err error) bool {
	return errors.Is(err, model.ErrAuth) || errors.Is(err, errNotConnecting) || errors.Is(err, errClosed) || cannotWrite(err)
}

// cannotWrite reports a destination that takes no more data: full, over its
// quota, or mounted read-only.
func cannotWrite(err error) bool {
	if errors.Is(err, model.ErrNoSpace) {
		return true
	}
	for _, e := range noSpaceErrnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// isConnLost reports whether err says the connection under the backend has
// died, so the next try must redial. It reads types only (D3): what the
// network stack and the ssh and sftp libraries return when the link goes.
// An error's text can hold a file name ("connection reset.txt") or the
// server's own words, and a connection dropped on a guess kills every
// transfer that shares it. An op-level EOF counts too: no backend method
// returns io.EOF from a healthy connection (readers are not wrapped here),
// so it can only mean the peer hung up. A failed dial is not one: no
// connection was made, so there is none to drop.
func isConnLost(err error) bool {
	if err == nil || dialFailure(err) {
		return false
	}
	if errors.Is(err, model.ErrConnLost) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, sftp.ErrSSHFxConnectionLost) {
		return true
	}
	for _, e := range connLostErrnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return anyInChain(err, func(e error) bool {
		// The ssh server ended the connection. x/crypto/ssh keeps the
		// type unexported, so it is known by its name, which no file
		// name or server text can put there.
		return fmt.Sprintf("%T", e) == "*ssh.disconnectMsg"
	})
}

// anyInChain reports whether match holds for err or any error it wraps.
func anyInChain(err error, match func(error) bool) bool {
	for err != nil {
		if match(err) {
			return true
		}
		switch u := err.(type) {
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if anyInChain(e, match) {
					return true
				}
			}
			return false
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		default:
			return false
		}
	}
	return false
}

// connectFailure gives an error from connecting and logging in its kind, by
// type: a certificate that does not verify is model.ErrAuth; a connect that
// failed to open, or that the server hung up on or let time out before the
// login ended, is model.ErrUnreachable. The backends mark a refused login
// themselves, where they read the server's answer. Others keep no kind.
func connectFailure(err error) error {
	var netErr net.Error
	switch {
	case err == nil, errors.Is(err, model.ErrAuth), errors.Is(err, model.ErrUnreachable):
		return err
	case certFailure(err):
		return model.WithKind(model.ErrAuth, err)
	case dialFailure(err), isConnLost(err), errors.Is(err, os.ErrDeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return model.WithKind(model.ErrUnreachable, err)
	}
	return err
}

// dialFailure reports an error from opening a connection: refused, no
// route, no such host, timed out. A call that opens its own connection (an
// HTTP request) fails this way when the server is gone.
func dialFailure(err error) bool {
	var dnsErr *net.DNSError
	var opErr *net.OpError
	return errors.Is(err, model.ErrUnreachable) || errors.As(err, &dnsErr) || errors.As(err, &opErr) && opErr.Op == "dial"
}

// certFailure reports a TLS certificate that does not verify.
func certFailure(err error) bool {
	var unknown x509.UnknownAuthorityError
	var host x509.HostnameError
	var invalid x509.CertificateInvalidError
	var verify *tls.CertificateVerificationError
	return errors.As(err, &unknown) || errors.As(err, &host) || errors.As(err, &invalid) || errors.As(err, &verify)
}
