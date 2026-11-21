package model

import "errors"

// Kinds of failure the retry policy tells apart (review D3). A backend marks
// what it returns with one of them where it knows the cause (WithKind), and
// the policy finds them with errors.Is: never by searching an error's text,
// which can hold a file name.
var (
	// ErrAuth: the server refused the login, or its host key or
	// certificate did not verify. Never tried again: every try is one more
	// failed login, which can lock the account or get the address banned.
	ErrAuth = errors.New("login or host key refused")
	// ErrUnreachable: no connection could be made: refused, timed out, no
	// route, no such host, or the server hung up before the login ended.
	// Tried again, up to a few connects in a row.
	ErrUnreachable = errors.New("cannot reach the server")
	// ErrTooMany: the server turned a connection away because it has as
	// many as it takes: an rsync daemon's "max connections", an FTP
	// server's 421. It is no failed connect: the server takes one again
	// when another closes. sc waits for one of its own to close, and opens
	// fewer at a time from then on.
	ErrTooMany = errors.New("the server takes no more connections now")
	// ErrConnLost: the connection died in the middle of a call. The call is
	// tried again on a new connection.
	ErrConnLost = errors.New("connection lost")
	// ErrNoSpace: the destination is full or over its quota. Never tried
	// again: the next try fails the same way.
	ErrNoSpace = errors.New("no space left on the destination")
	// ErrReadOnly: the location takes no writes at all, as a web server's
	// folders over http:// take none. Never tried again, and a copy to it
	// stops at its first file.
	ErrReadOnly = errors.New("the location takes no writes")
)

// kindError gives an error a kind without changing its message.
type kindError struct{ kind, err error }

func (e *kindError) Error() string   { return e.err.Error() }
func (e *kindError) Unwrap() []error { return []error{e.kind, e.err} }

// WithKind marks err as being of kind, its message unchanged: one of the
// kinds above, or another the retry policy reads with errors.Is, as
// fs.ErrNotExist. nil stays nil.
func WithKind(kind, err error) error {
	if err == nil || errors.Is(err, kind) {
		return err
	}
	return &kindError{kind: kind, err: err}
}
