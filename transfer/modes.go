package transfer

import (
	"context"
	"errors"
	"sync/atomic"

	"sc/model"
	"sc/transport"
)

// A destination that would not take a file's mode wrote the file all the
// same, and its write or commit says so with model.ErrModeNotSet (S20).
// That is no failure, as a copy whose times could not be set is none: the
// call counts as done, the copy notes the error, and once the copy went
// through the file is counted (Progress.ModesNotSet). A failed chmod
// failed the copy, which sent the file up to six times.
//
// Any try's note counts: a later try may commit the part an earlier one
// wrote, mode and all.

type modeNoteKey struct{}

// modeNote is the first mode error of a file's copy.
type modeNote struct{ err atomic.Pointer[error] }

// withModeNote gives the calls under ctx a note for a mode their
// destination would not take.
func withModeNote(ctx context.Context) (context.Context, *modeNote) {
	n := &modeNote{}
	return context.WithValue(ctx, modeNoteKey{}, n), n
}

// modeTaken is err, a write's or a commit's, with a mode the destination
// would not take noted in ctx, and the call taken as done. With no note in
// ctx the error stays: a copy that counts no modes must not lose one.
func modeTaken(ctx context.Context, err error) error {
	n, ok := ctx.Value(modeNoteKey{}).(*modeNote)
	if !ok || !errors.Is(err, model.ErrModeNotSet) {
		return err
	}
	n.err.CompareAndSwap(nil, &err)
	return nil
}

// get is the first mode error noted, or nil.
func (n *modeNote) get() error {
	if p := n.err.Load(); p != nil {
		return *p
	}
	return nil
}

// countMode counts relPath as copied without its mode when n holds an
// error, with a line that says why.
func (c *copier) countMode(relPath string, n *modeNote) {
	if err := n.get(); err != nil {
		c.p.ModesNotSet.Add(1)
		transport.Log.Add("copy", transport.DirErr, "chmod "+relPath+": "+err.Error())
	}
}
