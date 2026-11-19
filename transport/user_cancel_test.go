package transport

import (
	"context"
	"errors"
	"testing"
)

// The UI cancels with ErrUserCanceled when the user stops an operation.
// Code that returns a context's cause gives that error where it gave
// context.Canceled: it must still be a cancel, to the checks and the log.
func TestUserCanceledIsStillACancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	child, stop := context.WithCancel(ctx)
	defer stop()
	if UserCanceled(ctx) {
		t.Fatal("a context that runs passed for the user's x")
	}
	cancel(ErrUserCanceled)
	if !UserCanceled(ctx) || !UserCanceled(child) {
		t.Fatal("the user's x, or a context under it, did not pass for it")
	}
	cause := context.Cause(child)
	if !errors.Is(cause, context.Canceled) {
		t.Errorf("the cause %q is no context.Canceled", cause)
	}
	errs := Log.ErrCount()
	Log.Add("test", DirErr, "list d: "+cause.Error())
	if Log.ErrCount() != errs {
		t.Errorf("the log took %q as an error", cause)
	}
	// A fatal abort, or the copier's own stop, cancels with no cause.
	plain, end := context.WithCancelCause(context.Background())
	end(nil)
	if UserCanceled(plain) {
		t.Error("a cancel with no cause passed for the user's x")
	}
}
