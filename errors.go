package cascache

import (
	"errors"
	"fmt"
)

// Construction and input errors.
var (
	ErrNoNamespace = errors.New("cascache: namespace is required")
	ErrNoBackend   = errors.New("cascache: backend is required")
	ErrNoCodec     = errors.New("cascache: codec is required")
	ErrNoLoader    = errors.New("cascache: loader is required")
	// ErrInvalidSnapshot reports a zero snapshot passed to a write.
	ErrInvalidSnapshot = errors.New("cascache: invalid snapshot")
	// ErrInvalidTTL reports a negative TTL other than [NoExpiration].
	ErrInvalidTTL = errors.New("cascache: invalid TTL")
	// ErrInvalidLoadTimeout reports a negative [Options.LoadTimeout].
	ErrInvalidLoadTimeout = errors.New("cascache: invalid load timeout")
	// ErrInvalidCost reports a nonpositive admission cost.
	ErrInvalidCost = errors.New("cascache: invalid cost")
)

// ErrBackendContract reports a backend that returned something its contract
// does not allow, such as an invalid fence from Ensure.
var ErrBackendContract = errors.New("cascache: backend contract violation")

// ErrComputeTTL marks a fill skipped because [Options.ComputeTTL] failed. Load
// still returns the loaded value.
var ErrComputeTTL = errors.New("cascache: compute ttl")

var (
	// ErrLoaderPanic reports a loader that panicked.
	ErrLoaderPanic = errors.New("cascache: loader panicked")
	// ErrLoaderGoexit reports a loader that exited without returning.
	ErrLoaderGoexit = errors.New("cascache: loader exited without returning")
)

// OpError adds the operation and key to an error.
type OpError struct {
	Op  Op
	Key string
	Err error
}

func (e *OpError) Error() string {
	switch {
	case e == nil:
		return "<nil>"
	case e.Err == nil:
		return fmt.Sprintf("cascache %s %q: unknown error", e.Op, e.Key)
	default:
		return fmt.Sprintf("cascache %s %q: %v", e.Op, e.Key, e.Err)
	}
}

func (e *OpError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PanicError carries what a loader panicked with, and where. It unwraps to [ErrLoaderPanic].
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %v\n%s", ErrLoaderPanic.Error(), e.Value, e.Stack)
}

func (e *PanicError) Unwrap() error { return ErrLoaderPanic }
