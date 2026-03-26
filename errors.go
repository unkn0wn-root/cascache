package cascache

import (
	"errors"
	"fmt"
)

// ErrBatchReadSeedNeedsAdder identifies an invalid configuration where
// BatchReadSeedIfMissing is requested with a provider that does not implement
// Adder.
var ErrBatchReadSeedNeedsAdder = errors.New("BatchReadSeedIfMissing requires Adder")

type InvalidateError struct {
	Key        string
	AdvanceErr error
	DelErr     error
}

func (e *InvalidateError) Error() string {
	switch {
	case e.AdvanceErr != nil && e.DelErr != nil:
		return fmt.Sprintf(
			"invalidate %q failed: version advance and delete failed: advance=%v; delete=%v",
			e.Key,
			e.AdvanceErr,
			e.DelErr,
		)
	case e.AdvanceErr != nil:
		return fmt.Sprintf("invalidate %q: version advance failed: %v", e.Key, e.AdvanceErr)
	case e.DelErr != nil:
		return fmt.Sprintf("invalidate %q: delete failed: %v", e.Key, e.DelErr)
	default:
		return fmt.Sprintf("invalidate %q: unknown error", e.Key)
	}
}

func (e *InvalidateError) Unwrap() []error {
	switch {
	case e.AdvanceErr == nil:
		if e.DelErr == nil {
			return nil
		}
		return []error{e.DelErr}
	case e.DelErr == nil:
		return []error{e.AdvanceErr}
	default:
		return []error{e.AdvanceErr, e.DelErr}
	}
}

// Op identifies the logical cache operation that failed.
type Op string

const (
	OpGet           Op = "get"
	OpSet           Op = "set"
	OpAdd           Op = "add"
	OpSnapshot      Op = "snapshot"
	OpInvalidate    Op = "invalidate"
	OpGetMany       Op = "get_many"
	OpSetIfVersions Op = "set_if_versions"
)

// OpError reports an operation failure and, when applicable,
// the logical key that triggered it.
type OpError struct {
	Op  Op
	Key string // empty for non-key specific failures such as batch path failures

	// Err is the underlying cause.
	// Error panics if Err is nil.
	Err error
}

func (e *OpError) Error() string {
	if e == nil {
		return "<nil>"
	}

	err := e.Err.Error()

	switch {
	case e.Op != "" && e.Key != "":
		return fmt.Sprintf("%s %q: %s", e.Op, e.Key, err)
	case e.Op != "":
		return fmt.Sprintf("%s: %s", e.Op, err)
	case e.Key != "":
		return fmt.Sprintf("%q: %s", e.Key, err)
	default:
		return err
	}
}

func (e *OpError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func opError(op Op, key string, err error) error {
	if err == nil {
		return nil
	}
	return &OpError{
		Op:  op,
		Key: key,
		Err: err,
	}
}
