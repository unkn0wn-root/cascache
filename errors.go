package cascache

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ErrMissingObservedGens identifies a SetBulkWithGens caller error where at least
// one item key has no corresponding observed generation.
var ErrMissingObservedGens = errors.New("missing observed generations")

// ErrBulkSeedNeedsAdder identifies an invalid configuration where
// BulkSeedIfMissing is requested with a provider that does not implement
// Adder.
var ErrBulkSeedNeedsAdder = errors.New("BulkSeedIfMissing requires Adder")

type InvalidateError struct {
	Key     string
	BumpErr error
	DelErr  error
}

func (e *InvalidateError) Error() string {
	switch {
	case e.BumpErr != nil && e.DelErr != nil:
		return fmt.Sprintf("invalidate %q failed: gen bump and delete failed: bump=%v; delete=%v",
			e.Key, e.BumpErr, e.DelErr)
	case e.BumpErr != nil:
		return fmt.Sprintf("invalidate %q: gen bump failed: %v", e.Key, e.BumpErr)
	case e.DelErr != nil:
		return fmt.Sprintf("invalidate %q: delete failed: %v", e.Key, e.DelErr)
	default:
		return fmt.Sprintf("invalidate %q: unknown error", e.Key)
	}
}

func (e *InvalidateError) Unwrap() []error {
	switch {
	case e.BumpErr == nil:
		if e.DelErr == nil {
			return nil
		}
		return []error{e.DelErr}
	case e.DelErr == nil:
		return []error{e.BumpErr}
	default:
		return []error{e.BumpErr, e.DelErr}
	}
}

// MissingObservedGensError reports which logical keys were missing observed generations.
type MissingObservedGensError struct {
	Missing []string
}

func (e *MissingObservedGensError) Error() string {
	if e == nil || len(e.Missing) == 0 {
		return ErrMissingObservedGens.Error()
	}
	quoted := make([]string, len(e.Missing))
	for i, key := range e.Missing {
		quoted[i] = strconv.Quote(key)
	}
	return fmt.Sprintf("%s for keys [%s]", ErrMissingObservedGens, strings.Join(quoted, ", "))
}

func (e *MissingObservedGensError) Unwrap() error {
	return ErrMissingObservedGens
}

func newMissingObservedGensError(missing []string) *MissingObservedGensError {
	cp := append([]string(nil), missing...)
	sort.Strings(cp)
	return &MissingObservedGensError{Missing: cp}
}

// Op identifies the logical cache operation that failed.
type Op string

const (
	OpGet        Op = "get"
	OpSet        Op = "set"
	OpAdd        Op = "add"
	OpSnapshot   Op = "snapshot"
	OpInvalidate Op = "invalidate"
	OpGetBulk    Op = "get_bulk"
	OpSetBulk    Op = "set_bulk"
)

// OpError reports an operation failure and, when applicable,
// the logical key that triggered it.
type OpError struct {
	Op  Op
	Key string // empty for non-key specific failures such as bulk path failures

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
