package cascache

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

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
	errs := make([]error, 0, 2)
	if e.BumpErr != nil {
		errs = append(errs, e.BumpErr)
	}
	if e.DelErr != nil {
		errs = append(errs, e.DelErr)
	}
	return errs
}

// ErrMissingObservedGens identifies a SetBulkWithGens caller error where at least
// one item key has no corresponding observed generation.
var ErrMissingObservedGens = errors.New("cascache: missing observed generations")

// MissingObservedGensError reports which logical keys were missing observed generations.
type MissingObservedGensError struct {
	Missing []string
}

func (e *MissingObservedGensError) Error() string {
	if len(e.Missing) == 0 {
		return ErrMissingObservedGens.Error()
	}
	return fmt.Sprintf("%s for keys [%s]", ErrMissingObservedGens, strings.Join(e.Missing, ", "))
}

func (e *MissingObservedGensError) Is(target error) bool {
	return target == ErrMissingObservedGens
}

func newMissingObservedGensError(missing []string) *MissingObservedGensError {
	cp := append([]string(nil), missing...)
	sort.Strings(cp)
	return &MissingObservedGensError{Missing: cp}
}
