package cascache

import (
	"fmt"
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
