package redis

import (
	"fmt"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

func readResult(values []any) (backend.ReadResult, error) {
	if len(values) != 2 {
		return backend.ReadResult{}, errReplyLength
	}

	v, f := values[0], values[1]

	var out backend.ReadResult
	if v != nil {
		value, ok := asBytes(v)
		if !ok {
			return backend.ReadResult{}, errReplyType
		}
		out.Value, out.Found = value, true
	}

	if f == nil {
		return out, nil
	}
	fence, err := parseFence(f)
	if err != nil {
		if out.Found {
			return backend.ReadResult{}, err
		}
		// Leave malformed fences on misses for Ensure to report.
		return out, nil
	}
	out.Fence, out.FenceFound = fence, true
	return out, nil
}

func parseFence(value any) (backend.Fence, error) {
	raw, ok := asBytes(value)
	if !ok {
		return backend.Fence{}, errReplyType
	}
	fence, err := backend.ParseFence(raw)
	if err != nil {
		return backend.Fence{}, wrapFenceParse(err)
	}
	return fence, nil
}

func wrapFenceParse(err error) error {
	return fmt.Errorf("%w: %w", ErrFenceParse, err)
}

func asBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}
