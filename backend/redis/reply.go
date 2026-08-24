package redis

import (
	"fmt"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

// Parse the [value, fence] returned by MGET. Preserve a missing fence so the
// caller can reject the value.
func readResult(values []any) (backend.ReadResult, error) {
	if len(values) != 2 {
		return backend.ReadResult{}, errReplyLength
	}
	if values[0] == nil {
		return backend.ReadResult{}, nil
	}

	value, ok := asBytes(values[0])
	if !ok {
		return backend.ReadResult{}, errReplyType
	}
	out := backend.ReadResult{Value: value, Found: true}

	if values[1] == nil {
		return out, nil
	}
	fence, err := parseFence(values[1])
	if err != nil {
		return backend.ReadResult{}, err
	}
	out.Fence = fence
	out.FenceFound = true
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
