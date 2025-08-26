package codec

import "fmt"

// LimitCodec wraps another codec to enforce a maximum allowed payload size
// at Decode time. Encode is forwarded to Inner unchanged.
// If MaxDecode <= 0, size limiting is disabled.
//
// Typical use: protect against oversized/malicious inputs coming from a
// shared cache or untrusted source.
type LimitCodec[V any] struct {
	// Inner is the underlying codec being wrapped. It must be set.
	Inner interface {
		Encode(V) ([]byte, error)
		Decode([]byte) (V, error)
	}
	// MaxDecode is the maximum permitted length (in bytes) of the incoming
	// payload for Decode. If payload length exceeds MaxDecode, Decode returns
	// an error without invoking Inner.
	MaxDecode int
}

func (c LimitCodec[V]) Encode(v V) ([]byte, error) { return c.Inner.Encode(v) }
func (c LimitCodec[V]) Decode(b []byte) (V, error) {
	if c.MaxDecode > 0 && len(b) > c.MaxDecode {
		var zero V
		return zero, fmt.Errorf("payload too large: %d > %d", len(b), c.MaxDecode)
	}
	return c.Inner.Decode(b)
}
