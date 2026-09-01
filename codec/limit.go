package codec

import "fmt"

// LimitCodec rejects payloads larger than MaxDecode before decoding them.
// A nonpositive MaxDecode disables the limit. Encode passes values to Inner.
type LimitCodec[V any] struct {
	// Inner is the wrapped codec. It must not be nil.
	Inner Codec[V]

	// MaxDecode is the largest payload Decode accepts, in bytes.
	MaxDecode int
}

var _ Codec[struct{}] = LimitCodec[struct{}]{}

func (c LimitCodec[V]) Encode(v V) ([]byte, error) { return c.Inner.Encode(v) }
func (c LimitCodec[V]) Decode(b []byte) (V, error) {
	if c.MaxDecode > 0 && len(b) > c.MaxDecode {
		var zero V
		return zero, fmt.Errorf("payload too large: %d > %d", len(b), c.MaxDecode)
	}
	return c.Inner.Decode(b)
}
