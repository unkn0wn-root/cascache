package codecs

import "fmt"

type LimitCodec[V any] struct {
	Inner interface {
		Encode(V) ([]byte, error)
		Decode([]byte) (V, error)
	}
	MaxDecode int // bytes
}

func (c LimitCodec[V]) Encode(v V) ([]byte, error) { return c.Inner.Encode(v) }
func (c LimitCodec[V]) Decode(b []byte) (V, error) {
	if c.MaxDecode > 0 && len(b) > c.MaxDecode {
		var zero V
		return zero, fmt.Errorf("payload too large: %d > %d", len(b), c.MaxDecode)
	}
	return c.Inner.Decode(b)
}
